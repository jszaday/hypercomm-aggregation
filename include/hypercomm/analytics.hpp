#ifndef __HYPERCOMM_ANALYTICS_HPP__
#define __HYPERCOMM_ANALYTICS_HPP__

#include <map>
#include <mutex>
#include <sstream>

#include <hypercomm/aggregation.hpp>
#include <hypercomm/reductions.hpp>

namespace aggregation {
namespace detail {
struct stats_ {
  std::size_t nAggregatedMessages;
  std::size_t nBytesAggregated;
  std::size_t nFlushes;
  double avgUtilizationAtFlush;

  stats_(void)
      : nAggregatedMessages(0), nBytesAggregated(0), nFlushes(0),
        avgUtilizationAtFlush(0) {}

  void pup(PUP::er &p) {
    p | this->nAggregatedMessages;
    p | this->nBytesAggregated;
    p | this->nFlushes;
    p | this->avgUtilizationAtFlush;
  }

  stats_ &operator+=(const stats_ &rhs) {
    this->nAggregatedMessages += rhs.nAggregatedMessages;
    this->nBytesAggregated += rhs.nBytesAggregated;
    this->avgUtilizationAtFlush =
        (this->avgUtilizationAtFlush * this->nFlushes +
         rhs.avgUtilizationAtFlush * rhs.nFlushes) /
        (this->nFlushes + rhs.nFlushes);
    this->nFlushes += rhs.nFlushes;
    return *this;
  }

  void tally_message(const msg_size_t size) {
    this->nAggregatedMessages += 1;
    this->nBytesAggregated += size;
  }

  void tally_flush(const float &utilization) {
    auto &n = this->nFlushes;
    auto &cma = this->avgUtilizationAtFlush;
    cma = (utilization + n * cma) / (n + 1);
    n += 1;
  }
};
} // namespace detail
} // namespace aggregation

namespace aggregation {

namespace {
using stats_registry_t = std::map<endpoint_id_t, detail::stats_>;

CkpvDeclare(int, send_stats_idx_);
CkpvDeclare(stats_registry_t, stats_registry_);

detail::stats_ &stats_for_(const endpoint_id_t &id) {
  return (CkpvAccess(stats_registry_))[id];
}

envelope *pack_stats_(stats_registry_t &reg) {
  auto size = PUP::size(reg);
  auto env = _allocEnv(CkEnvelopeType::ForBocMsg, size);
  PUP::toMem p((char *)EnvToUsr(env));
  p | reg;
  CkAssertMsg(size == p.size(), "pup error!");
  return env;
}

void unpack_stats_(envelope *env, stats_registry_t &reg) {
  PUP::fromMem p((char *)EnvToUsr(env));
  p | reg;
}

void combine_stats_(stats_registry_t &lhs, stats_registry_t &rhs) {
  using value_type = typename stats_registry_t::value_type;
  std::set<endpoint_id_t> keys;

  auto helper = [](const value_type &val) { return val.first; };

  std::transform(std::begin(lhs), std::end(lhs),
                 std::inserter(keys, keys.begin()), helper);
  std::transform(std::begin(rhs), std::end(rhs),
                 std::inserter(keys, keys.begin()), helper);

  for (auto &key : keys) {
    auto search = rhs.find(key);
    if (search != std::end(rhs)) {
      lhs[key] += search->second;
    }
  }
}

envelope *merge_stats_(envelope *left, envelope *right) {
  if (left == nullptr || left->getUsersize() == 0) {
    if (left)
      CmiFree(left);
    return right;
  } else if (right == nullptr || right->getUsersize() == 0) {
    if (right)
      CmiFree(right);
    return left;
  } else {
    stats_registry_t ours, theirs;
    unpack_stats_(left, ours);
    unpack_stats_(right, theirs);

    combine_stats_(ours, theirs);

    CmiFree(left);
    CmiFree(right);

    return pack_stats_(ours);
  }
}

void print_stats_(stats_registry_t &registry) {
  std::stringstream ss;
  for (auto &pair : registry) {
    const auto &i = pair.first;
    const auto &stats = pair.second;

    ss << std::endl << "[INFO] Data for Aggregator " << i << ":" << std::endl;
    ss << "\tnbr of flushes = " << stats.nFlushes << std::endl;
    ss << "\tnbr of messages aggregated = " << stats.nAggregatedMessages
       << std::endl;
    ss << "\ttotal megabytes aggregated = "
       << stats.nBytesAggregated / (1024.0 * 1024.0) << "MB" << std::endl;
    auto avgAggregatedMessageSize =
        stats.nBytesAggregated / ((double)stats.nAggregatedMessages);
    ss << "\tavg bytes per aggregated message = " << avgAggregatedMessageSize
       << "B" << std::endl;
    ss << "\tavg messages per flush = "
       << (stats.nAggregatedMessages / ((double)stats.nFlushes)) << std::endl;
    ss << "\tavg buffer utilization at flush-time = "
       << (100.0 * stats.avgUtilizationAtFlush) << "%" << std::endl;
  }

  if (registry.empty()) {
    ss << "[INFO] No aggregation data available." << std::endl;
  }

  CkPrintf("%s\n", ss.str().c_str());
}

void recv_stats_(envelope *env) {
  // print the merged value(s)
  stats_registry_t merged;
  unpack_stats_(env, merged);
  print_stats_(merged);
  CmiFree(env);
  // then exit
  CkContinueExit();
}

void send_stats_(void *_) {
  auto &head = CkpvAccessOther(stats_registry_, 0);
  for (auto rank = 1; rank < CmiNodeSize(CmiMyNode()); rank += 1) {
    auto &next = CkpvAccessOther(stats_registry_, rank);
    combine_stats_(head, next);
  }

  hypercomm::reductions::node::contribute(pack_stats_(head), &recv_stats_,
                                          &merge_stats_);

  CmiFree(_);
}

void exit_handler_(void) {
  auto env = _allocEnv(CkEnvelopeType::ForBocMsg, 0);
  CmiSetHandler(env, CkpvAccess(send_stats_idx_));
  CmiSyncNodeBroadcastAllAndFree(env->getTotalsize(),
                                 reinterpret_cast<char *>(env));
}
} // namespace

namespace analytics {

inline void initialize(void) {
  CkpvInitialize(stats_registry_t, stats_registry_);
  CkpvInitialize(int, send_stats_idx_);
  CkpvAccess(send_stats_idx_) =
      CmiRegisterHandler(reinterpret_cast<CmiHandler>(&send_stats_));

  hypercomm::reductions::initialize();

  if (CkMyPe() == 0) {
    registerExitFn(reinterpret_cast<CkExitFn>(&exit_handler_));
  }
}

void tally_message(const endpoint_id_t &id, const msg_size_t &size) {
  stats_for_(id).tally_message(size);
}

void tally_flush(const endpoint_id_t &id, const float &utilization) {
  stats_for_(id).tally_flush(utilization);
}
} // namespace analytics
} // namespace aggregation

#endif
