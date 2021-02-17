#ifndef __HYPERCOMM_ANALYTICS_HPP__
#define __HYPERCOMM_ANALYTICS_HPP__

#include <mutex>
#include <sstream>

#include <hypercomm/aggregation.hpp>

namespace aggregation {
namespace detail {
struct stats_ {
  std::size_t nAggregatedMessages;
  std::size_t nBytesAggregated;
  std::size_t nFlushes;
  double avgUtilizationAtFlush;
};

namespace binary_tree_ {
inline int left_child(const int& i) { return (2 * i) + 1; }

inline int right_child(const int& i) { return (2 * i) + 2; }

inline int parent(const int& i) { return (i > 0) ? ((i - 1) / 2) : -1; }

inline int num_leaves(const int& n) { return (n + 1) / 2; }

inline int num_children(const int& n, const int& i) {
  return (left_child(i) < n) + (right_child(i) < n);
}
}
}
}

PUPbytes(aggregation::detail::stats_);

namespace aggregation {

namespace {
using stats_registry_t = std::vector<detail::stats_>;
using node_lock_t = std::mutex;
CkpvDeclare(int, _recv_stats_idx);
CksvDeclare(stats_registry_t, stats_registry_);
#if CMK_SMP
CksvDeclare(node_lock_t, node_lock_);
#endif

detail::stats_* stats_for(const endpoint_id_t& id) {
  if (!CsvInitialized(stats_registry_)) {
    CksvInitialize(stats_registry_t, stats_registry_);
  }
  auto reg = &(CksvAccess(stats_registry_));
  if (reg->size() <= id) {
    reg->resize(id + 1);
  }
  return &(*reg)[id];
}

envelope* pack_stats_(stats_registry_t& reg) {
  auto numStats = reg.size();
  PUP::sizer s;
  s | numStats;
  PUParray(s, reg.data(), numStats);
  auto env = _allocEnv(CkEnvelopeType::ForBocMsg, s.size());
  PUP::toMem p(EnvToUsr(env));
  p | numStats;
  PUParray(p, reg.data(), numStats);
  CkAssert(p.size() == s.size() && "pup error");
  return env;
}

// TODO evaluate whether CkAlign is necessary here
void unpack_stats_(const envelope* env, std::size_t& numStats,
                   detail::stats_*& stats) {
  auto buffer = static_cast<char*>(EnvToUsr(env));
  numStats = *(reinterpret_cast<std::size_t*>(buffer));
  stats = reinterpret_cast<detail::stats_*>(buffer + sizeof(std::size_t));
}

envelope* merge_stats_fn_(envelope* left, envelope* right) {
  if (left == nullptr || left->getUsersize() == 0) {
    if (left) CmiFree(left);
    return right;
  } else if (right == nullptr || right->getUsersize() == 0) {
    if (right) CmiFree(right);
    return left;
  }

  detail::stats_ *ours, *theirs;
  std::size_t szOurs, szTheirs;

  unpack_stats_(static_cast<envelope*>(left), szOurs, ours);
  unpack_stats_(static_cast<envelope*>(right), szTheirs, theirs);
  CkAssert(szOurs == szTheirs);

  for (auto j = 0; j < szOurs; j += 1) {
    ours[j].nAggregatedMessages += theirs[j].nAggregatedMessages;
    ours[j].nBytesAggregated += theirs[j].nBytesAggregated;
    ours[j].avgUtilizationAtFlush =
        (ours[j].avgUtilizationAtFlush * ours[j].nFlushes +
         theirs[j].avgUtilizationAtFlush * theirs[j].nFlushes) /
        (ours[j].nFlushes + theirs[j].nFlushes);
    ours[j].nFlushes += theirs[j].nFlushes;
  }

  CmiFree(right);
  return left;
}

void print_stats_(const detail::stats_* reg, const std::size_t& szReg) {
  std::stringstream ss;
  for (std::size_t i = 0; i < szReg; i += 1) {
    const auto& stats = reg[i];
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

  if (szReg == 0) {
    ss << "[INFO] No aggregation data available." << std::endl;
  }

  CkPrintf("%s\n", ss.str().c_str());
}

CsvDeclare(envelope*, sibling_);

void recv_stats_(void* impl_msg_) {
  auto env = static_cast<envelope*>(impl_msg_);

  const auto self = CmiMyNode();
  const auto left = detail::binary_tree_::left_child(self);
  const auto right = detail::binary_tree_::right_child(self);
  const auto parent = detail::binary_tree_::parent(self);
  const auto nChildren =
      detail::binary_tree_::num_children(CmiNumNodes(), self);

#if CMK_SMP
  CksvAccess(node_lock_).lock();
#endif
  envelope* sibling = nullptr;
  if (nChildren == 2) {
    if (CksvAccess(sibling_) == nullptr) {
      CksvAccess(sibling_) =
          merge_stats_fn_(env, pack_stats_(CksvAccess(stats_registry_)));
      CkAssert(CksvAccess(sibling_) != nullptr);
    } else {
      sibling = CksvAccess(sibling_);
    }
  } else {
    sibling = pack_stats_(CksvAccess(stats_registry_));
  }
#if CMK_SMP
  CksvAccess(node_lock_).unlock();
#endif

  if (sibling == nullptr && nChildren == 2) {
    // wait for the next message to arrive
    return;
  }

  auto ours = merge_stats_fn_(env, sibling);

  // if we are not the root node
  if (parent >= 0) {
    // send our merged value to our parent
    CmiSetHandler(ours, CkpvAccess(_recv_stats_idx));
    CmiSyncNodeSendAndFree(parent, ours->getTotalsize(),
                           reinterpret_cast<char*>(ours));
  } else {
    detail::stats_* reg;
    std::size_t szReg;
    // otherwise, print the merged value
    unpack_stats_(static_cast<envelope*>(ours), szReg, reg);
    print_stats_(reg, szReg);
    // and exit
    CmiFree(ours);
    CkContinueExit();
  }
}

void exit_handler_(void) {
  auto n = CmiNumNodes();
  auto l = detail::binary_tree_::num_leaves(n);

  std::vector<int> leaves(l);
  std::iota(std::begin(leaves), std::end(leaves), n - l);

  auto env = _allocEnv(CkEnvelopeType::ForBocMsg, 0);
  CmiSetHandler(env, CkpvAccess(_recv_stats_idx));
  for (const auto& leaf : leaves) {
    CmiSyncNodeSendFn(leaf, env->getTotalsize(), reinterpret_cast<char*>(env));
  }

  CmiFree(env);
}
}

namespace analytics {

inline void initialize(void) {
#if CMK_SMP
  CksvInitialize(node_lock_t, node_lock_);
#endif

  CksvInitialize(envelope*, sibling_);
  CksvAccess(sibling_) = nullptr;

  CkpvInitialize(int, _recv_stats_idx);
  CkpvAccess(_recv_stats_idx) =
      CmiRegisterHandler(reinterpret_cast<CmiHandler>(&recv_stats_));

  if (CkMyPe() == 0) {
    registerExitFn(reinterpret_cast<CkExitFn>(&exit_handler_));
  }
}

void tally_message(const endpoint_id_t& id, const msg_size_t& size) {
#if CMK_SMP
  CksvAccess(node_lock_).lock();
#endif
  auto& stats = *(stats_for(id));
  stats.nAggregatedMessages += 1;
  stats.nBytesAggregated += size;
#if CMK_SMP
  CksvAccess(node_lock_).unlock();
#endif
}

void tally_flush(const endpoint_id_t& id, const float& utilization) {
#if CMK_SMP
  CksvAccess(node_lock_).lock();
#endif
  auto& stats = *(stats_for(id));
  auto& n = stats.nFlushes;
  auto& cma = stats.avgUtilizationAtFlush;
  cma = (utilization + n * cma) / (n + 1);
  n++;
#if CMK_SMP
  CksvAccess(node_lock_).unlock();
#endif
}
}
}

#endif
