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
}
}

PUPbytes(aggregation::detail::stats_);

namespace aggregation {

namespace {
using stats_registry_t = std::vector<detail::stats_>;
using node_lock_t = std::mutex;
CkpvDeclare(int, _recvStatsIdx);
CkpvDeclare(int, _cntrStatsIdx);
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
  CmiSetHandler(env, CkpvAccess(_recvStatsIdx));
  PUP::toMem p(EnvToUsr(env));
  p | numStats;
  PUParray(p, reg.data(), numStats);
  CkAssert(p.size() == s.size() && "pup error");
  return env;
}

void unpack_stats_(const envelope* env, stats_registry_t& reg) {
  PUP::fromMem p(EnvToUsr(env));
  std::size_t numStats;
  p | numStats;
  reg.resize(numStats);
  PUParray(p, reg.data(), numStats);
}

void* merge_stats_fn_(int* size, void* local, void** remote, int n) {
  stats_registry_t ours;
  unpack_stats_(static_cast<envelope*>(local), ours);
  for (auto i = 0; i < n; i += 1) {
    stats_registry_t theirs;
    unpack_stats_(static_cast<envelope*>(remote[i]), theirs);
    CkAssert(ours.size() == theirs.size());
    for (auto j = 0; j < ours.size(); j += 1) {
      ours[j].nAggregatedMessages += theirs[j].nAggregatedMessages;
      ours[j].nBytesAggregated += theirs[j].nBytesAggregated;
      ours[j].avgUtilizationAtFlush =
          (ours[j].avgUtilizationAtFlush * ours[j].nFlushes +
           theirs[j].avgUtilizationAtFlush * theirs[j].nFlushes) /
          (ours[j].nFlushes + theirs[j].nFlushes);
      ours[j].nFlushes += theirs[j].nFlushes;
    }
  }
  CmiFree(local);
  return pack_stats_(ours);
}

void contribute_stats_(void* _) {
  auto env = pack_stats_(CksvAccess(stats_registry_));
  CmiNodeReduce(reinterpret_cast<char*>(env), env->getTotalsize(),
                reinterpret_cast<CmiReduceMergeFn>(&merge_stats_fn_));
}

void recv_stats_(void* msg) {
  stats_registry_t reg;
  unpack_stats_(static_cast<envelope*>(msg), reg);
  std::stringstream ss;

  for (std::size_t i = 0; i < reg.size(); i += 1) {
    const auto& stats = reg[i];
    ss << std::endl << "[INFO] Data for Aggregator " << i << ":" << std::endl;
    ss << "\tnbr of flushes = " << stats.nFlushes << std::endl;
    ss << "\tnbr of messages aggregated = " << stats.nAggregatedMessages
       << std::endl;
    ss << "\tnbr of megabytes aggregated = "
       << stats.nBytesAggregated / (1024.0 * 1024.0) << "MB" << std::endl;
    auto avgAggregatedMessageSize =
        stats.nBytesAggregated / ((double)stats.nAggregatedMessages);
    ss << "\tavg bytes per aggregated message = " << avgAggregatedMessageSize
       << "B" << std::endl;
    ss << "\tavg buffer utilization at flush-time = "
       << (100.0 * stats.avgUtilizationAtFlush) << "%" << std::endl;
  }

  if (reg.empty()) {
    CkPrintf("[INFO] No aggregation data available.\n");
  } else {
    CkPrintf("%s\n", ss.str().c_str());
  }

  CkContinueExit();
}

void exit_handler_(void) {
  auto env = _allocEnv(CkEnvelopeType::ForBocMsg, 0);
  CmiSetHandler(env, CkpvAccess(_cntrStatsIdx));
  CmiSyncNodeBroadcastAllAndFree(env->getTotalsize(),
                                 reinterpret_cast<char*>(env));
}
}

namespace analytics {

inline void initialize(void) {
#if CMK_SMP
  CksvInitialize(node_lock_t, node_lock_);
#endif
  CkpvInitialize(int, _recvStatsIdx);
  CkpvInitialize(int, _cntrStatsIdx);
  CkpvAccess(_cntrStatsIdx) =
      CmiRegisterHandler(reinterpret_cast<CmiHandler>(&contribute_stats_));
  CkpvAccess(_recvStatsIdx) =
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