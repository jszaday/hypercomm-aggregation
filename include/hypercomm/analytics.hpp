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

// TODO evaluate whether CkAlign is necessary here
void unpack_stats_(const envelope* env, std::size_t& numStats,
                   detail::stats_*& stats) {
  auto buffer = static_cast<char*>(EnvToUsr(env));
  numStats = *(reinterpret_cast<std::size_t*>(buffer));
  stats = reinterpret_cast<detail::stats_*>(buffer + sizeof(std::size_t));
}

void* merge_stats_fn_(int* size, void* local, void** remote, int szRemote) {
  detail::stats_* ours;
  std::size_t szOurs;
  unpack_stats_(static_cast<envelope*>(local), szOurs, ours);
  for (auto i = 0; i < szRemote; i += 1) {
    detail::stats_* theirs;
    std::size_t szTheirs;
    unpack_stats_(static_cast<envelope*>(remote[i]), szTheirs, theirs);
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
  }
  return local;
}

void contribute_stats_(void* _) {
  auto env = pack_stats_(CksvAccess(stats_registry_));
  CmiNodeReduce(reinterpret_cast<char*>(env), env->getTotalsize(),
                reinterpret_cast<CmiReduceMergeFn>(&merge_stats_fn_));
}

void recv_stats_(void* msg) {
  detail::stats_* reg;
  std::size_t szReg;
  unpack_stats_(static_cast<envelope*>(msg), szReg, reg);

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

  CmiFree(msg);

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
