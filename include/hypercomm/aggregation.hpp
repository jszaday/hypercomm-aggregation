#ifndef __HYPERCOMM_AGGREGATION_HPP__
#define __HYPERCOMM_AGGREGATION_HPP__

#include <ck.h>
#include <map>
#include <functional>

namespace aggregation {
using endpoint_fn_t = std::function<void(void*)>;
using endpoint_id_t = std::size_t;
using endpoint_registry_t = std::vector<endpoint_fn_t>;
using msg_queue_t = std::vector<CkMarshalledMessage>;

template <typename... Ts>
struct aggregator;

namespace {
int _bundleIdx;
CkpvDeclare(endpoint_registry_t, endpoint_registry_);

void _bundle_handler(void* impl_env) {
  auto env = static_cast<envelope*>(impl_env);
  auto msg = static_cast<CkMarshallMsg*>(EnvToUsr(env));
  auto p = PUP::fromMem(msg->msgBuf);
  endpoint_id_t idx;
  std::size_t nMsgs;
  p | idx;
  p | nMsgs;
  const auto& fn = (CkpvAccess(endpoint_registry_))[idx];
  for (auto i = 0; i < nMsgs; i++) {
    CkMarshalledMessage m;
    p | m;
    QdProcess(1);
    fn(m.getMessage());
  }
  CkFreeMsg(msg);
}

template <typename... Ts>
static void _when_idle(void* self) {
  static_cast<aggregator<Ts...>*>(self)->on_idle();
}
}

void initialize(void) {
  CkpvInitialize(endpoint_registry_t, endpoint_registry_);
  CmiAssignOnce(&_bundleIdx, CmiRegisterHandler((CmiHandler)_bundle_handler));
}

endpoint_id_t register_endpoint_fn(const endpoint_fn_t& fn) {
  auto& reg = CkpvAccess(endpoint_registry_);
  reg.push_back(fn);
  return reg.size() - 1;
}

template <typename... Ts>
struct aggregator {
  aggregator(int msgThreshold, double flushTimeout,
             const endpoint_fn_t& endpoint)
      : mMsgThreshold(msgThreshold),
        mFlushTimeout(flushTimeout) {
    mEndpoint = register_endpoint_fn(endpoint);
    set_cond();
  }

  inline bool timed_out(const int& pe) {
    auto found = mLastFlush.find(pe);
    if (found == mLastFlush.end()) {
      mLastFlush[pe] = CkWallTimer();
      return false;
    } else {
      return (CkWallTimer() - found->second) >= mFlushTimeout;
    }
  }

  inline bool should_flush(const int& pe) {
    return (mQueues[pe].size() >= mMsgThreshold) || timed_out(pe);
  }

  void flush(const int& pe) {
    auto& queue = mQueues[pe];
    auto nMsgs = queue.size();

    auto pupFn = [&](PUP::er& p) {
      p | mEndpoint;
      p | nMsgs;
      for (auto& msg : queue) {
        p | msg;
      }
      return p.size();
    };

    PUP::sizer s;
    auto size = pupFn(s);
    auto msg = CkAllocateMarshallMsg(size);
    PUP::toMem p(msg->msgBuf);
    if (pupFn(p) != size) {
      CkAbort("pup failure");
    }

    envelope* env = UsrToEnv(msg);
    env->setSrcPe(CkMyPe());
    CmiSetHandler(env, _bundleIdx);
    CmiSyncSendAndFree(pe, env->getTotalsize(), (char*)env);

    mLastFlush[pe] = CkWallTimer();
    queue.clear();
  }

  void on_idle(void) {
    for (const auto& pair : mQueues) {
      if (!pair.second.empty() && timed_out(pair.first)) {
        flush(pair.first);
      }
    }

    set_cond();
  }

  void send(const int& pe, const Ts&... const_ts) {
    auto args = std::forward_as_tuple(const_cast<Ts&>(const_ts)...);
    auto size = PUP::size(args);
    auto msg = CkAllocateMarshallMsg(size);
    PUP::toMemBuf(args, msg->msgBuf, size);
    send(pe, msg);
  }

  void send(const int& pe, CkMessage* msg) {
    QdCreate(1);
    put_msg(pe, msg);
    if (should_flush(pe)) {
      flush(pe);
    }
  }

 private:
  int mMsgThreshold;
  double mFlushTimeout;
  endpoint_id_t mEndpoint;

  std::map<int, double> mLastFlush;
  std::map<int, msg_queue_t> mQueues;

  inline void put_msg(const int& pe, CkMessage* msg) {
    mQueues[pe].emplace_back(msg);
  }

  inline void set_cond(void) {
    // Could alternatively use a periodic timer
    CcdCallOnCondition(CcdPROCESSOR_STILL_IDLE,
                       reinterpret_cast<CcdVoidFn>(&_when_idle<Ts...>),
                       this);
  }
};
}

#endif
