#ifndef __HYPERCOMM_AGGREGATION_HPP__
#define __HYPERCOMM_AGGREGATION_HPP__

#include <map>
#include <ck.h>
#include <deque>
#include <mutex>
#include <functional>

namespace aggregation {
using endpoint_fn_t = std::function<void(void*)>;
using endpoint_id_t = std::size_t;
using endpoint_registry_t = std::vector<endpoint_fn_t>;
using msg_queue_t = std::deque<CkMarshalledMessage>;

CkpvExtern(int, _bundleIdx);
extern endpoint_id_t register_endpoint_fn(const endpoint_fn_t& fn, bool nodeLevel);

template <typename... Ts>
struct aggregator;

namespace {
template <typename... Ts>
static void _on_condition(void* self) {
  static_cast<aggregator<Ts...>*>(self)->on_cond();
}
}

template <typename... Ts>
struct aggregator {
  // example conditions may be:
  //   CcdPERIODIC_10ms or CcdPROCESSOR_STILL_IDLE
  // see converse.h for the full listing
  aggregator(int msgThreshold, double flushTimeout,
             const endpoint_fn_t& endpoint, bool nodeLevel = false,
             int ccsCondition = CcdIGNOREPE)
      : mNodeLevel(nodeLevel),
        mMsgThreshold(msgThreshold),
        mFlushTimeout(flushTimeout) {
    mEndpoint = register_endpoint_fn(endpoint, nodeLevel);

    if (mNodeLevel) {
      nElements = CkNumNodes();
      mQueueLocks.resize(nElements);
    } else {
      nElements = CkNumPes();
    }

    mQueues.resize(nElements);
    mLastFlush.resize(nElements);

    if (ccsCondition != CcdIGNOREPE) {
      CcdCallOnConditionKeep(ccsCondition,
                             reinterpret_cast<CcdVoidFn>(&_on_condition<Ts...>),
                             this);
    }
  }

  inline bool timed_out(const int& pe) {
    auto& last = mLastFlush[pe];
    if (last == 0.0) {
      last = CkWallTimer();
      return false;
    } else {
      return (CkWallTimer() - last) >= mFlushTimeout;
    }
  }

  inline bool should_flush(const int& pe) {
    return (mQueues[pe].size() >= static_cast<std::size_t>(mMsgThreshold)) || timed_out(pe);
  }

  void flush(const int& pe) {
    auto& queue = mQueues[pe];

    int ndLvl = static_cast<int>(mNodeLevel);
    int idx = static_cast<int>(mEndpoint);
    int nMsgs = static_cast<int>(queue.size());

    auto pupFn = [&](PUP::er& p) {
      p | ndLvl;
      p | idx;
      p | nMsgs;
      for (auto& msg : queue) {
        p | msg;
      }
      return p.size();
    };

    PUP::sizer ps;
    auto size = pupFn(ps);
    envelope* env = _allocEnv(CkEnvelopeType::ForBocMsg, size);
    PUP::toMem p((char*)EnvToUsr(env));
    if (pupFn(p) != size) {
      CkAbort("pup failure");
    }

    CmiSetHandler(env, CkpvAccess(_bundleIdx));
    if (mNodeLevel) {
      if (pe != CmiMyNode()) {
        CmiSyncNodeSendAndFree(pe, env->getTotalsize(), env);
      } else {
        CsdNodeEnqueue(env);
      }
    } else {
      CmiSyncSendAndFree(pe, env->getTotalsize(), reinterpret_cast<char*>(env));
    }

    mLastFlush[pe] = CkWallTimer();
    queue.clear();
  }

  void on_cond(void) {
    for (auto pe = 0; pe < nElements; pe++) {
      if (mNodeLevel) mQueueLocks[pe].lock();
      if (!mQueues[pe].empty() && timed_out(pe)) {
        flush(pe);
      }
      if (mNodeLevel) mQueueLocks[pe].unlock();
    }
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
    if (mNodeLevel) mQueueLocks[pe].lock();
    put_msg(pe, msg);
    if (should_flush(pe)) {
      flush(pe);
    }
    if (mNodeLevel) mQueueLocks[pe].unlock();
  }

 private:
  bool mNodeLevel;
  int mMsgThreshold;
  int nElements;
  double mFlushTimeout;
  endpoint_id_t mEndpoint;

  std::deque<double> mLastFlush;
  std::deque<msg_queue_t> mQueues;
  std::deque<std::mutex> mQueueLocks;

  inline void put_msg(const int& pe, CkMessage* msg) {
    mQueues[pe].emplace_back(msg);
  }
};
}

#endif
