#ifndef __HYPERCOMM_AGGREGATION_HPP__
#define __HYPERCOMM_AGGREGATION_HPP__

#include <map>
#include <ck.h>
#include <deque>
#include <mutex>
#include <tuple>
#include <numeric>
#include <functional>

namespace aggregation {
using endpoint_id_t = std::size_t;
using msg_size_t = int;
using msg_queue_t = std::deque<std::pair<msg_size_t, std::shared_ptr<char>>>;
using endpoint_fn_t = std::function<void(const msg_size_t&, char*)>;
using endpoint_registry_t = std::vector<endpoint_fn_t>;

CkpvExtern(int, _bundleIdx);

template <typename Fn>
endpoint_fn_t copy2msg(const Fn& fn) {
  return [fn](const msg_size_t& size, char* buffer) {
    auto msg = CkAllocateMarshallMsg(size);
    std::copy(buffer, buffer + size, msg->msgBuf);
    return fn(msg);
  };
}

extern endpoint_id_t register_endpoint_fn(const endpoint_fn_t& fn,
                                          bool nodeLevel);

class direct_buffer;
class dynamic_buffer;

template <typename Buffer, typename... Ts>
struct aggregator;

namespace {
template <typename... Ts>
static void _on_condition(void* self) {
  static_cast<aggregator<Ts...>*>(self)->on_cond();
}
}

template <typename Buffer, typename... Ts>
struct aggregator {
  using buffer_arg_t = typename Buffer::arg_t;
  // example conditions may be:
  //   CcdPERIODIC_10ms or CcdPROCESSOR_STILL_IDLE
  // see converse.h for the full listing
  aggregator(const buffer_arg_t& arg, double utilizationCap,
             double flushTimeout, const endpoint_fn_t& endpoint,
             bool nodeLevel = false, int ccsCondition = CcdIGNOREPE)
      : mNodeLevel(nodeLevel),
        nElements(mNodeLevel ? CkNumNodes() : CkNumPes()),
        mUtilizationCap(utilizationCap),
        mFlushTimeout(flushTimeout),
        mBuffer(arg, 3 * sizeof(int), nElements) {
    mEndpoint = register_endpoint_fn(endpoint, nodeLevel);

    mCounts.resize(nElements);
    if (mNodeLevel) mQueueLocks.resize(nElements);

    if (ccsCondition != CcdIGNOREPE) {
      CcdCallOnConditionKeep(
          ccsCondition,
          reinterpret_cast<CcdVoidFn>(&_on_condition<Buffer, Ts...>), this);
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
    return (mBuffer.utilization(pe) >= mUtilizationCap) || timed_out(pe);
  }

  void flush(const int& pe) {
    int ndLvl = static_cast<int>(mNodeLevel);
    int idx = static_cast<int>(mEndpoint);
    int nMsgs = mCounts[pe].load();
    auto env = mBuffer.flush(pe);
    PUP::toMem p(EnvToUsr(env));

    p | ndLvl;
    p | idx;
    p | nMsgs;

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
    mCounts[pe].store(0);
  }

  void on_cond(void) {
    for (auto pe = 0; pe < nElements; pe++) {
      if (mNodeLevel) mQueueLocks[pe].lock();
      if (mCounts[pe] != 0 && timed_out(pe)) {
        flush(pe);
      }
      if (mNodeLevel) mQueueLocks[pe].unlock();
    }
  }

  void send(const int& pe, const Ts&... const_ts) {
    auto args = std::forward_as_tuple(const_cast<Ts&>(const_ts)...);
    auto size = static_cast<msg_size_t>(PUP::size(args));
    auto tsize = sizeof(size) + size;
    QdCreate(1);
    if (mNodeLevel) mQueueLocks[pe].lock();
    auto buff = mBuffer.get_buffer(pe, tsize);
    if (buff == nullptr) {
      // assume a capacity failure, and flush
      flush(pe);
      // then retry
      buff = mBuffer.get_buffer(pe, tsize);
      // abort if we fail again
      if (buff == nullptr) {
        CkAbort("failed to acquire allocation after flush");
      }
    }
    // count the message once we get an allocation
    mCounts[pe]++;
    PUP::toMem p(buff);
    p | size;
    p | args;
    if (p.size() != tsize) {
      CkAbort("pup failure");
    }
    if (should_flush(pe)) flush(pe);
    if (mNodeLevel) mQueueLocks[pe].unlock();
  }

 private:
  bool mNodeLevel;
  int nElements;
  double mUtilizationCap;
  double mFlushTimeout;
  endpoint_id_t mEndpoint;

  Buffer mBuffer;
  std::deque<std::atomic<int>> mCounts;
  std::deque<double> mLastFlush;
  std::deque<std::mutex> mQueueLocks;
};

class direct_buffer {
  // this tuple represents <envelope, start, current>
  std::vector<std::tuple<envelope*, char*, char*>> mQueues;
  std::size_t mHeaderSize;
  std::size_t mBufferSize;

 public:
  using arg_t = std::size_t;

  direct_buffer(const arg_t& bufferSize, const std::size_t& headerSize,
                const int& nPes)
      : mBufferSize(bufferSize), mHeaderSize(headerSize) {
    for (auto i = 0; i < nPes; i++) {
      auto env = _allocEnv(CkEnvelopeType::ForBocMsg, mBufferSize);
      auto start = static_cast<char*>(EnvToUsr(env)) + mHeaderSize;
      mQueues.emplace_back(env, start, start);
    }
  }

  ~direct_buffer() {
    for (auto& queue : mQueues) {
      CmiFree(std::get<0>(queue));
    }
  }

  char* get_buffer(const int& pe, const std::size_t& sz) {
    if ((size(pe) + sz) > mBufferSize) {
      return nullptr;
    } else {
      auto current = std::get<2>(mQueues[pe]);
      std::get<2>(mQueues[pe]) += sz;
      return current;
    }
  }

  envelope* flush(const int& pe) {
    envelope* env = std::get<0>(mQueues[pe]);
    env->setUsersize(size(pe) + mHeaderSize);
    std::get<0>(mQueues[pe]) =
        _allocEnv(CkEnvelopeType::ForBocMsg, mBufferSize);
    std::get<1>(mQueues[pe]) =
        static_cast<char*>(EnvToUsr(std::get<0>(mQueues[pe]))) + mHeaderSize;
    std::get<2>(mQueues[pe]) = std::get<1>(mQueues[pe]);
    return env;
  }

  inline std::size_t size(const int& pe) {
    return std::get<2>(mQueues[pe]) - std::get<1>(mQueues[pe]);
  }

  inline float utilization(const int& pe) {
    return size(pe) / ((float)mBufferSize);
  }
};

class dynamic_buffer {
  using uptr_t = std::unique_ptr<char, decltype(std::free)*>;
  using queue_t = std::deque<std::pair<std::size_t, uptr_t>>;
  std::deque<queue_t> mQueues;
  std::size_t mHeaderSize;
  std::size_t mMaxMsgs;

 public:
  using arg_t = std::size_t;

  dynamic_buffer(const arg_t& maxMsgs, const std::size_t& headerSize,
                 const int& nPes)
      : mMaxMsgs(maxMsgs), mHeaderSize(headerSize) {
    mQueues.resize(nPes);
  }

  char* get_buffer(const int& pe, const std::size_t& size) {
    if (mQueues[pe].size() >= mMaxMsgs) {
      return nullptr;
    } else {
      auto buffer = static_cast<char*>(std::malloc(size));
      mQueues[pe].emplace_back(size, uptr_t{buffer, std::free});
      return buffer;
    }
  }

  envelope* flush(const int& pe) {
    auto& queue = mQueues[pe];
    auto size =
        mHeaderSize +
        std::accumulate(queue.begin(), queue.end(), static_cast<std::size_t>(0),
                        [](std::size_t a, const queue_t::value_type& b) {
                          return a + b.first;
                        });
    auto env = _allocEnv(CkEnvelopeType::ForBocMsg, size);
    PUP::toMem p((char*)EnvToUsr(env));
    p.advance(mHeaderSize);
    for (auto& item : queue) {
      p(item.second.get(), item.first);
    }
    if (p.size() != size) {
      CkAbort("pup failure");
    }
    queue.clear();
    return env;
  }

  float utilization(const int& pe) {
    return ((float)mQueues[pe].size()) / mMaxMsgs;
  }
};
}

#endif
