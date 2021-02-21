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

using msg_size_t = std::uint32_t;

namespace detail {
struct header_ {
  int dest;
  std::uint32_t size;
};

struct aggregator_base_ {
  virtual void send(const int& pe, const msg_size_t& size, char* data) = 0;
  virtual void on_cond(void) = 0;
};
}
}

PUPbytes(aggregation::detail::header_);

namespace aggregation {

using endpoint_id_t = std::size_t;
using msg_queue_t = std::deque<std::pair<msg_size_t, std::shared_ptr<char>>>;
using endpoint_fn_t = std::function<void(const msg_size_t&, char*)>;

CkpvExtern(int, bundle_idx_);

template <typename Fn>
endpoint_fn_t copy2msg(const Fn& fn) {
  return [fn](const msg_size_t& size, char* buffer) {
    auto msg = CkAllocateMarshallMsg(size);
    std::copy(buffer, buffer + size, msg->msgBuf);
    return fn(msg);
  };
}

namespace analytics {
extern void tally_flush(const endpoint_id_t& id, const float& utilization);
}
extern endpoint_id_t register_endpoint_fn(detail::aggregator_base_* self,
                                          const endpoint_fn_t& fn,
                                          bool nodeLevel);

class direct_buffer;
class dynamic_buffer;

template <typename Buffer, typename Router, typename... Ts>
struct aggregator;

template <typename Buffer, typename Router, typename... Ts>
struct aggregator : public detail::aggregator_base_ {
  using buffer_arg_t = typename Buffer::arg_t;
  aggregator(const buffer_arg_t& arg, double utilizationCap,
             double flushTimeout, const endpoint_fn_t& endpoint,
             bool nodeLevel = false, int ccsCondition = CcdIGNOREPE)
      : aggregator(arg, utilizationCap, flushTimeout, endpoint, nodeLevel,
                   nodeLevel, ccsCondition) {}

  // example conditions may be:
  //   CcdPERIODIC_10ms or CcdPROCESSOR_STILL_IDLE
  // see converse.h for the full listing
  aggregator(const buffer_arg_t& arg, double utilizationCap,
             double flushTimeout, const endpoint_fn_t& endpoint, bool nodeLevel,
             bool enableLocks, int ccsCondition)
      : mNodeLevel(nodeLevel),
        nElements(CkNumNodes()),
        mUtilizationCap(utilizationCap),
        mFlushTimeout(flushTimeout),
        mBuffer(arg, 3 * sizeof(int), nElements),
        mRouter(nElements),
        mLastFlush(nElements),
        mLocksEnabled(enableLocks),
        mQueueLocks(mLocksEnabled ? nElements : 0) {
    mEndpoint = register_endpoint_fn(this, endpoint, nodeLevel);

    for (auto i = 0; i < nElements; i += 1) {
      mCounts.emplace_back(0);
    }

    if (ccsCondition != CcdIGNOREPE) {
      CcdCallOnConditionKeep(ccsCondition,
                             reinterpret_cast<CcdVoidFn>(&on_condition_), this);
    }
  }

  inline bool timed_out(const int& node) {
    auto& last = mLastFlush[node];
    if (last == 0.0) {
      last = CkWallTimer();
      return false;
    } else {
      return (CkWallTimer() - last) >= mFlushTimeout;
    }
  }

  inline bool should_flush(const int& node) {
    return (mBuffer.utilization(node) >= mUtilizationCap) || timed_out(node);
  }

  void flush(const int& node) {
    int ndLvl = static_cast<int>(mNodeLevel);
    int idx = static_cast<int>(mEndpoint);
    int nMsgs = mCounts[node].load();
#ifdef HYPERCOMM_TRACING_ON
    analytics::tally_flush(idx, mBuffer.utilization(node));
#endif
    auto env = mBuffer.flush(node);
    PUP::toMem p(EnvToUsr(env));

    p | ndLvl;
    p | idx;
    p | nMsgs;

    CmiSetHandler(env, CkpvAccess(bundle_idx_));

    if (node == CmiMyNode()) {
      CsdNodeEnqueue(env);
    } else {
      CmiSyncNodeSendAndFree(node, env->getTotalsize(),
                             reinterpret_cast<char*>(env));
    }

    mLastFlush[node] = CkWallTimer();
    mCounts[node].store(0);
  }

  virtual void on_cond(void) override {
    CkAssert(!mLocksEnabled || !mQueueLocks.empty());
    for (auto pe = 0; pe < nElements; pe++) {
      if (mLocksEnabled) mQueueLocks[pe].lock();
      if (mCounts[pe] != 0 && timed_out(pe)) {
        flush(pe);
      }
      if (mLocksEnabled) mQueueLocks[pe].unlock();
    }
  }

  template <typename Fn>
  inline void send(const int& dest, const msg_size_t& size, const Fn& pupFn) {
    const auto destNode = mNodeLevel ? dest : CkNodeOf(dest);
    const auto mine = CkMyNode();
    // query the router about where we should send the value
    auto next = mRouter.next(mine, destNode);
    // route it directly to our send queue if it would go to us
    if (next == mine) next = destNode;
    CkAssert(next < nElements && "invalid destination");
    detail::header_ header = {.dest = dest, .size = size};
    const auto tsize = sizeof(header) + size;
    QdCreate(1);
    if (mLocksEnabled) mQueueLocks[next].lock();
    auto buff = mBuffer.get_buffer(next, tsize);
    if (buff == nullptr) {
      // assume a capacity failure, and flush
      flush(next);
      // then retry
      buff = mBuffer.get_buffer(next, tsize);
      // abort if we fail again
      if (buff == nullptr) {
        CkAbort("failed to acquire allocation after flush");
      }
    }
    // count the message once we get an allocation
    mCounts[next]++;
    PUP::toMem p(buff);
    p | header;
    pupFn(p);
    if (p.size() != tsize) {
      CkAbort("pup failure");
    }
    if (should_flush(next)) flush(next);
    if (mLocksEnabled) mQueueLocks[next].unlock();
  }

  virtual void send(const int& dest, const msg_size_t& size,
                    char* data) override {
    send(dest, size,
         [&](PUP::er& p) { p(data, static_cast<std::size_t>(size)); });
  }

  void send(const int& dest, const Ts&... const_ts) {
    auto args = std::forward_as_tuple(const_cast<Ts&>(const_ts)...);
    auto size = static_cast<msg_size_t>(PUP::size(args));
    send(dest, size, [&](PUP::er& p) { p | args; });
  }

 private:
  bool mNodeLevel;
  bool mLocksEnabled;
  int nElements;
  double mUtilizationCap;
  double mFlushTimeout;
  endpoint_id_t mEndpoint;

  Buffer mBuffer;
  Router mRouter;
  std::deque<std::atomic<int>> mCounts;
  std::deque<double> mLastFlush;
  std::deque<std::mutex> mQueueLocks;

  static void on_condition_(void* self) {
    static_cast<detail::aggregator_base_*>(self)->on_cond();
  }
};

namespace detail {
template <typename... Ts>
using front_t = typename std::enable_if<
    sizeof...(Ts) >= 1,
    typename std::tuple_element<0, std::tuple<Ts...>>::type>::type;

template <typename... Ts>
constexpr bool is_message_t(void) {
  return (sizeof...(Ts) == 1) &&
         std::is_base_of<CkMessage, typename std::remove_pointer<
                                        front_t<Ts...>>::type>::value;
}

template <typename... Ts>
using wrap_msg_t = typename std::conditional<is_message_t<Ts...>(),
                                             CkMarshalledMessage, Ts...>::type;
}

template <typename Buffer, typename Router, typename... Ts>
struct array_aggregator : public aggregator<Buffer, Router, int, CkArrayIndex,
                                            detail::wrap_msg_t<Ts...>> {
  using buffer_arg_t = typename Buffer::arg_t;
  using parent_t =
      aggregator<Buffer, Router, int, CkArrayIndex, detail::wrap_msg_t<Ts...>>;

  array_aggregator(const CkArrayID& id, int entryIndex, const buffer_arg_t& arg,
                   double utilizationCap, double flushTimeout,
                   bool enableLocks = false, int ccsCondition = CcdIGNOREPE)
      : parent_t(arg, utilizationCap, flushTimeout,
                 make_endpoint_fn_(id, entryIndex), false, enableLocks,
                 ccsCondition),
        mArray(static_cast<CkArray*>(_localBranch(id))) {
    CkAssert(mArray != nullptr);
  }

  inline void send(const CkArrayIndex& idx, const Ts&... const_ts) {
    // use tag dispatching based on whether we're in message mode or not
    this->send(idx, const_ts...,
               typename std::integral_constant<bool, message_mode_>::type());
  }

  inline void send(const CkArrayIndex& idx, const Ts&... const_ts,
                   std::false_type) {
    parent_t::send(CkMyPe(), mArray->lastKnown(idx), idx, const_ts...);
  }

  inline void send(const CkArrayIndex& idx, const Ts&... const_ts,
                   std::true_type) {
    parent_t::send(CkMyPe(), mArray->lastKnown(idx), idx,
                   CkMarshalledMessage{const_ts...});
  }

  inline void send(const CProxyElement_ArrayElement& element,
                   const Ts&... const_ts) {
    CkAssert(CkArrayID{mArray->getGroupID()} == (CkArrayID)element);
    this->send(element.ckGetIndex(), const_ts...);
  }

 private:
  CkArray* mArray;

  static constexpr bool message_mode_ = detail::is_message_t<Ts...>();

  static inline endpoint_fn_t make_endpoint_fn_(const CkArrayID& id,
                                                const int& entryIndex) {
    return [id, entryIndex](const msg_size_t& sz, char* begin) {
      // tuples are currently pup'd in reverse so we grab the idx from the end
      const auto end = begin + sz - sizeof(CkArrayIndex) - sizeof(int);
      const auto& idx = *(reinterpret_cast<CkArrayIndex*>(end));
      const auto& src = *(reinterpret_cast<int*>(end + sizeof(CkArrayIndex)));
      auto& arr = *(static_cast<CkArray*>(_localBranch(id)));

      CkMessage* msg;
      if (message_mode_) {
        PUP::fromMem p(begin);
        CkPupMessage(p, reinterpret_cast<void**>(&msg), 1);
      } else {
        msg = CkAllocateMarshallMsg(sz - sizeof(CkArrayIndex));
        std::copy(begin, end, ((CkMarshallMsg*)msg)->msgBuf);
      }

      auto env = UsrToEnv(static_cast<void*>(msg));
      env->setArrayMgr(id);
      env->getsetArraySrcPe() = src;
      env->setEpIdx(entryIndex);
      env->getsetArrayHops() = 2 * (arr.lookup(idx) == nullptr);
      CkSetMsgArrayIfNotThere(msg);

      arr.deliver(msg, idx, CkDeliver_queue, 0);
    };
  }
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
      auto start = static_cast<char*>(EnvToUsr(env));
      mQueues.emplace_back(env, start, start + mHeaderSize);
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
    env->setUsersize(size(pe));
    std::get<0>(mQueues[pe]) =
        _allocEnv(CkEnvelopeType::ForBocMsg, mBufferSize);
    std::get<1>(mQueues[pe]) =
        static_cast<char*>(EnvToUsr(std::get<0>(mQueues[pe])));
    std::get<2>(mQueues[pe]) = std::get<1>(mQueues[pe]) + mHeaderSize;
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
