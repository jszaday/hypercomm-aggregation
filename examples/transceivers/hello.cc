#include "hello.decl.h"

#include <cstdlib> 
#include <type_traits>

#include <hypercomm/routing.hpp>
#include <hypercomm/aggregation.hpp>
#include <hypercomm/registration.hpp>

#ifdef NODE_LEVEL
constexpr bool kNodeLevel = true;
#else
constexpr bool kNodeLevel = false;
#endif

#ifdef DIRECT_BUFFER
constexpr bool kDirectBuffer = true;
#else
constexpr bool kDirectBuffer = false;
#endif

#ifdef INLINE_SEND
constexpr bool kCopy2Msg = false;
#else
constexpr bool kCopy2Msg = true;
#endif

#ifdef DIRECT_ROUTE
constexpr bool kDirectRoute = true;
using router_t = aggregation::routing::direct;
#else
constexpr bool kDirectRoute = false;
using router_t = aggregation::routing::mesh<2>;
#endif

#ifdef RANDOMIZE_SENDS
constexpr bool kRandomizeSends = true;
#else
constexpr bool kRandomizeSends = false;
#endif

template <typename T>
class Transceivers : public CBase_Transceivers<T> {
  using buffer_t =
      typename std::conditional<kDirectBuffer, aggregation::direct_buffer,
                                aggregation::dynamic_buffer>::type;
  using aggregator_t = aggregation::aggregator<buffer_t, router_t, T>;
  std::unique_ptr<aggregator_t> aggregator;

  int nIters, nElements;
  std::atomic<int> nRecvd;

 public:
  Transceivers(int _nIters) : nIters(_nIters), nRecvd(0) {
    std::srand(static_cast<unsigned int>(CkWallTimer()));

    auto flushPeriod = nIters / 4;
    auto bufArg =
        kDirectBuffer
            ? flushPeriod * sizeof(double) * sizeof(aggregation::detail::header_)
            : flushPeriod;
    auto cutoff = kDirectBuffer ? 0.85 : 1.0;

    if (this->thisIndex == 0) {
      if (kDirectBuffer) {
        CkPrintf("[INFO] Using buffers of size %.3f KB.\n", bufArg / 1024.0);
      } else {
        CkPrintf("[INFO] Setting max messages to %zu.\n", bufArg);
      }
    }

    // a periodic condition is typically necessary for non-direct routing
    auto cond = CcdPROCESSOR_STILL_IDLE;

    aggregation::endpoint_fn_t fn;
    if (kCopy2Msg) {
      // Copy from the receive buffer, and send to a callback
      CkCallback endCb(CkIndex_Transceivers<T>::receive_value(T{}),
                       this->thisProxy[this->thisIndex]);
      fn = aggregation::copy2msg([endCb](void* msg) { endCb.send(msg); });
    } else {
      // PUP directly from the receive buffer
      fn = [this](const aggregation::msg_size_t& size, char* data) {
        PUP::detail::TemporaryObjectHolder<T> t;
        PUP::fromMemBuf(t, data, size);
        // call with [inline] semantics
        this->receive_value(t.t);
      };
    }

    aggregator = std::unique_ptr<aggregator_t>(
        new aggregator_t(bufArg, cutoff, 0.05, fn, kNodeLevel, cond));
    if (kNodeLevel) {
      nElements = CkNumNodes();
    } else {
      nElements = CkNumPes();
    }

    this->contribute(
      CkCallback(CkIndex_Transceivers<T>::send_values(), this->thisProxy));
  }

  void contribute_count(void) {
    this->contribute(sizeof(int), &nRecvd, CkReduction::sum_int, CkCallback(
      CkReductionTarget(Transceivers<T>, check_count),
      this->thisProxy[this->thisIndex]
    ));
  }

  void check_count(int sum) {
    auto nExpected = nIters * nElements * nElements;
    if (sum == nExpected) {
      CkPrintf("[INFO] All values received, done.\n");
      CkExit();
    } else {
      CkAbort("%d did not receive all expected messages (%d vs. %d)!\n",
              this->thisIndex, (int)nRecvd, nExpected);
    }
  }

  void receive_value(const T& f) { nRecvd++; }

  void send_values(void) {
    for (auto i = 0; i < nIters; i++) {
      for (auto j = 0; j < nElements; j++) {
        int dest = kRandomizeSends ? (std::rand() % nElements) : j;
        aggregator->send(dest, static_cast<T>((i + 1) * (j + 1)));
      }
    }
  }
};

class Main : public CBase_Main {
 public:
  Main(CkArgMsg* msg) {
    int nIters = 2048;
    CProxy_Transceivers<double> ts = CProxy_Transceivers<double>::ckNew(nIters);
    CkStartQD(CkCallback(CkIndex_Transceivers<double>::contribute_count(), ts));
  }
};

#define CK_TEMPLATES_ONLY
#include "hello.def.h"
#undef CK_TEMPLATES_ONLY
#include "hello.def.h"
