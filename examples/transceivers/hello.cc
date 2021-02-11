#include "hello.decl.h"

#include <hypercomm/aggregation.hpp>
#include <hypercomm/registration.hpp>
#include <type_traits>

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

template<typename T>
class Transceivers : public CBase_Transceivers<T> {
  using buffer_t = typename std::conditional<kDirectBuffer, aggregation::direct_buffer, aggregation::dynamic_buffer>::type;
  using aggregator_t = aggregation::aggregator<buffer_t, T>;
  std::unique_ptr<aggregator_t> aggregator;

  int nIters, nElements;
  std::atomic<int> nRecvd;

 public:
  Transceivers(int _nIters) : nIters(_nIters), nRecvd(0) {
    auto flushPeriod = nIters / 4;
    auto bufArg = kDirectBuffer ? flushPeriod * (sizeof(double) + sizeof(aggregation::msg_size_t)) + 3 * sizeof(aggregation::msg_size_t)
                                : flushPeriod;
    auto cutoff = kDirectBuffer ? 0.75 : 1.0;
    if (this->thisIndex == 0) {
      if (kDirectBuffer) {
        CkPrintf("[INFO] Using buffers of size %.3f KB.\n", bufArg / 1024.0);
      } else {
        CkPrintf("[INFO] Setting max messages to %zu.\n", bufArg);
      }
    }
    CkCallback endCb(CkIndex_Transceivers<T>::receive_value(0),
                     this->thisProxy[this->thisIndex]);
    aggregator = std::unique_ptr<aggregator_t>(
        new aggregator_t(nIters / 4, cutoff, 1.0, aggregation::copy2msg([endCb](void* msg) {
          endCb.send(msg);
        }), kNodeLevel));
    if (kNodeLevel) {
      nElements = CkNumNodes();
    } else {
      nElements = CkNumPes();
    }
  }

  void check_count(void) {
    auto nExpected = nIters * nElements;
    if (nRecvd >= nExpected) {
      this->contribute(CkCallback(CkCallback::ckExit));
    } else {
      CkAbort("%d did not receive all expected messages (%d vs. %d)!\n", this->thisIndex, (int)nRecvd, nExpected);
    }
  }

  void receive_value(const T& f) {
    nRecvd++;
  }

  void send_values(void) {
    for (auto i = 0; i < nIters; i++) {
      for (auto j = 0; j < nElements; j++) {
        aggregator->send(j, static_cast<T>((i + 1) * (j + 1)));
      }
    }
  }
};

class Main : public CBase_Main {
 public:
  Main(CkArgMsg* msg) {
    int nIters = 2048;
    CProxy_Transceivers<double> ts = CProxy_Transceivers<double>::ckNew(nIters);
    ts.send_values();
    CkStartQD(CkCallback(CkIndex_Transceivers<double>::check_count(), ts));
  }
};

#define CK_TEMPLATES_ONLY
#include "hello.def.h"
#undef CK_TEMPLATES_ONLY
#include "hello.def.h"
