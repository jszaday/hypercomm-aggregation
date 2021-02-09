#include "hello.decl.h"

#include <memory>
#include <vector>

#include <hypercomm/aggregation.hpp>

#ifdef NODE_LEVEL
bool kNodeLevel = true;
#else
bool kNodeLevel = false;
#endif

template<typename T>
class Transceivers : public CBase_Transceivers<T> {
  using aggregator_t = aggregation::aggregator<T>;
  std::unique_ptr<aggregator_t> aggregator;

  int nIters, nRecvd, nElements;

 public:
  Transceivers(int _nIters) : nIters(_nIters), nRecvd(0) {
    aggregator = std::unique_ptr<aggregator_t>(
        new aggregator_t(nIters / 4, 0.1, [this](void* msg) {
          CkFreeMsg(msg);
          this->receive_value(0.0);
        }, kNodeLevel));
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
      CkAbort("%d did not receive all expected messages (%d vs. %d)!\n", this->thisIndex, nRecvd, nExpected);
    }
  }

  void receive_value(const T& f) {
    nRecvd += 1;
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
