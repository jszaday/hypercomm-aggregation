#include "hello.decl.h"

#include <memory>
#include <vector>

#include <hypercomm/aggregation.hpp>

template<typename T>
class Transceivers : public CBase_Transceivers<T> {
  using aggregator_t = aggregation::aggregator<T>;
  std::unique_ptr<aggregator_t> aggregator;

  int nIters, nRecvd;

 public:
  Transceivers(int _nIters) : nIters(_nIters), nRecvd(0) {
    CkCallback endCb(CkIndex_Transceivers<T>::receive_value(0), this->thisProxy[this->thisIndex]);
    // each pe registers its aggregator (and endpoint)
    aggregator = std::unique_ptr<aggregator_t>(
        new aggregator_t(nIters / 4, 0.1, [endCb](void* msg) {
          endCb.send(msg);
        }));
  }

  void check_count(void) {
    if (nRecvd >= (nIters * CkNumPes())) {
      this->contribute(CkCallback(CkCallback::ckExit));
    } else {
      CkAbort("%d did not receive all expected messages!\n", CkMyPe());
    }
  }

  void receive_value(const T& f) {
    nRecvd += 1;
  }

  void send_values(void) {
    auto mine = CkMyPe();
    for (auto i = 0; i < nIters; i++) {
      for (auto j = 0; j < CkNumPes(); j++) {
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
