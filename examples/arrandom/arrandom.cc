#include "arrandom.decl.h"

#include <cstdlib>
#include <type_traits>

#include <hypercomm/routing.hpp>
#include <hypercomm/aggregation.hpp>
#include <hypercomm/registration.hpp>

#ifdef DIRECT_BUFFER
constexpr bool kDirectBuffer = true;
#else
constexpr bool kDirectBuffer = false;
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

#ifndef SCALING_FACTOR
#define SCALING_FACTOR 4
#endif

constexpr bool kNodeLevel = false;
constexpr int kScalingFactor = SCALING_FACTOR;

struct PacketMsg : CMessage_PacketMsg {
  char* payload;
};

using base_t = aggregation::detail::aggregator_base_;
using aggregators_t = std::vector<std::shared_ptr<base_t>>;
CkpvDeclare(aggregators_t, aggregators_);

template <typename T>
class Transceivers : public CBase_Transceivers<T> {
  using buffer_t =
      typename std::conditional<kDirectBuffer, aggregation::direct_buffer,
                                aggregation::dynamic_buffer>::type;

  using val_aggregator_t =
      aggregation::array_aggregator<buffer_t, router_t, T>;
  std::shared_ptr<val_aggregator_t> val_aggregator;

  using msg_aggregator_t =
      aggregation::array_aggregator<buffer_t, router_t, PacketMsg*>;
  std::shared_ptr<msg_aggregator_t> msg_aggregator;

  int nIters, nElements, nRecvd, nExpected;

 public:
  Transceivers(CkMigrateMessage*) {}

  Transceivers(int _nIters, int _nElements)
      : nIters(_nIters), nElements(_nElements), nRecvd(0), nExpected(nIters * nElements) {
    std::srand(static_cast<unsigned int>(CkWallTimer()));

    this->usesAtSync = true;
    this->setMigratable(true);

    this->initialize_aggregators_(false);

    this->contribute(
        CkCallback(CkIndex_Transceivers<T>::send_values(), this->thisProxy));
  }

  void contribute_count(void) {
    this->contribute(sizeof(int), &nRecvd, CkReduction::sum_int,
                     CkCallback(CkReductionTarget(Transceivers<T>, check_count),
                                this->thisProxy[0]));
  }

  void check_count(int sum) {
    if (sum == nElements * nExpected) {
      CkPrintf("[INFO] All values received, done.\n");
      CkExit();
    } else {
      CkAbort("%d did not receive all expected messages (%d vs. %d)!\n",
              this->thisIndex, (int)nRecvd, nExpected);
    }
  }

  inline void count_value(void) {
    if ((++nRecvd % (nExpected / 2)) == 0 && nRecvd < nExpected) {
      if (this->thisIndex == 0) {
        CkPrintf("[INFO] Invoking at sync.\n");
      }
      this->AtSync();
    }
  }

  void receive_value(const T& f) { this->count_value(); }

  void receive_value(PacketMsg* msg) {
    this->count_value();

    delete msg;
  }

  void send_values(void) {
    for (auto i = 0; i < nIters; i++) {
      for (auto j = 0; j < nElements; j++) {
        auto dest = kRandomizeSends ? (std::rand() % nElements) : j;
        auto sendValue = kRandomizeSends ? (bool)(std::rand() % 2) : (j % 2 == 0);

        if (sendValue) {
          val_aggregator->send(this->thisProxy[dest],
                               static_cast<T>((i + 1) * (j + 1)));
        } else {
          auto sz = std::rand() % nIters + 1;
          auto msg = new (sz) PacketMsg();
          msg_aggregator->send(this->thisProxy[dest], msg);
        }
      }
    }
  }

  void pup(PUP::er& p) {
    p | nIters;
    p | nElements;
    p | nRecvd;
    p | nExpected;
  }

  virtual void ResumeFromSync(void) override {
    this->initialize_aggregators_(true);
  }

 private:
  void initialize_aggregators_(bool migration) {
    auto flushPeriod = nIters / 2;
    auto bufArg = kDirectBuffer
                      ? flushPeriod * (sizeof(double) +
                                       sizeof(aggregation::detail::header_))
                      : flushPeriod;
    auto cutoff = kDirectBuffer ? 0.85 : 1.0;

    if (this->thisIndex == 0 && !migration) {
      CkPrintf("[INFO] Created %d array elements.\n", nElements);

      if (kDirectBuffer) {
        CkPrintf("[INFO] Using buffers of size %.3f KB.\n", bufArg / 1024.0);
      } else {
        CkPrintf("[INFO] Setting max messages to %zu.\n", bufArg);
      }
    }

    // a periodic condition is typically necessary for non-direct routing
    auto cond = CcdPROCESSOR_STILL_IDLE;

    if (!CkpvInitialized(aggregators_) && !migration) {
      val_aggregator = std::make_shared<val_aggregator_t>(
          this->thisProxy, CkIndex_Transceivers<T>::receive_value(T{}), bufArg,
          cutoff, 0.05, kNodeLevel, cond);

      msg_aggregator = std::make_shared<msg_aggregator_t>(
          this->thisProxy,
          CkIndex_Transceivers<T>::receive_value((PacketMsg*)nullptr), bufArg,
          cutoff, 0.05, kNodeLevel, cond);

      CkpvInitialize(aggregators_t, aggregators_);
      CkpvAccess(aggregators_) = {
          std::static_pointer_cast<base_t>(val_aggregator),
          std::static_pointer_cast<base_t>(msg_aggregator)};
    } else {
      CkAssert(CkpvInitialized(aggregators_));

      val_aggregator = std::static_pointer_cast<val_aggregator_t>(
          CkpvAccess(aggregators_)[0]);
      msg_aggregator = std::static_pointer_cast<msg_aggregator_t>(
          CkpvAccess(aggregators_)[1]);
    }
  }
};

class Main : public CBase_Main {
 public:
  Main(CkArgMsg* msg) {
    int nElements = kScalingFactor * CkNumPes();
    int nIters = 2 * 1024;
    CProxy_Transceivers<double> ts =
        CProxy_Transceivers<double>::ckNew(nIters, nElements, nElements);
    CkStartQD(CkCallback(CkIndex_Transceivers<double>::contribute_count(), ts));
  }
};

#define CK_TEMPLATES_ONLY
#include "arrandom.def.h"
#undef CK_TEMPLATES_ONLY
#include "arrandom.def.h"
