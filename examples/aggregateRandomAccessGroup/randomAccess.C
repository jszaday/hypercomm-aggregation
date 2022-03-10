#include <hypercomm/routing.hpp>
#include <hypercomm/aggregation.hpp>
#include <hypercomm/registration.hpp>

typedef CmiUInt8 dtype;
#include "randomAccess.decl.h"
#include "TopoManager.h"

#define POLY 0x0000000000000007ULL
#define PERIOD 1317624576693539401LL

#ifdef DIRECT_ROUTE
constexpr bool kDirectRoute = true;
using router_t = aggregation::routing::direct;
constexpr auto kCondition = CcdPROCESSOR_STILL_IDLE;
#else
constexpr bool kDirectRoute = false;
using router_t = aggregation::routing::mesh<2>;
// a periodic condition may be required for indirect routing
// (so consider testing that if this doesn't show benefits!)
constexpr auto kCondition = CcdPROCESSOR_STILL_IDLE;
#endif

#ifdef DYNAMIC_BUFFER
constexpr bool kDirectBuffer = false;
#else
constexpr bool kDirectBuffer = true;
#endif

#ifdef USE_MESSAGES
constexpr bool kCopy2Msg = true;
#else
constexpr bool kCopy2Msg = false;
#endif

using buffer_t =
    typename std::conditional<kDirectBuffer, aggregation::direct_buffer,
                              aggregation::dynamic_buffer>::type;
using aggregator_t = aggregation::aggregator<buffer_t, router_t, dtype>;

// log_2 of the table size per PE
int N;
// The local table size
CmiInt8 localTableSize;
// Handle to the test driver (chare)
CProxy_TestDriver driverProxy;
// Max number of keys buffered by communication library
const int numMsgsBuffered = 1024;

CmiUInt8 HPCC_starts(CmiInt8 n);

class TestDriver : public CBase_TestDriver {
private:
  CProxy_Updater  updater_group;
  double starttime;
  CmiInt8 tableSize;

public:
  TestDriver(CkArgMsg* args) {
    N = atoi(args->argv[1]);
    localTableSize = 1l << N;
    tableSize = localTableSize * CkNumPes();

    CkPrintf("Global table size   = 2^%d * %d = %" PRId64 " words\n",
             N, CkNumPes(), tableSize);
    CkPrintf("Number of processors = %d\nNumber of updates = %" PRId64 "\n",
             CkNumPes(), 4 * tableSize);

    driverProxy = thishandle;
    // Create the chares storing and updating the global table
    updater_group   = CProxy_Updater::ckNew();
    int dims[2] = {CkNumNodes(), CkNumPes() / CkNumNodes()};
    CkPrintf("Aggregation topology: %d %d\n", dims[0], dims[1]);

    delete args;
  }

  void start() {
    starttime = CkWallTimer();
    CkCallback endCb(CkIndex_TestDriver::startVerificationPhase(), thisProxy);
    updater_group.generateUpdates();
    CkStartQD(endCb);
  }

  void startVerificationPhase() {
    double update_walltime = CkWallTimer() - starttime;
    double gups = 1e-9 * tableSize * 4.0/update_walltime;

    CkPrintf("CPU time used = %.6f seconds\n", update_walltime);
    CkPrintf("%.9f Billion(10^9) Updates    per second [GUP/s]\n", gups);
    CkPrintf("%.9f Billion(10^9) Updates/PE per second [GUP/s]\n",
             gups / CkNumPes());

    // Repeat the update process to verify
    // At the end of the second update phase, check the global table
    //  for errors in Updater::checkErrors()
    CkCallback endCb(CkIndex_Updater::checkErrors(), updater_group);
    updater_group.generateUpdates();
    CkStartQD(endCb);
  }

  void reportErrors(CmiInt8 globalNumErrors) {
    CkPrintf("Found %" PRId64 " errors in %" PRId64 " locations (%s).\n", globalNumErrors,
             tableSize, globalNumErrors <= 0.01 * tableSize ?
             "passed" : "failed");
    CkExit();
  }
};

// Charm++ Group (i.e. one chare on each PE)
// Each chare: owns a portion of the global table
//             performs updates on its portion
//             generates random keys and sends them to the appropriate chares
class Updater : public CBase_Updater {
private:
  CmiUInt8 *HPCC_Table;
  CmiUInt8 globalStartmyProc;
  std::unique_ptr<aggregator_t> aggregator;

public:
  Updater() {
    // Compute table start for this chare
    globalStartmyProc = CkMyPe() * localTableSize;
    // Create table
    HPCC_Table = (CmiUInt8*)malloc(sizeof(CmiUInt8) * localTableSize);
    // Initialize
    for(CmiInt8 i = 0; i < localTableSize; i++)
      HPCC_Table[i] = i + globalStartmyProc;
    // Create the aggregator
    this->makeAggregator();
    // Contribute to a reduction to signal the end of the setup phase
    contribute(CkCallback(CkReductionTarget(TestDriver, start), driverProxy));
  }

  inline void makeAggregator(void) {
    auto flushTimeout = 1.0 / 256;
    auto flushPeriod = localTableSize;
    auto bufArg =
        kDirectBuffer
            ? flushPeriod * (sizeof(double) + sizeof(aggregation::detail::header_))
            : flushPeriod;
    auto cutoff = kDirectBuffer ? 0.85 : 1.0;

    if (this->thisIndex == 0) {
      if (kDirectBuffer) {
        CkPrintf("[INFO] Using buffers of size %.3f KB.\n", bufArg / 1024.0);
      } else {
        CkPrintf("[INFO] Setting max messages to %zu.\n", bufArg);
      }
    }

    aggregation::endpoint_fn_t fn;
    if (kCopy2Msg) {
      // Copy from the receive buffer, and send to a callback
      CkCallback endCb(CkIndex_Updater::insertData(dtype{}),
                       this->thisProxy[this->thisIndex]);
      fn = aggregation::copy2msg([endCb](void* msg) { endCb.send(msg); });
    } else {
      // PUP directly from the receive buffer
      fn = [this](const aggregation::msg_size_t& size, char* data) {
        PUP::detail::TemporaryObjectHolder<dtype> t;
        PUP::fromMemBuf(t, data, size);
        // call with [inline] semantics
        this->insertData(t.t);
      };
    }

    aggregator = std::unique_ptr<aggregator_t>(
        new aggregator_t(bufArg, cutoff, flushTimeout, fn, false, kCondition));
  }

  // Communication library calls this to deliver each randomly generated key
  inline void insertData(const dtype  &key) {
    CmiInt8  localOffset = key & (localTableSize - 1);
    // Apply update
    HPCC_Table[localOffset] ^= key;
  }

  void generateUpdates() {
    CmiUInt8 key = HPCC_starts(4 * globalStartmyProc);

    // Generate this chare's share of global updates
    for(CmiInt8 i = 0; i < 4 * localTableSize; i++) {
      key = key << 1 ^ ((CmiInt8) key < 0 ? POLY : 0);
      int destinationIndex = key >> N & CkNumPes() - 1;
      // Submit generated key to chare owning that portion of the table
      this->aggregator->send(destinationIndex, key);
    }
  }

  void checkErrors() {
    CmiInt8 numErrors = 0;
    // The second verification phase should have returned the table
    //  to its initial state
    for (CmiInt8 j = 0; j < localTableSize; j++)
      if (HPCC_Table[j] != j + globalStartmyProc)
        numErrors++;
    // Sum the errors observed across the entire system
    contribute(sizeof(CmiInt8), &numErrors, CkReduction::sum_long,
               CkCallback(CkReductionTarget(TestDriver, reportErrors),
                          driverProxy));
  }
};


/** random number generator */
CmiUInt8 HPCC_starts(CmiInt8 n) {
  int i, j;
  CmiUInt8 m2[64];
  CmiUInt8 temp, ran;
  while (n < 0) n += PERIOD;
  while (n > PERIOD) n -= PERIOD;
  if (n == 0) return 0x1;
  temp = 0x1;
  for (i = 0; i < 64; i++) {
    m2[i] = temp;
    temp = temp << 1 ^ ((CmiInt8) temp < 0 ? POLY : 0);
    temp = temp << 1 ^ ((CmiInt8) temp < 0 ? POLY : 0);
  }
  for (i = 62; i >= 0; i--)
    if (n >> i & 1)
      break;

  ran = 0x2;
  while (i > 0) {
    temp = 0;
    for (j = 0; j < 64; j++)
      if (ran >> j & 1)
        temp ^= m2[j];
    ran = temp;
    i -= 1;
    if (n >> i & 1)
      ran = ran << 1 ^ ((CmiInt8) ran < 0 ? POLY : 0);
  }
  return ran;
}

#include "randomAccess.def.h"
