#ifndef __HYPERCOMM_REGISTRATION_HPP__
#define __HYPERCOMM_REGISTRATION_HPP__

#include <hypercomm/aggregation.hpp>
#ifdef HYPERCOMM_TRACING_ON
#include <hypercomm/analytics.hpp>
#endif

namespace aggregation {

CkpvDeclare(int, bundle_idx_);

namespace {
constexpr int piranha_magic_nbr_ = 0x12345678;
using aggregator_t = detail::aggregator_base_*;
using endpoint_registry_t = std::vector<std::pair<aggregator_t, endpoint_fn_t>>;

CkpvDeclare(endpoint_registry_t, endpoint_registry_);

void unbundle_(envelope* env);

void bundle_handler_(void* msg) {
  envelope* env = static_cast<envelope*>(msg);
  int& nodeLevel = *(reinterpret_cast<int*>(EnvToUsr(env)));
#if CMK_SMP
  auto nRanks = CmiNodeSize(CmiMyNode());
  if (!nodeLevel && nRanks > 1) {
    // set node level to indicate the receivers are piranhas
    nodeLevel = piranha_magic_nbr_;
    // and let 'em rip
    for (auto rank = 0; rank < nRanks; rank++) {
      if (rank < (nRanks - 1)) CmiReference(env);
      CmiPushPE(rank, env);
    }
  } else {
#endif
    unbundle_(env);
#if CMK_SMP
  }
#endif
}

void unbundle_(envelope* env) {
  PUP::fromMem p(EnvToUsr(env));

  int nodeLevel, idx, nMsgs;
  p | nodeLevel;
  p | idx;
  p | nMsgs;

  auto piranha = nodeLevel == piranha_magic_nbr_;
  if (piranha) nodeLevel = 0;

  const auto node = CkMyNode();
  const auto mine = nodeLevel ? node : CkMyPe();
  const auto rank = nodeLevel ? -1 : CkRankOf(mine);
  const auto nRanks = CmiNodeSize(node);

  const auto& reg = nodeLevel ? CkpvAccessOther(endpoint_registry_, 0)
                              : CkpvAccess(endpoint_registry_);
  if (static_cast<std::size_t>(idx) >= reg.size()) {
    CkAbort("invalid endpoint id on rank %d of %d", CkRankOf(mine), CkMyNode());
  }
  const auto& self = reg[idx].first;
  const auto& fn = reg[idx].second;
  for (auto i = 0; i < nMsgs; i++) {
    detail::header_ hdr;
    p | hdr;
    const auto& dest = hdr.dest;
    const auto& size = hdr.size;
#ifdef HYPERCOMM_TRACING_ON
    analytics::tally_message(idx, size);
#endif
    if ((p.size() + size) > env->getUsersize()) {
      CkAbort("exceeded message bounds");
    } else if (dest == mine) {
      fn(size, p.get_current_pointer());
      QdProcess(1);
    } else if (!piranha ||
               (node != CkNodeOf(dest) && rank == (dest % nRanks))) {
      self->send(dest, size, p.get_current_pointer());
      QdProcess(1);
    }
    p.advance(static_cast<std::size_t>(size));
  }
  CmiFree(env);
}
}

void initialize(void) {
  CkpvInitialize(endpoint_registry_t, endpoint_registry_);
  CkpvInitialize(int, bundle_idx_);
  CkpvAccess(bundle_idx_) =
      CmiRegisterHandler(reinterpret_cast<CmiHandler>(bundle_handler_));
#ifdef HYPERCOMM_TRACING_ON
  analytics::initialize();
#endif
}

endpoint_id_t register_endpoint_fn(aggregator_t self, const endpoint_fn_t& fn,
                                   bool nodeLevel) {
  auto& reg = nodeLevel ? CkpvAccessOther(endpoint_registry_, 0)
                        : CkpvAccess(endpoint_registry_);
  reg.emplace_back(self, fn);
  return reg.size() - 1;
}
}

#endif
