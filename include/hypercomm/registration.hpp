#ifndef __HYPERCOMM_REGISTRATION_HPP__
#define __HYPERCOMM_REGISTRATION_HPP__

#include <hypercomm/aggregation.hpp>

namespace aggregation {

CkpvDeclare(int, _bundleIdx);

namespace {
using aggregator_t = detail::aggregator_base_*;
using endpoint_registry_t = std::vector<std::pair<aggregator_t, endpoint_fn_t>>;

CkpvDeclare(endpoint_registry_t, endpoint_registry_);

void _bundle_handler(void* msg) {
  envelope* env = static_cast<envelope*>(msg);
  PUP::fromMem p(EnvToUsr(env));
  int nodeLevel, idx, nMsgs;
  p | nodeLevel;
  p | idx;
  p | nMsgs;
  const auto mine = nodeLevel ? CkMyNode() : CkMyPe();
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
    if ((p.size() + size) > env->getUsersize()) {
      CkAbort("exceeded message bounds");
    } else if (dest == mine) {
      fn(size, p.get_current_pointer());
    } else {
      self->send(dest, size, p.get_current_pointer());
    }
    p.advance(static_cast<std::size_t>(size));
  }
  QdProcess(nMsgs);
  CmiFree(env);
}
}

void initialize(void) {
  CkpvInitialize(endpoint_registry_t, endpoint_registry_);
  CkpvInitialize(int, _bundleIdx);
  CkpvAccess(_bundleIdx) = CmiRegisterHandler((CmiHandler)_bundle_handler);
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
