#ifndef __HYPERCOMM_REGISTRATION_HPP__
#define __HYPERCOMM_REGISTRATION_HPP__

#include <hypercomm/aggregation.hpp>

namespace aggregation {

CkpvDeclare(int, _bundleIdx);

namespace {

CkpvDeclare(endpoint_registry_t, endpoint_registry_);

void _bundle_handler(void* msg) {
  envelope* env = static_cast<envelope*>(msg);
  PUP::fromMem p(EnvToUsr(env));
  int nodeLevel, idx, nMsgs;
  p | nodeLevel;
  p | idx;
  p | nMsgs;
  const auto& reg = nodeLevel ? CkpvAccessOther(endpoint_registry_, 0)
                              : CkpvAccess(endpoint_registry_);
  if (static_cast<std::size_t>(idx) >= reg.size()) {
    CkAbort("invalid endpoint id");
  }
  const auto& fn = reg[idx];
  for (auto i = 0; i < nMsgs; i++) {
    msg_size_t size;
    p | size;
    if ((p.size() + size) > env->getUsersize()) {
      CkAbort("exceeded message bounds");
    } else {
      fn(size, p.get_current_pointer());
      p.advance(static_cast<std::size_t>(size));
    }
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

endpoint_id_t register_endpoint_fn(const endpoint_fn_t& fn, bool nodeLevel) {
  auto& reg = nodeLevel ? CkpvAccessOther(endpoint_registry_, 0)
                        : CkpvAccess(endpoint_registry_);
  reg.push_back(fn);
  return reg.size() - 1;
}
}

#endif