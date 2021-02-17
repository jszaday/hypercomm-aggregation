#include <ck.h>
#include <mutex>

namespace hypercomm {
using envelope_t = envelope*;

namespace detail {
namespace binary_tree_ {
inline int left_child(const int& i) { return (2 * i) + 1; }

inline int right_child(const int& i) { return (2 * i) + 2; }

inline int parent(const int& i) { return (i > 0) ? ((i - 1) / 2) : -1; }

inline int num_leaves(const int& n) { return (n + 1) / 2; }

inline int num_children(const int& n, const int& i) {
  return (left_child(i) < n) + (right_child(i) < n);
}
}
}

template <typename T = int>
struct ispace {
  ispace(T n) : t(RANGE), d{.range_ = {.start = 0, .end = n, .step = 1}} {}

  ispace(T start, T end, T step = 1)
      : t(RANGE), d{.range_ = {.start = start, .end = end, .step = step}} {}

  ispace(T n, T* is) : t(SPARSE), d{.sparse_ = {.n = n, .is = is}} {}

  std::size_t size(void) const {
    return sizeof(ispace<T>) + ((t == SPARSE) ? d.sparse_.n : 0) * sizeof(T);
  }

  T elements(void) const {
    switch (t) {
      case RANGE:
        return (d.range_.end / d.range_.step) -
               (d.range_.start / d.range_.step);
      case SPARSE:
        return d.sparse_.n;
      default:
        CkAbort("unknown ispace type");
    }
  }

  T position(const int& i) const {
    switch (t) {
      case RANGE:
        return (i - d.range_.start) / d.range_.step;
      case SPARSE:
        return *(std::find(d.sparse_.is, d.sparse_.is + d.sparse_.n, i));
      default:
        CkAbort("unknown ispace type");
    }
  }

  T index(const int& pos) const {
    switch (t) {
      case RANGE:
        return d.range_.start + (pos * d.range_.step);
      case SPARSE:
        return d.sparse_.is[pos];
      default:
        CkAbort("unknown ispace type");
    }
  }

 private:
  enum type_ : T { IGNORE, RANGE, SPARSE };
  union data_ {
    struct s_range_ {
      T start;
      T end;
      T step;
    } range_;
    struct s_sparse_ {
      T n;
      T* is;
    } sparse_;
  };
  type_ t;
  data_ d;
};

using callback_fn = std::function<void(envelope_t)>;

namespace reductions {
using merge_fn = std::function<envelope_t(envelope_t, envelope_t)>;
using redn_id_t = uint32_t;

namespace node {
using id_t = int;

namespace {
using redn_table_t =
    std::map<redn_id_t,
             std::tuple<callback_fn, merge_fn, ispace<id_t>, envelope_t, int>>;

CsvDeclare(std::mutex, redn_lock_);
CsvDeclare(redn_id_t, redn_count_);
CsvDeclare(redn_table_t, redn_table_);
CpvDeclare(int, recv_value_idx_);

redn_id_t create_redn_(const callback_fn& cb, const merge_fn& fn,
                       const ispace<id_t>& is) {
  CsvAccess(redn_lock_).lock();
  auto self = CsvAccess(redn_count_)++;
  (CsvAccess(redn_table_))
      .emplace(self, std::forward_as_tuple(
                         cb, fn, is, static_cast<envelope_t>(nullptr), 0));
  CsvAccess(redn_lock_).unlock();
  return self;
}

void recv_values_(void* impl_msg_) {
  auto env = static_cast<envelope_t>(impl_msg_);
  auto redn = CmiGetRedID(env);

  CsvAccess(redn_lock_).lock();
  auto& self = CsvAccess(redn_table_).at(redn);
  auto& next = std::get<3>(self);
  next = (std::get<1>(self))(env, next);
  auto nRecvd = ++(std::get<4>(self));
  CsvAccess(redn_lock_).unlock();

  const auto& is = std::get<2>(self);
  auto n = is.elements();
  auto i = is.position(CmiMyNode());

  const auto parent = detail::binary_tree_::parent(i);
  const auto nExptd = detail::binary_tree_::num_children(n, i) + 1;

  if (nRecvd != nExptd) {
    CmiAssert(nRecvd < nExptd &&
              "should not receive more messages than expected");
    return;
  } else if (parent >= 0) {
    CmiSetHandler(next, CpvAccess(recv_value_idx_));
    CmiSyncNodeSendAndFree(is.index(parent), next->getTotalsize(),
                           reinterpret_cast<char*>(next));
  } else {
    (std::get<0>(self))(next);
  }

  CsvAccess(redn_lock_).lock();
  CsvAccess(redn_table_).erase(redn);
  CsvAccess(redn_lock_).unlock();
}
}

void contribute(envelope* env, const merge_fn& fn,
                const ispace<id_t>& is = {CmiNumNodes()}) {
  // create a reduction for the redn
  auto hdl = CmiGetHandler(env);
  auto self = create_redn_([hdl](envelope_t env) {
    CmiSetHandler(env, hdl);
    CsdNodeEnqueue(env);
  }, fn, is);
  // set the reduction id and handler
  CmiSetRedID(env, self);
  CmiSetHandler(env, CpvAccess(recv_value_idx_));
  // send the message off to be handled
  CsdNodeEnqueue(env);
}

void contribute(envelope* env,
                const callback_fn& cb, const merge_fn& fn,
                const ispace<id_t>& is = {CmiNumNodes()}) {
  // create a reduction for the redn
  auto self = create_redn_(cb, fn, is);
  // set the reduction id and handler
  CmiSetRedID(env, self);
  CmiSetHandler(env, CpvAccess(recv_value_idx_));
  // send the message off to be handled
  CsdNodeEnqueue(env);
}

void initialize(void) {
  CsvInitialize(std::mutex, redn_lock_);
  CsvInitialize(redn_id_t, redn_count_);
  CsvInitialize(redn_table_t, redn_table_);

  CpvInitialize(int, recv_value_idx_);
  CpvAccess(recv_value_idx_) =
      CmiRegisterHandler(reinterpret_cast<CmiHandler>(&recv_values_));
}
}

void initialize(void) {
  node::initialize();
}
}
}
