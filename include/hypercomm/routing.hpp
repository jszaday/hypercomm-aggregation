#ifndef __HYPERCOMM_ROUTING_HPP__
#define __HYPERCOMM_ROUTING_HPP__

#include <ck.h>
#include <array>
#include <cmath>
#include <numeric>
#include <cinttypes>
#include <functional>

namespace aggregation {
namespace routing {

class direct {
  direct(const std::size_t _) {}
  std::size_t next(const std::size_t& _, const std::size_t& to) const {
    return to;
  }
};

template <std::size_t ND>
class mesh {
  static_assert(ND == 2, "only 2d meshes are currently implemented");

  using coordinate_t = std::array<std::size_t, ND>;

  std::size_t nElements, nNodes;
  coordinate_t mShape;

  inline std::size_t accum_helper(std::size_t n) const {
    return std::accumulate(mShape.begin(), mShape.begin() + n, 1,
                           std::multiplies<std::size_t>());
  }

  inline std::size_t factor_helper(std::size_t n) const {
    return std::accumulate(mShape.begin() + 1, mShape.begin() + (ND - n), 1,
                           std::multiplies<std::size_t>());
  }

 public:
  mesh(const std::size_t& _nElements) : nElements(_nElements) {
    mShape[0] = (std::size_t)(std::ceil(std::pow(nElements, 1.0 / ND)));
    for (auto i = 1; i < ND; i += 1) {
      mShape[i] = (std::size_t)(std::ceil(
          std::pow((1.0 * nElements) / accum_helper(i), 1.0 / (ND - i))));
    }
    nNodes = accum_helper(ND);
    CkAssert(nNodes >= nElements &&
             "insufficent nbr of nodes to encompass all elements");
  }

  inline const coordinate_t& shape() const { return mShape; }

  inline const std::size_t& num_nodes() const { return nNodes; }

  inline const std::size_t& num_elements() const { return nElements; }

  inline std::size_t ordinal_for(const coordinate_t& coord) const {
    std::size_t ord = 0;
    for (auto i = 0; i < (ND - 1); i += 1) {
      ord += coord[i] * factor_helper(i);
    }
    ord += coord[ND - 1];
    return ord;
  }

  inline coordinate_t coordinate_for(std::size_t ord) const {
    coordinate_t coord;
    for (auto i = 0; i < (ND - 1); i++) {
      auto factor = factor_helper(i);
      coord[i] = ord / factor;
      if (i < (ND - 2)) {
        ord -= coord[i] * factor;
      }
    }
    coord[ND - 1] = ord % mShape[ND - 1];
    return coord;
  }

  inline bool is_hole(const std::size_t& ord) const { return ord >= nElements; }

  // src : https://charm.cs.illinois.edu/newPapers/02-10/paper.pdf
  inline std::size_t route_to_2d(const std::size_t& i, const std::size_t& j,
                                 const std::size_t& k) const {
    auto tmp = ordinal_for({i, k});
    if (is_hole(tmp)) {
      return ordinal_for({j % (mShape[0] - 1), k});
    } else {
      return tmp;
    }
  }

  std::size_t next(const std::size_t& from, const std::size_t& to) const {
    auto src = coordinate_for(from), dst = coordinate_for(to);
    if (src[1] == dst[1]) {
      // if we're in the same column, accumulate at the source
      return from;
    } else {
      // else, route to destination column
      return route_to_2d(src[0], src[1], dst[1]);
    }
  }
};
}
}

#endif
