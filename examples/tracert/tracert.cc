#include <hypercomm/routing.hpp>

void EmergencyExit(void) { return; }

void test_2d_mesh(aggregation::routing::mesh<2>& m) {
  const auto& shape = m.shape();
  CkPrintf("topology of mesh with shape (%zu, %zu):\n", shape[0], shape[1]);
  for (auto i = 0; i < m.num_nodes(); i += 1) {
    auto coord = m.coordinate_for(i);
    CkPrintf("\t(%zu, %zu)", coord[0], coord[1]);
    if (coord[1] == (shape[1] - 1)) {
      CkPrintf("\n");
    }
    auto j = m.ordinal_for(coord);
    if (i != j) {
      CkAbort("\nfailure, expected %d instead got %zu\n", i, j);
    }
  }

  CkPrintf("routing msg from 0 to 7 => %zu\n", m.next(0, 7));

  auto nxt = m.next(26, 4);
  auto cnxt = m.coordinate_for(nxt);
  CkPrintf("routing msg from 26 to 4 => %zu (%zu, %zu)\n", nxt, cnxt[0],
           cnxt[1]);
}

int main(void) {
  aggregation::routing::mesh<2> m(36);
  test_2d_mesh(m);

  CkPrintf("\n");

  aggregation::routing::mesh<2> n(27);
  test_2d_mesh(n);

  return 0;
}
