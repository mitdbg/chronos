#include <pybind11/pybind11.h>

#include "chronosfs_fuse.hpp"
#include "copy_branch_store.hpp"
#include "interval_data_plane.hpp"

namespace py = pybind11;

PYBIND11_MODULE(_native_interval, m) {
    m.doc() = "Native Chronos branching primitives";
    chronos::native::bind_interval_data_plane(m);
    chronos::native::bind_copy_branch_store(m);
    chronos::native::bind_chronosfs_fuse(m);
}
