#include <pybind11/pybind11.h>

#include "chronosfs_fuse.hpp"
#include "chronos_s3.hpp"
#include "copy_branch_store.hpp"
#include "interval_data_plane.hpp"

namespace py = pybind11;

PYBIND11_MODULE(_native_interval, m) {
    m.doc() = "Native Chronos branching primitives";
    chronos::native::bind_interval_data_plane(m);
    chronos::native::bind_copy_branch_store(m);
#ifdef CHRONOS_WITH_FILESYSTEM
    chronos::native::bind_chronosfs_fuse(m);
#endif
#ifdef CHRONOS_WITH_S3
    chronos::native::bind_chronos_s3(m);
#endif
    m.attr("has_filesystem") =
#ifdef CHRONOS_WITH_FILESYSTEM
        true;
#else
        false;
#endif
    m.attr("has_duckdb") =
#ifdef CHRONOS_WITH_DUCKDB
        true;
#else
        false;
#endif
    m.attr("has_s3") =
#ifdef CHRONOS_WITH_S3
        true;
#else
        false;
#endif
}
