#pragma once

#include <pybind11/pybind11.h>

namespace chronos::native {

void bind_chronosfs_fuse(pybind11::module_ &m);

} // namespace chronos::native
