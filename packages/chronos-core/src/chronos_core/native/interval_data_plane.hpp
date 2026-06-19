#pragma once

#include <pybind11/pybind11.h>

namespace chronos::native {

void bind_interval_data_plane(pybind11::module_ &m);

} // namespace chronos::native
