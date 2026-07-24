#pragma once

#include <pybind11/pybind11.h>

namespace chronos::native {

void bind_chronos_s3(pybind11::module_ &m);

} // namespace chronos::native
