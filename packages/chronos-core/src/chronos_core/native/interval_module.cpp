#include <pybind11/pybind11.h>

#include "interval_data_plane.hpp"

namespace py = pybind11;

PYBIND11_MODULE(_native_interval, m) {
    m.doc() = "Native Chronos interval data-plane primitives";
    chronos::native::bind_interval_data_plane(m);
}
