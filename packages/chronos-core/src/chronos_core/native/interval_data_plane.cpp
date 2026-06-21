#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <boost/multiprecision/cpp_int.hpp>
#include <duckdb.h>
#include <libpq-fe.h>
#include <pg_query.h>
#include <protobuf/pg_query.pb-c.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <Python.h>
#include <sqlite3.h>

#include "interval_data_plane.hpp"

namespace py = pybind11;


// The native interval backend is built as one translation unit because the
// branch store, SQL drivers, parser helpers, and Python bindings share a
// deliberately private implementation namespace.  The code is split into
// layer-specific source fragments here so each subsystem can evolve without
// growing a monolithic file while preserving the existing internal linkage.
#include "interval_common.cpp"
#include "interval_sql_runtime.cpp"
#include "interval_branch_store.cpp"
#include "interval_branch_session.cpp"
#include "interval_python_bindings.cpp"
