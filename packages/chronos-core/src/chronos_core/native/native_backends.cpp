// Native backend aggregation for the _native_interval extension module.
//
// The existing native SQL runtime is deliberately private and is shared here as
// a utility layer: connection adapters, value conversion, SQL rewriting, and
// profiling. Interval and copy backend logic stays in separate source files.

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

#include "copy_branch_store.hpp"
#include "interval_data_plane.hpp"

namespace py = pybind11;

#include "interval_common.cpp"
#include "interval_sql_runtime.cpp"
#include "interval_branch_store.cpp"
#include "interval_branch_session.cpp"
#include "interval_python_bindings.cpp"
#include "copy_branch_store.cpp"
#include "copy_python_bindings.cpp"
