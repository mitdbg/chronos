cmake_minimum_required(VERSION 3.20)

project(chronos_core_native LANGUAGES CXX)

find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE3 REQUIRED IMPORTED_TARGET fuse3)

include(FetchContent)
find_package(nlohmann_json CONFIG QUIET)
if(NOT TARGET nlohmann_json::nlohmann_json)
  find_path(NLOHMANN_JSON_INCLUDE_DIR nlohmann/json.hpp QUIET)
  if(NLOHMANN_JSON_INCLUDE_DIR)
    add_library(nlohmann_json_header INTERFACE)
    add_library(nlohmann_json::nlohmann_json ALIAS nlohmann_json_header)
    target_include_directories(nlohmann_json_header INTERFACE "${NLOHMANN_JSON_INCLUDE_DIR}")
  else()
    FetchContent_Declare(
      nlohmann_json
      URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
    )
    FetchContent_MakeAvailable(nlohmann_json)
  endif()
endif()
FetchContent_Declare(
  libpg_query
  URL https://github.com/pganalyze/libpg_query/archive/refs/tags/17-6.2.2.tar.gz
)
FetchContent_Declare(
  duckdb_lib
  URL https://github.com/duckdb/duckdb/releases/download/v1.5.4/libduckdb-linux-amd64.zip
)
FetchContent_GetProperties(libpg_query)
if(NOT libpg_query_POPULATED)
  FetchContent_Populate(libpg_query)
endif()
FetchContent_GetProperties(duckdb_lib)
if(NOT duckdb_lib_POPULATED)
  FetchContent_Populate(duckdb_lib)
endif()

set(LIBPG_QUERY_A "${libpg_query_SOURCE_DIR}/libpg_query.a")
add_custom_command(
  OUTPUT "${LIBPG_QUERY_A}"
  COMMAND make build
  WORKING_DIRECTORY "${libpg_query_SOURCE_DIR}"
  COMMENT "Building libpg_query"
  VERBATIM
)
add_custom_target(libpg_query_build DEPENDS "${LIBPG_QUERY_A}")
add_library(libpg_query_static STATIC IMPORTED GLOBAL)
set_target_properties(libpg_query_static PROPERTIES
  IMPORTED_LOCATION "${LIBPG_QUERY_A}"
  INTERFACE_INCLUDE_DIRECTORIES "${libpg_query_SOURCE_DIR};${libpg_query_SOURCE_DIR}/vendor"
)
add_dependencies(libpg_query_static libpg_query_build)

add_library(duckdb_shared SHARED IMPORTED GLOBAL)
set_target_properties(duckdb_shared PROPERTIES
  IMPORTED_LOCATION "${duckdb_lib_SOURCE_DIR}/libduckdb.so"
  INTERFACE_INCLUDE_DIRECTORIES "${duckdb_lib_SOURCE_DIR}"
)

pybind11_add_module(_native_interval
  src/chronos_core/native/interval_module.cpp
  src/chronos_core/native/native_backends.cpp
  src/chronos_core/native/chronosfs_fuse.cpp
)
target_compile_features(_native_interval PRIVATE cxx_std_17)
target_link_libraries(_native_interval PRIVATE SQLite::SQLite3 PostgreSQL::PostgreSQL PkgConfig::FUSE3 libpg_query_static duckdb_shared nlohmann_json::nlohmann_json)
set_target_properties(_native_interval PROPERTIES
  BUILD_RPATH "${duckdb_lib_SOURCE_DIR}"
  INSTALL_RPATH "$ORIGIN"
)

install(TARGETS _native_interval LIBRARY DESTINATION chronos_core)
install(FILES "${duckdb_lib_SOURCE_DIR}/libduckdb.so" DESTINATION chronos_core)
