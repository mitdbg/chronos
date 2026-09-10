cmake_minimum_required(VERSION 3.20)

project(chronos_core_native LANGUAGES CXX)

option(CHRONOS_WITH_FILESYSTEM "Build ChronosFS and Linux FUSE support" OFF)
option(CHRONOS_WITH_DUCKDB "Build the DuckDB data adapter" OFF)
option(CHRONOS_WITH_S3 "Build the S3 gateway" OFF)

find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(Boost REQUIRED)
if(CHRONOS_WITH_FILESYSTEM)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "ChronosFS currently requires Linux")
  endif()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(FUSE3 REQUIRED IMPORTED_TARGET fuse3)
endif()
if(CHRONOS_WITH_S3)
  find_package(Boost REQUIRED COMPONENTS system)
  find_package(CURL REQUIRED)
  find_package(OpenSSL REQUIRED)
  find_package(pugixml REQUIRED)
endif()

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
  URL_HASH SHA256=e68962c18dbf5890821511be6c5c42261170bf8bfd51a82ea9176069f3d0df8b
)
if(CHRONOS_WITH_DUCKDB)
  set(CHRONOS_DUCKDB_ROOT "" CACHE PATH "Directory containing duckdb.h and libduckdb")
  if(CHRONOS_DUCKDB_ROOT)
    find_path(DUCKDB_INCLUDE_DIR duckdb.h PATHS "${CHRONOS_DUCKDB_ROOT}" REQUIRED NO_DEFAULT_PATH)
    find_library(DUCKDB_LIBRARY duckdb PATHS "${CHRONOS_DUCKDB_ROOT}" REQUIRED NO_DEFAULT_PATH)
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
    FetchContent_Declare(duckdb_lib
      URL https://github.com/duckdb/duckdb/releases/download/v1.5.4/libduckdb-linux-amd64.zip
      URL_HASH SHA256=838d98a85e697bab9935010c88a8c67d3312ccedcab4cb4a0ba01da65113bb70)
    FetchContent_GetProperties(duckdb_lib)
    if(NOT duckdb_lib_POPULATED)
      FetchContent_Populate(duckdb_lib)
    endif()
    set(DUCKDB_INCLUDE_DIR "${duckdb_lib_SOURCE_DIR}")
    set(DUCKDB_LIBRARY "${duckdb_lib_SOURCE_DIR}/libduckdb.so")
  else()
    message(FATAL_ERROR "Supply CHRONOS_DUCKDB_ROOT on this platform, or disable CHRONOS_WITH_DUCKDB")
  endif()
endif()
if(CHRONOS_WITH_FILESYSTEM)
  FetchContent_Declare(unordered_dense
    URL https://github.com/martinus/unordered_dense/archive/refs/tags/v4.8.1.tar.gz
    URL_HASH SHA256=9f7202ec6d8353932ef865d33f5872e4b7a1356e9032da7cd09c3a0c5bb2b7de)
  FetchContent_MakeAvailable(unordered_dense)
endif()
FetchContent_GetProperties(libpg_query)
if(NOT libpg_query_POPULATED)
  FetchContent_Populate(libpg_query)
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

pybind11_add_module(_native_interval
  src/chronos_core/native/interval_module.cpp
  src/chronos_core/native/native_backends.cpp
)
target_compile_features(_native_interval PRIVATE cxx_std_17)
target_include_directories(_native_interval PRIVATE ${Boost_INCLUDE_DIRS})
target_link_libraries(_native_interval PRIVATE
  SQLite::SQLite3
  PostgreSQL::PostgreSQL
  libpg_query_static
  nlohmann_json::nlohmann_json
)
if(CHRONOS_WITH_FILESYSTEM)
  target_sources(_native_interval PRIVATE src/chronos_core/native/chronosfs_fuse.cpp)
  target_compile_definitions(_native_interval PRIVATE CHRONOS_WITH_FILESYSTEM)
  target_link_libraries(_native_interval PRIVATE PkgConfig::FUSE3 unordered_dense::unordered_dense)
endif()
if(CHRONOS_WITH_S3)
  target_sources(_native_interval PRIVATE src/chronos_core/native/chronos_s3.cpp)
  target_compile_definitions(_native_interval PRIVATE CHRONOS_WITH_S3)
  target_link_libraries(_native_interval PRIVATE Boost::system CURL::libcurl OpenSSL::Crypto pugixml::pugixml)
endif()
if(CHRONOS_WITH_DUCKDB)
  target_compile_definitions(_native_interval PRIVATE CHRONOS_WITH_DUCKDB)
  target_include_directories(_native_interval PRIVATE "${DUCKDB_INCLUDE_DIR}")
  target_link_libraries(_native_interval PRIVATE "${DUCKDB_LIBRARY}")
  install(FILES "${DUCKDB_LIBRARY}" DESTINATION chronos_core)
endif()
set_target_properties(_native_interval PROPERTIES
  INSTALL_RPATH "$ORIGIN"
)

install(TARGETS _native_interval LIBRARY DESTINATION chronos_core)
