cmake_minimum_required(VERSION 3.20)

project(chronos_core_native LANGUAGES CXX)

find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(PostgreSQL REQUIRED)

pybind11_add_module(_native_interval
  src/chronos_core/native/interval_module.cpp
  src/chronos_core/native/interval_data_plane.cpp
)
target_compile_features(_native_interval PRIVATE cxx_std_17)
target_link_libraries(_native_interval PRIVATE SQLite::SQLite3 PostgreSQL::PostgreSQL)

install(TARGETS _native_interval LIBRARY DESTINATION chronos_core)
