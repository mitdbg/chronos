cmake_minimum_required(VERSION 3.20)

project(chronos_gateway LANGUAGES CXX)

option(CHRONOS_GATEWAY_BUILD_TESTS "Build Chronos gateway unit tests" ON)
option(CHRONOS_GATEWAY_WITH_CHRONOS "Link the native Chronos engines" ON)
set(
  CHRONOS_GATEWAY_E2B_PYTHON ""
  CACHE FILEPATH "Python interpreter containing the E2B SDK for the live test")

if(CHRONOS_GATEWAY_WITH_CHRONOS)
  find_package(Python COMPONENTS Interpreter Development.Embed REQUIRED)
  if(NOT CHRONOS_GATEWAY_E2B_PYTHON)
    set(CHRONOS_GATEWAY_E2B_PYTHON "${Python_EXECUTABLE}")
  endif()
  set(CHRONOS_WITH_FILESYSTEM ON CACHE BOOL "" FORCE)
  set(CHRONOS_BUILD_PYTHON_MODULE OFF CACHE BOOL "" FORCE)
  add_subdirectory(
    "${CMAKE_CURRENT_SOURCE_DIR}/../chronos-core"
    "${CMAKE_CURRENT_BINARY_DIR}/chronos-core")
endif()

find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE3 REQUIRED IMPORTED_TARGET fuse3)

add_library(chronos_gateway_runtime STATIC
  src/access_grant_registry.cpp
)
target_compile_features(chronos_gateway_runtime PUBLIC cxx_std_17)
target_compile_options(chronos_gateway_runtime PRIVATE -Wall -Wextra -Wpedantic)
target_include_directories(chronos_gateway_runtime PUBLIC include)
target_link_libraries(chronos_gateway_runtime PUBLIC OpenSSL::Crypto Threads::Threads)
if(CHRONOS_GATEWAY_WITH_CHRONOS)
  target_sources(
    chronos_gateway_runtime
    PRIVATE
      src/runtime.cpp
      src/config.cpp
      src/control_server.cpp
      src/postgres_server.cpp
      src/filesystem_view.cpp
      src/fuse_server.cpp
      src/nfs_server.cpp)
  target_link_libraries(
    chronos_gateway_runtime
    PUBLIC chronos_native_backend Python::Python Boost::system nlohmann_json::nlohmann_json
           PkgConfig::FUSE3 ${CMAKE_DL_LIBS})
endif()

add_executable(chronos-gateway src/main.cpp)
target_compile_features(chronos-gateway PRIVATE cxx_std_17)
target_compile_options(chronos-gateway PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(chronos-gateway PRIVATE chronos_gateway_runtime)
if(CHRONOS_GATEWAY_WITH_CHRONOS)
  target_link_libraries(chronos-gateway PRIVATE chronos_native_backend Python::Python)
endif()

if(CHRONOS_GATEWAY_BUILD_TESTS)
  enable_testing()
  add_executable(chronos_gateway_access_grant_tests tests/access_grant_registry_test.cpp)
  target_compile_features(chronos_gateway_access_grant_tests PRIVATE cxx_std_17)
  target_link_libraries(chronos_gateway_access_grant_tests PRIVATE chronos_gateway_runtime)
  add_test(NAME chronos_gateway_access_grant_tests COMMAND chronos_gateway_access_grant_tests)
  if(CHRONOS_GATEWAY_WITH_CHRONOS)
    add_executable(
      chronos_gateway_filesystem_tests
      tests/filesystem_engine_test.cpp)
    target_compile_features(chronos_gateway_filesystem_tests PRIVATE cxx_std_17)
    target_link_libraries(
      chronos_gateway_filesystem_tests
      PRIVATE chronos_native_backend Python::Python)
    add_test(
      NAME chronos_gateway_filesystem_tests
      COMMAND chronos_gateway_filesystem_tests)
    add_executable(
      chronos_gateway_runtime_tests
      tests/runtime_test.cpp)
    target_compile_features(chronos_gateway_runtime_tests PRIVATE cxx_std_17)
    target_link_libraries(
      chronos_gateway_runtime_tests
      PRIVATE chronos_gateway_runtime)
    add_test(
      NAME chronos_gateway_runtime_tests
      COMMAND chronos_gateway_runtime_tests)
    add_executable(
      chronos_gateway_postgres_tests
      tests/postgres_server_test.cpp)
    target_compile_features(chronos_gateway_postgres_tests PRIVATE cxx_std_17)
    target_link_libraries(
      chronos_gateway_postgres_tests
      PRIVATE chronos_gateway_runtime PostgreSQL::PostgreSQL Boost::system)
    add_test(
      NAME chronos_gateway_postgres_tests
      COMMAND chronos_gateway_postgres_tests)
    add_executable(
      chronos_gateway_concurrency_tests
      tests/concurrency_test.cpp)
    target_compile_features(chronos_gateway_concurrency_tests PRIVATE cxx_std_17)
    target_link_libraries(
      chronos_gateway_concurrency_tests
      PRIVATE chronos_gateway_runtime PostgreSQL::PostgreSQL Boost::system)
    add_test(
      NAME chronos_gateway_concurrency_tests
      COMMAND chronos_gateway_concurrency_tests)
    set_tests_properties(
      chronos_gateway_concurrency_tests PROPERTIES TIMEOUT 60)
    add_executable(
      chronos_gateway_control_tests
      tests/control_server_test.cpp)
    target_compile_features(chronos_gateway_control_tests PRIVATE cxx_std_17)
    target_link_libraries(
      chronos_gateway_control_tests
      PRIVATE chronos_gateway_runtime Boost::system nlohmann_json::nlohmann_json)
    add_test(
      NAME chronos_gateway_control_tests
      COMMAND chronos_gateway_control_tests)
    add_executable(
      chronos_gateway_filesystem_view_tests
      tests/filesystem_view_test.cpp)
    target_compile_features(chronos_gateway_filesystem_view_tests PRIVATE cxx_std_17)
    target_link_libraries(
      chronos_gateway_filesystem_view_tests
      PRIVATE chronos_gateway_runtime)
    add_test(
      NAME chronos_gateway_filesystem_view_tests
      COMMAND chronos_gateway_filesystem_view_tests)
    add_executable(
      chronos_gateway_fuse_tests
      tests/fuse_server_test.cpp)
    target_compile_features(chronos_gateway_fuse_tests PRIVATE cxx_std_17)
    target_link_libraries(
      chronos_gateway_fuse_tests
      PRIVATE chronos_gateway_runtime PkgConfig::FUSE3)
    add_test(
      NAME chronos_gateway_fuse_tests
      COMMAND chronos_gateway_fuse_tests)
    set_tests_properties(
      chronos_gateway_fuse_tests PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 30)
    add_executable(
      chronos_gateway_nfs_config_tests
      tests/nfs_config_test.cpp)
    target_compile_features(chronos_gateway_nfs_config_tests PRIVATE cxx_std_17)
    target_link_libraries(
      chronos_gateway_nfs_config_tests
      PRIVATE chronos_gateway_runtime)
    add_test(
      NAME chronos_gateway_nfs_config_tests
      COMMAND chronos_gateway_nfs_config_tests)
    add_executable(
      chronos_gateway_config_tests
      tests/config_test.cpp)
    target_compile_features(chronos_gateway_config_tests PRIVATE cxx_std_17)
    target_link_libraries(
      chronos_gateway_config_tests
      PRIVATE chronos_gateway_runtime)
    add_test(
      NAME chronos_gateway_config_tests
      COMMAND chronos_gateway_config_tests)
    add_executable(
      chronos_gateway_nfs_integration_tests
      tests/nfs_server_integration_test.cpp)
    target_compile_features(chronos_gateway_nfs_integration_tests PRIVATE cxx_std_17)
    target_link_libraries(
      chronos_gateway_nfs_integration_tests
      PRIVATE chronos_gateway_runtime Boost::system)
    add_test(
      NAME chronos_gateway_nfs_integration_tests
      COMMAND chronos_gateway_nfs_integration_tests)
    set_tests_properties(
      chronos_gateway_nfs_integration_tests
      PROPERTIES SKIP_RETURN_CODE 77 TIMEOUT 90)
    add_executable(
      chronos_gateway_e2b_fixture
      tests/e2b_fixture.cpp)
    target_compile_features(chronos_gateway_e2b_fixture PRIVATE cxx_std_17)
    target_link_libraries(
      chronos_gateway_e2b_fixture
      PRIVATE chronos_gateway_runtime)
    add_test(
      NAME chronos_e2b_adapter_tests
      COMMAND
        ${CHRONOS_GATEWAY_E2B_PYTHON}
        ${CMAKE_CURRENT_SOURCE_DIR}/../chronos-e2b/tests/test_adapter.py)
    add_test(
      NAME chronos_gateway_e2b_integration_tests
      COMMAND
        ${CHRONOS_GATEWAY_E2B_PYTHON}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/e2b_integration_test.py
        --gateway $<TARGET_FILE:chronos-gateway>
        --fixture $<TARGET_FILE:chronos_gateway_e2b_fixture>)
    set_tests_properties(
      chronos_gateway_e2b_integration_tests
      PROPERTIES
        LABELS "external;e2b"
        SKIP_RETURN_CODE 77
        TIMEOUT 1200)
  endif()
endif()

install(TARGETS chronos-gateway RUNTIME DESTINATION bin)
