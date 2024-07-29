# ---------------------------------------------------------------------------
# Transfuse
# ---------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.28)
project(mmap_btree)

add_executable(mmap_btree main.cpp)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(-Wall -Wextra -Wvla)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -w")

if (APPLE)
    list(APPEND CMAKE_PREFIX_PATH /usr/local/opt/bison)
    list(APPEND CMAKE_PREFIX_PATH /usr/local/opt/flex)
endif (APPLE)

if (CYGWIN)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++")
endif (CYGWIN)



# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

# This command finds and loads settings for the Threads package, which is necessary for multi-threaded applications.
find_package(Threads REQUIRED)
# This setting prefers using the POSIX Threads (pthread) library for threading.
# By setting this flag to ON, CMake will prioritize pthreads over other threading libraries available on the system.
set(THREADS_PREFER_PTHREAD_FLAG ON)

# This includes the clang-tidy.cmake script located in the cmake directory at the root of your source tree.  This script is likely used to integrate clang-tidy, a static analysis tool for C++ code, into your build process. It allows you to automatically run clang-tidy checks as part of the compilation.
include("${CMAKE_SOURCE_DIR}/cmake/clang-tidy.cmake")
# This includes the benchmark.cmake script located in the vendor directory at the root of your source tree. This script likely configures and integrates the Google Benchmark library into your project, which is useful for microbenchmarking code to measure performance.
include("${CMAKE_SOURCE_DIR}/vendor/benchmark.cmake")
# This includes the gflags.cmake script from the vendor directory. This script is likely responsible for configuring and integrating the gflags library, which is a library for command-line flag parsing.
include("${CMAKE_SOURCE_DIR}/vendor/gflags.cmake")
# This includes the googletest.cmake script from the vendor directory. This script configures and integrates the Google Test framework, which is used for writing and running C++ unit tests.
include("${CMAKE_SOURCE_DIR}/vendor/googletest.cmake")
# This includes the rapidjson.cmake script from the vendor directory. This script configures and integrates the RapidJSON library, which is a fast JSON parser and generator for C++.
include("${CMAKE_SOURCE_DIR}/vendor/rapidjson.cmake")


# ---------------------------------------------------------------------------
# Includes
# ---------------------------------------------------------------------------
# This command includes the local.cmake script located in the include directory at the root of your source tree. This script may contain additional CMake commands and settings specific to the include directory.
include("${CMAKE_SOURCE_DIR}/include/local.cmake")
# This command adds the include directory at the root of your source tree to the list of directories that the compiler will search for header files.
include_directories(
        ${CMAKE_SOURCE_DIR}/include
)

include_directories(SYSTEM
        ${LLVM_INCLUDE_DIRS}
        ${GTEST_INCLUDE_DIR}
        ${GFLAGS_INCLUDE_DIR}
        ${BENCHMARK_INCLUDE_DIR}
)

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/src/local.cmake")

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/test/local.cmake")

# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/bench/local.cmake")

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------

add_custom_target(lint)
add_dependencies(lint rapidjson)
add_dependencies(lint ${lint_targets})

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

message(STATUS "[TRANSFUSE] settings")
message(STATUS "    LLVM_INCLUDE_DIRS           = ${LLVM_INCLUDE_DIRS}")
message(STATUS "    LLVM_INSTALL_PREFIX         = ${LLVM_INSTALL_PREFIX}")
message(STATUS "    GFLAGS_INCLUDE_DIR          = ${GFLAGS_INCLUDE_DIR}")
message(STATUS "    GFLAGS_LIBRARY_PATH         = ${GFLAGS_LIBRARY_PATH}")
message(STATUS "[TEST] settings")
message(STATUS "    GTEST_INCLUDE_DIR           = ${GTEST_INCLUDE_DIR}")
message(STATUS "    GTEST_LIBRARY_PATH          = ${GTEST_LIBRARY_PATH}")
