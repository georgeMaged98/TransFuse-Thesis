cmake_minimum_required(VERSION 3.10)
project(MyFuseProject)

# Set the C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Find the FUSE library
find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE REQUIRED fuse3)

# Include directories
include_directories(${FUSE_INCLUDE_DIRS})

# Add your source files
add_executable(transfuse_app transfuse.cpp
        passthrough.cpp
        passthrough_helpers.h
        hello.cpp
        utils.cc)

# Link the FUSE library
target_link_libraries(transfuse_app ${FUSE_LIBRARIES})
