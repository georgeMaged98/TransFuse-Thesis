cmake_minimum_required(VERSION 3.23)
project(proof_of_failure)

set(CMAKE_CXX_STANDARD 23)

#include(FetchContent)
#FetchContent_Declare(
#        googletest
#        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
#)
#FetchContent_MakeAvailable(googletest)
#
#enable_testing()

include_directories(include)

add_executable(proof_of_failure
        main.cpp
        src/file_mapper.cpp
        src/mmap_manager.cpp
        )

# Include directories for test executable
#add_executable(
#        mmap_test
#        test/mmap_test.cpp
#)

#target_link_libraries(
#        mmap_test
#        GTest::gtest_main
#)

#include(GoogleTest)
#gtest_discover_tests(mmap_test)