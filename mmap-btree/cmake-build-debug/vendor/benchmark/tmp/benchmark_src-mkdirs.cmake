# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-build"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/tmp"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp${cfgdir}") # cfgdir has leading slash
endif()
