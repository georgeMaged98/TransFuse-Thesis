# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/gtm/src/googletest"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/gtm/src/googletest-build"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/gtm"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/gtm/tmp"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/gtm/src/googletest-stamp"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/gtm/src"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/gtm/src/googletest-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/gtm/src/googletest-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/gtm/src/googletest-stamp${cfgdir}") # cfgdir has leading slash
endif()
