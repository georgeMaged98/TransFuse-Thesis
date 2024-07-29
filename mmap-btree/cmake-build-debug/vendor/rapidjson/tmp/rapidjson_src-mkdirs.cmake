# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/rapidjson/src/rapidjson_src"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/rapidjson/src/rapidjson_src-build"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/rapidjson"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/rapidjson/tmp"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/rapidjson/src/rapidjson_src-stamp"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/rapidjson/src"
  "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/rapidjson/src/rapidjson_src-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/rapidjson/src/rapidjson_src-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/rapidjson/src/rapidjson_src-stamp${cfgdir}") # cfgdir has leading slash
endif()
