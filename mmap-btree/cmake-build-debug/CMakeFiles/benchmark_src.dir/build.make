# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.28

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /snap/clion/284/bin/cmake/linux/x64/bin/cmake

# The command to remove a file.
RM = /snap/clion/284/bin/cmake/linux/x64/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug

# Utility rule file for benchmark_src.

# Include any custom commands dependencies for this target.
include CMakeFiles/benchmark_src.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/benchmark_src.dir/progress.make

CMakeFiles/benchmark_src: CMakeFiles/benchmark_src-complete

CMakeFiles/benchmark_src-complete: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-install
CMakeFiles/benchmark_src-complete: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-mkdir
CMakeFiles/benchmark_src-complete: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-download
CMakeFiles/benchmark_src-complete: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-update
CMakeFiles/benchmark_src-complete: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-patch
CMakeFiles/benchmark_src-complete: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-configure
CMakeFiles/benchmark_src-complete: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-build
CMakeFiles/benchmark_src-complete: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-install
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --blue --bold --progress-dir=/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Completed 'benchmark_src'"
	/snap/clion/284/bin/cmake/linux/x64/bin/cmake -E make_directory /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/CMakeFiles
	/snap/clion/284/bin/cmake/linux/x64/bin/cmake -E touch /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/CMakeFiles/benchmark_src-complete
	/snap/clion/284/bin/cmake/linux/x64/bin/cmake -E touch /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp/benchmark_src-done

vendor/benchmark/src/benchmark_src-stamp/benchmark_src-build: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-configure
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --blue --bold --progress-dir=/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Performing build step for 'benchmark_src'"
	cd /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-build && $(MAKE)
	cd /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-build && /snap/clion/284/bin/cmake/linux/x64/bin/cmake -E touch /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp/benchmark_src-build

vendor/benchmark/src/benchmark_src-stamp/benchmark_src-configure: vendor/benchmark/tmp/benchmark_src-cfgcmd.txt
vendor/benchmark/src/benchmark_src-stamp/benchmark_src-configure: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-patch
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --blue --bold --progress-dir=/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Performing configure step for 'benchmark_src'"
	cd /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-build && /snap/clion/284/bin/cmake/linux/x64/bin/cmake -DCMAKE_INSTALL_PREFIX=/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark -DCMAKE_C_COMPILER=/usr/bin/cc -DCMAKE_CXX_COMPILER=/usr/bin/c++ -DCMAKE_CXX_FLAGS= -DCMAKE_BUILD_TYPE:STRING=Debug "-GCodeBlocks - Unix Makefiles" -S /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src -B /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-build
	cd /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-build && /snap/clion/284/bin/cmake/linux/x64/bin/cmake -E touch /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp/benchmark_src-configure

vendor/benchmark/src/benchmark_src-stamp/benchmark_src-download: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-gitinfo.txt
vendor/benchmark/src/benchmark_src-stamp/benchmark_src-download: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-mkdir
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --blue --bold --progress-dir=/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Performing download step (git clone) for 'benchmark_src'"
	cd /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src && /snap/clion/284/bin/cmake/linux/x64/bin/cmake -P /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/tmp/benchmark_src-gitclone.cmake
	cd /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src && /snap/clion/284/bin/cmake/linux/x64/bin/cmake -E touch /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp/benchmark_src-download

vendor/benchmark/src/benchmark_src-stamp/benchmark_src-install: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-build
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --blue --bold --progress-dir=/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_5) "Performing install step for 'benchmark_src'"
	cd /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-build && $(MAKE) install
	cd /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-build && /snap/clion/284/bin/cmake/linux/x64/bin/cmake -E touch /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp/benchmark_src-install

vendor/benchmark/src/benchmark_src-stamp/benchmark_src-mkdir:
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --blue --bold --progress-dir=/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_6) "Creating directories for 'benchmark_src'"
	/snap/clion/284/bin/cmake/linux/x64/bin/cmake -Dcfgdir= -P /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/tmp/benchmark_src-mkdirs.cmake
	/snap/clion/284/bin/cmake/linux/x64/bin/cmake -E touch /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp/benchmark_src-mkdir

vendor/benchmark/src/benchmark_src-stamp/benchmark_src-patch: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-patch-info.txt
vendor/benchmark/src/benchmark_src-stamp/benchmark_src-patch: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-update
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --blue --bold --progress-dir=/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_7) "No patch step for 'benchmark_src'"
	/snap/clion/284/bin/cmake/linux/x64/bin/cmake -E echo_append
	/snap/clion/284/bin/cmake/linux/x64/bin/cmake -E touch /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp/benchmark_src-patch

vendor/benchmark/src/benchmark_src-stamp/benchmark_src-update: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-update-info.txt
vendor/benchmark/src/benchmark_src-stamp/benchmark_src-update: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-download
	@$(CMAKE_COMMAND) -E cmake_echo_color "--switch=$(COLOR)" --blue --bold --progress-dir=/media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_8) "No update step for 'benchmark_src'"
	cd /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src && /snap/clion/284/bin/cmake/linux/x64/bin/cmake -E echo_append
	cd /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src && /snap/clion/284/bin/cmake/linux/x64/bin/cmake -E touch /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/vendor/benchmark/src/benchmark_src-stamp/benchmark_src-update

benchmark_src: CMakeFiles/benchmark_src
benchmark_src: CMakeFiles/benchmark_src-complete
benchmark_src: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-build
benchmark_src: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-configure
benchmark_src: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-download
benchmark_src: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-install
benchmark_src: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-mkdir
benchmark_src: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-patch
benchmark_src: vendor/benchmark/src/benchmark_src-stamp/benchmark_src-update
benchmark_src: CMakeFiles/benchmark_src.dir/build.make
.PHONY : benchmark_src

# Rule to build all files generated by this target.
CMakeFiles/benchmark_src.dir/build: benchmark_src
.PHONY : CMakeFiles/benchmark_src.dir/build

CMakeFiles/benchmark_src.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/benchmark_src.dir/cmake_clean.cmake
.PHONY : CMakeFiles/benchmark_src.dir/clean

CMakeFiles/benchmark_src.dir/depend:
	cd /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug /media/george-elfayoumi/Academic/TUM/TransFuse-Thesis/mmap-btree/cmake-build-debug/CMakeFiles/benchmark_src.dir/DependInfo.cmake "--color=$(COLOR)"
.PHONY : CMakeFiles/benchmark_src.dir/depend

