#!/bin/bash

# Define an array of MemoryMax values
memory_limits=("8G" "6.4G" "4G")
memory_percentages=("1" "0.8" "0.5")

executables=("cmake-build-release/test8GB")

# Loop through each combination of MemoryMax and executable
for i in "${!memory_limits[@]}"; do
    memory_limit="${memory_limits[i]}"
    memory_percentage="${memory_percentages[i]}"

    for executable in "${executables[@]}"; do
        echo "Running with MemoryMax=${memory_limit}, Executable=${executable}, MemoryPercent=${memory_percentage}"
        sudo systemd-run --scope -p MemoryMax="${memory_limit}" ./"${executable}" "${memory_percentage}"
        sleep 1
    done
done
