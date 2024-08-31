# Use the latest LTS version of Ubuntu
FROM ubuntu:24.04

# Set the non-interactive frontend for apt-get
ENV DEBIAN_FRONTEND=noninteractive

# Update the package list and install essential packages
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    libfuse3-dev \
    cmake \
    gdb \
    clang \
    llvm \
    make \
    git \
    pkg-config \
    software-properties-common \
    # fuse \
    fuse3
    
# Clean up the package lists to reduce the image size
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /TransFuse

COPY . /TransFuse

# Default command to run when the container starts
CMD ["/bin/bash"]
