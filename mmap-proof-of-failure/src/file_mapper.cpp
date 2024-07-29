//
// Created by George Maged on 03.06.24.
//
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "file_mapper.h"
#include <iostream>

using namespace dbms;
using namespace std;

char *FileMapper::map_file(char *filePath, off_t offset) {
    int fd = open(filePath, O_RDWR | O_CREAT, (mode_t) 0664);
    struct stat fileInfo;

    // Before mapping a file to memory, we need to get a file descriptor for it
    // by using the open() system call
    if (fd == -1) {
        perror("open");
        exit(1);
    }

    if (stat(filePath, &fileInfo) == -1) {
        perror("stat");
        exit(1);
    }

    void *data = mmap(nullptr, this->pageSizeInBytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);

    if (data == MAP_FAILED) {
        close(fd);
        perror("mmap");
        exit(1);
    }

    close(fd);
    char *page = static_cast<char *>(data);
    return page;
}

void FileMapper::append_to_file(char *filePath, const char *data, size_t size) {
    int fd = open(filePath, O_RDWR | O_CREAT, (mode_t) 0664);
    struct stat fileInfo{};

    // Before mapping a file to memory, we need to get a file descriptor for it
    // by using the open() system call
    if (fd == -1) {
        perror("open");
        exit(1);
    }

    if (stat(filePath, &fileInfo) == -1) {
        perror("stat");
        exit(1);
    }

    // Stretch the file size to write the array of char
    size_t oldFileSize = fileInfo.st_size;
    size_t fileSizeNew = fileInfo.st_size + size;
    if (ftruncate(fd, fileSizeNew) == -1) {
        close(fd);
        perror("Error resizing the file");
        exit(1);
    }

    char *page;
    page = static_cast<char *>(mmap(nullptr, fileSizeNew, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));

    if (page == MAP_FAILED) {
        close(fd);
        perror("mmap");
        exit(1);
    }

    for (size_t i = 0; i < size; i++) {
        page[i + oldFileSize] = data[i];
    }

    // Write it now to disk
    if (msync(page, fileSizeNew, MS_SYNC) == -1) {
        perror("Could not sync the file to disk");
    }

    // Free the mmapped memory
    if (munmap(page, fileSizeNew) == -1) {
        close(fd);
        perror("Error un-mmapping the file");
        exit(1);
    }
    close(fd);
};


void FileMapper::write_to_file(char *filePath, const char *data, size_t size) {
    int fd = open(filePath, O_RDWR | O_CREAT, (mode_t) 0664);
    struct stat fileInfo{};

    // Before mapping a file to memory, we need to get a file descriptor for it
    // by using the open() system call
    if (fd == -1) {
        perror("open");
        exit(1);
    }

    if (stat(filePath, &fileInfo) == -1) {
        perror("stat");
        exit(1);
    }

    // Stretch the file size to write the array of char
    if (ftruncate(fd, size) == -1) {
        close(fd);
        perror("Error resizing the file");
        exit(1);
    }

    char *page;
    page = static_cast<char *>(mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));

    if (page == MAP_FAILED) {
        close(fd);
        perror("mmap");
        exit(1);
    }

    for (size_t i = 0; i < size; i++) {
        page[i] = data[i];
    }

    // Write it NOW to disk
    if (msync(page, size, MS_SYNC) == -1) {
        perror("Could not sync the file to disk");
    }

    // Free the mmapped memory
    if (munmap(page, size) == -1) {
        close(fd);
        perror("Error un-mmapping the file");
        exit(1);
    }
    close(fd);
}

uint64_t FileMapper::get_file_size(char *filePath) {
    int fd = open(filePath, O_RDONLY);
    struct stat fileInfo;

    // Before mapping a file to memory, we need to get a file descriptor for it
    // by using the open() system call
    if (fd == -1) {
        perror("open");
        exit(1);
    }

    if (stat(filePath, &fileInfo) == -1) {
        perror("stat");
        exit(1);
    }

    return fileInfo.st_size;
};

