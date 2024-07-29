//
// Created by George Maged on 02.06.24.
//

#ifndef PROOF_OF_FAILURE_FILE_MAPPER_H
#define PROOF_OF_FAILURE_FILE_MAPPER_H

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cerrno>
#include <vector>

namespace dbms {

    class FileMapper {
        size_t pageSizeInBytes;
    public:
        explicit FileMapper(size_t pageSizeInBytes) : pageSizeInBytes(pageSizeInBytes) {};

        char *map_file(char *filePath, off_t offset);

        uint64_t  get_file_size(char *filePath);

        static void append_to_file(char *filePath, const char *data, size_t size);

        void write_to_file(char *filePath, const char *data, size_t size);
    };

}
#endif //PROOF_OF_FAILURE_FILE_MAPPER_H
