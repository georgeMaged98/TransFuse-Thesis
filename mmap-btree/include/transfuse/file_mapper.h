//
// Created by george-elfayoumi on 7/29/24.
//

#ifndef FILEMAPPER_H
#define FILEMAPPER_H

#include <cstdlib>
#include <fstream>
#include <shared_mutex>
#include <sys/mman.h>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>
#include <sys/stat.h>

using namespace std;

namespace transfuse {
    class Page {
    public:
        // Delete the copy constructor and assignment operator
        Page(const Page &) = delete;

        Page &operator=(const Page &) = delete;

        explicit Page(char *mapped_data) {
            id = reinterpret_cast<size_t *>(mapped_data);
            count = reinterpret_cast<size_t *>(mapped_data + sizeof(size_t));
            exclusive = reinterpret_cast<bool *>(mapped_data + 2 * sizeof(size_t));
            data = mapped_data + 2 * sizeof(size_t) + sizeof(bool);
        }

        // Mutators
        void set_id(const size_t new_id) const { *id = new_id; }
        void set_count(const size_t new_count) const { *count = new_count; }
        void set_exclusive(bool is_exclusive) const { *exclusive = is_exclusive; }

        /// Returns a pointer to this page's data.
        [[nodiscard]] size_t get_id() const { return *id; }
        [[nodiscard]] size_t get_count() const { return *count; }
        [[nodiscard]] bool is_exclusive() const { return *exclusive; }
        [[nodiscard]] char *get_data() const { return data; }

        /// This std::shared_mutex makes the class non-movable and non-copyable.
        std::shared_mutex latch;

    private:
        friend class Segment;

        size_t *id = nullptr;
        size_t *count = nullptr;
        bool *exclusive = nullptr;
        char *data = nullptr;
    };

    class FileMapper {
        static constexpr uint64_t maxMapSize = 10LL << 30; // 10GB
        static constexpr uint64_t maxMmapStep = 1LL << 30; // 1GB
        static constexpr uint64_t minMmapFileSize = 1LL << 30; // 32KB

    public:
        FileMapper(std::string filename, size_t page_size);

        ~FileMapper();

        [[nodiscard]] Page *get_page(size_t page_number, bool is_exclusive);

        [[nodiscard]] size_t page_size() const { return page_size_; }

        void write_to_file(const void *data, size_t size) const;

    private:
        std::string filename_;
        size_t page_size_;
        uint64_t file_size_;
        /// Max number of pages that the current file can hold.
        /// (e.g.) a file with size of 32KB can hold (32KB / page_size) entries.
        size_t num_pages_;
        void *mmap_ptr_;

        [[nodiscard]] uint64_t calculate_file_size(uint64_t oldFileSize) const;

        void map_file(const uint64_t minSize) {
            const int fd = open(filename_.c_str(), O_RDWR | O_CREAT,
                                static_cast<mode_t>(0664));

            if (fd == -1) {
                perror("Failed to open file");
                return;
            }

            struct stat fileInfo{};
            if (stat(filename_.c_str(), &fileInfo) == -1) {
                perror("stat");
                exit(1);
            }

            uint64_t size = minSize;
            if (fileInfo.st_size > minSize) {
                size = fileInfo.st_size;
            }

            file_size_ = calculate_file_size(size);


            if (file_size_ < minMmapFileSize) {
                file_size_ = minMmapFileSize;
            }

            if (ftruncate(fd, file_size_) == -1) {
                perror("Failed to set file size");
                close(fd);
                return;
            }

            num_pages_ = file_size_ / page_size_;

            mmap_ptr_ = mmap(nullptr, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            if (mmap_ptr_ == MAP_FAILED) {
                close(fd);
                perror("Failed to mmap file");
                return;
            }

            // Use madvise with the MADV_NOHUGEPAGE flag
            if (madvise(mmap_ptr_, file_size_, MADV_NOHUGEPAGE) != 0) {
                perror("madvise failed");
                munmap(mmap_ptr_, file_size_);
                close(fd);
                return;
            }


            close(fd);
        }
    };
} // transfuse

#endif //FILEMAPPER_H
