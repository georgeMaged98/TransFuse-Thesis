//
// Created by george-elfayoumi on 7/29/24.
//

#include "moderndbs/file_mapper.h"
#include "moderndbs/segment.h"
#include <filesystem>
#include <iostream>
#include <random>
#include <stdexcept>
#include <utility>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace moderndbs {
    static thread_local std::mt19937_64 engine{std::random_device{}()}; // Use a random device as seed

    FileMapper::FileMapper(std::string filename, const size_t page_size)
        : filename_(std::move(filename)), page_size_(page_size), file_size_(0), mmap_ptr_(nullptr) {
        // lock mmaped file
        // adapt the mmaped size while having the lock
        // check mmap size in boltDB

        map_file(minMmapFileSize);
    }

    FileMapper::~FileMapper() {
        if (mmap_ptr_) {
            munmap(mmap_ptr_, file_size_);
        }
    }

    std::shared_ptr<Page> FileMapper::get_page(const size_t page_number, const bool is_exclusive) {
        if (page_number >= num_pages_) {
            this->map_file(file_size_ + page_size_);
            //throw std::out_of_range("Page number out of range");
        }

        const size_t pos = page_number * page_size_;
        char *mapped_data = static_cast<char *>(mmap_ptr_) + pos;
        // const auto page = new Page(mapped_data, page_size_);
        // cout << "HERE " << pos << "\n";
        shared_ptr<Page> page_ptr = std::make_shared<Page>(mapped_data, page_size_);
        page_ptr->set_id(page_number);
        page_ptr->set_exclusive(is_exclusive);

        if (is_exclusive) {
            page_ptr->custom_latch.lock_exclusive();
        } else {
            page_ptr->custom_latch.lock_shared();
        }

        page_ptr->set_readers_count(page_ptr->custom_latch.readers_count.load());
        page_ptr->set_state(static_cast<int>(page_ptr->custom_latch.state.load()));

        // if (msync(mmap_ptr_ + pos, page_size_, MS_SYNC) == -1) {
        //    perror("msync");
        //    munmap(mmap_ptr_, file_size_);
        // }
        //
        // if (madvise(mmap_ptr_ + pos, page_size_, MADV_DONTNEED) == -1) {
        //    perror("madvise");
        // }
        //
        // unsigned char *vec = static_cast<unsigned char *>(calloc(2, sizeof(unsigned char)));
        // if (!vec) {
        //    perror("calloc");
        //    munmap(mmap_ptr_, file_size_);
        // }
        //
        // if (mincore(mmap_ptr_ + pos, page_size_, vec) != 0) {
        //    perror("mincore");
        //    free(vec);
        //    munmap(mmap_ptr_, file_size_);
        // }
        //
        // for (size_t i = 0; i < 2; i++) {
        //    printf("Page %zu: %s\n", i, (vec[i] & 1) ? "Resident" : "Not resident");
        // }
        //
        // free(vec);

        return page_ptr;
    }


    void *FileMapper::get_page_address_in_mapped_region(const size_t page_number) const {
        const size_t pos = page_number * page_size_;
        char *mapped_data = static_cast<char *>(mmap_ptr_) + pos;
        int readers_count_ref = *reinterpret_cast<const int *>(mapped_data);
        int state = *reinterpret_cast<const int *>(mapped_data + sizeof(int));
        return mapped_data;
    }


    void FileMapper::make_no_op(const size_t page_offset) {
        cout << "INSIDE NO OP \n";
        if (mmap_ptr_ == nullptr || mmap_ptr_ == MAP_FAILED) {
            std::cerr << "Error: mmap_ptr_ is invalid.\n";
            return;
        }

        if (page_offset >= file_size_) {
            std::cerr << "Error: page_offset is out of bounds.\n";
            return;
        }
        char *mapped_data = static_cast<char *>(mmap_ptr_) + page_offset;
        std::cout << "Accessing mapped memory at offset: " << page_offset << "\n";
        // int readers_count_ref = *reinterpret_cast<const int *>(mapped_data);
        // std::cout << "First int: " << readers_count_ref << "\n";
        // int state = *reinterpret_cast<const int *>(mapped_data + sizeof(int));
        // std::cout << "Second int: " << state << "\n";

        char *byte_to_modify = mapped_data;
        char original_value = *byte_to_modify;
        *byte_to_modify = original_value;
        std::cout << "No-op write performed at offset " << page_offset << " (address: " << static_cast<void *>(byte_to_modify) << ").\n";
    }

    /**
     * Function to check if a memory address is resident in memory.
     *
     * @param page_number: Page number in file that we want to check if it's resident in memory.
     * @return: 1 if the page is resident, 0 if not, -1 on error.
     */
    int FileMapper::is_memory_resident(const size_t page_offset) {
        void *addr = mmap_ptr_ + page_offset;
        unsigned char vec; // Only one page, so ONLY one byte is needed

        // Check if page is resident in memory using mincore
        if (mincore(addr, page_size_, &vec) != 0) {
            std::cerr << "mincore failed: " << strerror(errno) << "\n";
            return -1;
        }

        // Check the least significant bit to determine residency
        return (vec & 1) ? 1 : 0;
    }


    void FileMapper::release_page(std::shared_ptr<Page> page) {
        // Create a Bernoulli distribution with an 10% chance of success
        // std::bernoulli_distribution d(0.1);
        // if (bool success = d(engine)) {
        //    std::cout << " WAIT\n";
        //    std::this_thread::sleep_for(std::chrono::milliseconds(200));
        // }
        if (page->is_exclusive()) {
            page->custom_latch.unlock_exclusive();
        } else {
            page->custom_latch.unlock_shared();
        }
        page->set_readers_count(page->custom_latch.readers_count.load());
        page->set_state(static_cast<int>(page->custom_latch.state.load()));
    }

    uint64_t FileMapper::calculate_file_size(const uint64_t oldFileSize) const {
        // Double the size from 32KB until 1GB.
        for (unsigned int i = 15; i <= 30; ++i) {
            if (oldFileSize <= (1 << i)) {
                return 1 << i;
            }
        }

        // Verify the requested size is not above the maximum allowed.
        if (oldFileSize > maxMapSize) {
            throw std::runtime_error("mmap too large");
        }

        // If larger than 1GB, then grow by 1GB at a time.
        auto sz = static_cast<int64_t>(oldFileSize);
        if (uint64_t remainder = sz % maxMmapStep; remainder > 0) {
            sz += maxMmapStep - remainder;
        }

        // Ensure that the mmap size is a multiple of the page size.
        if (sz % page_size_ != 0) {
            sz = ((sz / page_size_) + 1) * page_size_;
        }

        // If we've exceeded the max size, then only grow up to the max size.
        if (sz > maxMapSize) {
            sz = maxMapSize;
        }

        return static_cast<uint64_t>(sz);
    }

    void FileMapper::write_to_file(const void *data, size_t size) const {
        int fd = open(filename_.c_str(), O_RDWR | O_CREAT | O_APPEND, static_cast<mode_t>(0664));

        if (fd == -1) {
            perror("open");
            exit(1);
        }

        // Write the data to the file
        ssize_t written = pwrite(fd, data, size, 0);
        if (written == -1) {
            perror("write");
            close(fd);
            exit(1);
        }

        if (static_cast<size_t>(written) != size) {
            std::cerr << "Partial write: expected " << size << " bytes, wrote " << written << " bytes.\n";
            close(fd);
            exit(1);
        }
        close(fd);
    }

    void FileMapper::msync_file(const uint64_t page_offset, const uint64_t write_size) const {
        char *mapped_data = static_cast<char *>(mmap_ptr_) + page_offset;
        int readers_count_ref = *reinterpret_cast<const int *>(mapped_data);
        int state = *reinterpret_cast<const int *>(mapped_data + sizeof(int));
        // printf(" PAGE OFFSET %lu  First int: %d, Second int: %d\n", page_offset, readers_count_ref, state);
        if ((reinterpret_cast<uintptr_t>(mmap_ptr_ + page_offset) % sysconf(_SC_PAGESIZE)) != 0) {
            printf("Address is not page-aligned.\n");
        }
        int res;
        int retry_count = 0;
        int MAX_RETRIES = 3;
        while (retry_count < MAX_RETRIES && ((res = msync(mmap_ptr_ + page_offset, write_size, MS_ASYNC) == -1))) {
            printf("Retrying write attempt %d\n", retry_count + 1);
            retry_count++;
        }

        if (res == -1) {
            perror("Error writing to file after retries");
        }
        // if (msync(mmap_ptr_ + page_offset, page_size_, MS_ASYNC) == -1) {
        //    printf("msync error: %d, %s\n", errno, strerror(errno));
        // }
    }

    void FileMapper::append_to_wal_file(const char *data, size_t data_size) {
        std::shared_ptr<Page> page = get_page(0, true);
        const auto &latest_flushed_LSN = *reinterpret_cast<uint64_t *>(page->get_data());
        // printf(" BEFORE APPENDING: LATEST FLUSHED LSN: %lu \n", latest_flushed_LSN);
        /// sizeof(uint64_t) -> LSN COUNT
        size_t offset = headerSize + lockDataSize + sizeof(uint64_t) + latest_flushed_LSN * sizeof(LogRecord);
        /// TODO: File Resizing
        uint64_t new_file_size = offset + data_size;
        if (new_file_size > file_size_) {
            map_file(file_size_ + data_size);
        }
        char *data_start = static_cast<char *>(mmap_ptr_) + offset;
        uint64_t num_records = data_size / sizeof(LogRecord);
        uint64_t new_lsn_count = latest_flushed_LSN + num_records;
        *reinterpret_cast<uint64_t *>(page->get_data()) = new_lsn_count;
        // printf(" LATEST FLUSHED LSN: %lu \n", new_lsn_count);
        memcpy(data_start, data, data_size);
        release_page(page);
        // TODO: msync
        // Write it now to disk
        if (msync(mmap_ptr_, new_file_size, MS_ASYNC) == -1) {
            perror("Could not sync the file to disk");
        }
    };

    void FileMapper::sync_file_range_with_disk(const uint64_t page_offset_in_file, const uint64_t write_size) const {
        struct stat file_stat;
        if (fstat(fd, &file_stat) == -1) {
            perror("fstat error");
        } else if (S_ISREG(file_stat.st_mode)) {
            std::cout << "fd is a regular file." << std::endl;
        } else {
            std::cerr << "fd does not refer to a regular file." << std::endl;
        }
        /// sync_file_range() permits fine control when synchronizing the  open file referred to by the file descriptor fd with disk.
        /// SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE
        /// Ensures that all pages in the specified range which were dirty when sync_file_range() was called are placed under write-out.
        /// This is a start-write-for-data-integrity operation.
        /// https://www.man7.org/linux/man-pages/man2/sync_file_range.2.html
        int res = sync_file_range(fd, page_offset_in_file, write_size,
                                  SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE);
        if (res == -1) {
            printf("sync_file_range error: %d, %s\n", errno, strerror(errno));
        }
    }
} // transfuse
