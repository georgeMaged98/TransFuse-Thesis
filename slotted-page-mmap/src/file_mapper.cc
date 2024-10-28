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
#include <gtest/internal/gtest-port.h>
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
      if (mmap_ptr_) {
         if (msync(mmap_ptr_, file_size_, MS_SYNC) == -1) {
            perror("msync failed");
            return nullptr;
         }

         if (munmap(mmap_ptr_, file_size_) == -1) {
            perror("Failed to unmap file");
            return nullptr;
         }
         mmap_ptr_ = nullptr;
      }
      this->map_file(file_size_ + page_size_);
      //throw std::out_of_range("Page number out of range");
   }

   const size_t pos = page_number * page_size_;
   char* mapped_data = static_cast<char*>(mmap_ptr_) + pos;
   // const auto page = new Page(mapped_data, page_size_);

   auto page_ptr = std::make_shared<Page>(mapped_data, page_size_);
   page_ptr->set_id(page_number);
   page_ptr->set_exclusive(is_exclusive);

   if (is_exclusive) {
      page_ptr->custom_latch.lock_exclusive();
   } else {
      page_ptr->custom_latch.lock_shared();
   }

   page_ptr->set_readers_count(page_ptr->custom_latch.readers_count.load());
   page_ptr->set_state(static_cast<int>(page_ptr->custom_latch.state.load()));

   if (madvise(mmap_ptr_ + pos, page_size_, MADV_DONTNEED) == -1) {
      perror("madvise");
   }

   return page_ptr;
}

void FileMapper::release_page(std::shared_ptr<Page> page) {
   // Create a Bernoulli distribution with an 10% chance of success
   std::bernoulli_distribution d(0.1);
   if (bool success = d(engine)) {
      std::cout << " WAIT\n";
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
   }
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

void FileMapper::write_to_file(const void* data, size_t size) const {
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

void FileMapper::msync_file(const uint64_t page_offset) const {
   char* mapped_data = static_cast<char*>(mmap_ptr_) + page_offset;
   int readers_count_ref = *reinterpret_cast<const int*>(mapped_data);
   int state = *reinterpret_cast<const int*>(mapped_data + sizeof(int));
   printf(" PAGE OFFSET %lu  First int: %d, Second int: %d\n", page_offset, readers_count_ref, state);
   if ((reinterpret_cast<uintptr_t>(mmap_ptr_ + page_offset) % sysconf(_SC_PAGESIZE)) != 0) {
      printf("Address is not page-aligned.\n");
   }
   int res;
   int retry_count = 0;
   int MAX_RETRIES = 3;
   while (retry_count < MAX_RETRIES && ((res = msync(mmap_ptr_ + page_offset, page_size_, MS_SYNC) == -1))) {
      printf("Retrying write attempt %d\n", retry_count + 1);
      retry_count++;
   }

   if (res == -1) {
      perror("Error writing to file after retries");
   }
   // if (msync(mmap_ptr_ + page_offset, page_size_, MS_SYNC) == -1) {
   //    printf("msync error: %d, %s\n", errno, strerror(errno));
   // }
}

void FileMapper::append_to_wal_file(const char* data, size_t data_size) {
   std::shared_ptr<Page> page = get_page(0, true);
   const auto& latest_flushed_LSN = *reinterpret_cast<uint64_t*>(page->get_data());
   printf(" BEFORE APPENDING: LATEST FLUSHED LSN: %lu \n", latest_flushed_LSN);
   /// sizeof(uint64_t) -> LSN COUNT
   size_t offset = headerSize + lockDataSize + sizeof(uint64_t) + latest_flushed_LSN * sizeof(LogRecord);
   /// TODO: File Resizing
   uint64_t new_file_size = offset + data_size;
   if (new_file_size > file_size_) {
      map_file(file_size_ + data_size);
   }
   char* data_start = static_cast<char*>(mmap_ptr_) + offset;
   uint64_t num_records = data_size / sizeof(LogRecord);
   uint64_t new_lsn_count = latest_flushed_LSN + num_records;
   *reinterpret_cast<uint64_t*>(page->get_data()) = new_lsn_count;
   printf(" LATEST FLUSHED LSN: %lu \n", new_lsn_count);
   memcpy(data_start, data, data_size);
   release_page(page);
   // TODO: msync
   // Write it now to disk
   if (msync(mmap_ptr_, new_file_size, MS_SYNC) == -1) {
      perror("Could not sync the file to disk");
   }
};

} // transfuse
