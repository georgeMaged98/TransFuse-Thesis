//
// Created by george-elfayoumi on 7/29/24.
//

#include "moderndbs/file_mapper.h"
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
   page_ptr->set_count(0);
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
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
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

   return static_cast<int>(sz);
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

void FileMapper::msync_file(uint64_t page_number) {
   const size_t pos = page_number * page_size_;
   if (msync(mmap_ptr_ + pos, page_size_, MS_SYNC) == -1) {
      perror("msync failed");
   }
}

} // transfuse
