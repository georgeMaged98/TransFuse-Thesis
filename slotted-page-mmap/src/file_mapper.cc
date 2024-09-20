//
// Created by george-elfayoumi on 7/29/24.
//

#include "moderndbs/file_mapper.h"
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <utility>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace moderndbs {
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

Page* FileMapper::get_page(const size_t page_number, const bool is_exclusive) {
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
   const auto page = new Page(mapped_data, page_size_);
   page->set_id(page_number);
   page->set_count(0);
   page->set_exclusive(is_exclusive);
   // if (is_exclusive) {
   //     page->latch.lock();
   // } else {
   //     page->latch.lock_shared();
   // }
   return page;
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
} // transfuse
