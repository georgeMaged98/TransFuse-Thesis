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

   // Initialize Locks from file.
   // int& readers_count_ref = *reinterpret_cast<int*>(new_frame->get_data_with_locks());
   // const std::atomic_ref<int> readers_count_atomic(readers_count_ref);
   // new_frame->custom_latch.readers_count.store(readers_count_atomic.load());  // Use atomic_ref to load the readers count atomically
   // // Interpret the next sizeof(int) bytes of the data buffer as state
   // int& state_ref = *reinterpret_cast<int*>(new_frame->get_data_with_locks() + sizeof(int));
   // std::atomic_ref<int> state_atomic(state_ref);
   // new_frame->custom_latch.state.store(static_cast<CustomReadWriteLock::State>(state_atomic.load()));


   if (is_exclusive) {
       page_ptr->custom_latch.lock_exclusive();
   } else {
       page_ptr->custom_latch.lock_shared();
   }
   return page_ptr;
}

void FileMapper::release_page(std::shared_ptr<Page> page) {
   // const auto data = page->get_data_with_locks();
   // // Write the readers_count and state to the first bytes of data using atomic_ref
   // int& readers_count_ref = *reinterpret_cast<int*>(data);
   // std::atomic_ref<int> readers_count_atomic(readers_count_ref);
   // readers_count_atomic.store(page->custom_latch.readers_count.load());
   //
   // // Store state in the next bytes of the data buffer using atomic_ref
   // int& state_ref = *reinterpret_cast<int*>(data + sizeof(int));
   // std::atomic_ref<int> state_atomic(state_ref);
   // state_atomic.store(static_cast<int>(page->custom_latch.state.load()));

   if(page->is_exclusive()) {
      page->custom_latch.unlock_exclusive();
   }else {
      page->custom_latch.unlock_shared();
   }
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
