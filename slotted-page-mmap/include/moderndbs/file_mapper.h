//
// Created by george-elfayoumi on 7/29/24.
//

#ifndef FILEMAPPER_H
#define FILEMAPPER_H

#include "custom_read_writer_lock.h"

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <shared_mutex>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;

namespace moderndbs {

class Page {
   public:
   // Delete the copy constructor and assignment operator
   Page(const Page&) = delete;

   Page& operator=(const Page&) = delete;

   explicit Page(char* mapped_data, size_t page_size) {
      size_t offset = 0;

      // readers_count and state mapped from file
      readers_count = reinterpret_cast<int*>(mapped_data + offset);
      offset += sizeof(int);

      state = reinterpret_cast<int*>(mapped_data + offset);
      offset += sizeof(int);

      // id, lsn, exclusive fields mapped from file
      id = reinterpret_cast<size_t*>(mapped_data + offset);
      offset += sizeof(size_t);

      // Map the lsn field
      lsn = reinterpret_cast<size_t*>(mapped_data + offset);
      offset += sizeof(size_t);

      // Map the exclusive field
      exclusive = reinterpret_cast<bool*>(mapped_data + offset);
      offset += sizeof(bool);

      // Calculate the remaining space for the data field
      data_size = page_size - offset;
      data = mapped_data + offset;

      // Optional: Check memory alignment
      if (reinterpret_cast<uintptr_t>(mapped_data) % alignof(size_t) != 0) {
         std::cerr << "Memory is not properly aligned for size_t\n";
         throw std::runtime_error("Memory alignment issue");
      }
   }

   // Mutators
   void set_id(const size_t new_id) const { *id = new_id; }
   void set_lsn(const size_t new_lsn) const { *lsn = new_lsn; }
   void set_exclusive(bool is_exclusive) const { *exclusive = is_exclusive; }
   void set_readers_count(const int new_readers_count) const { *readers_count = new_readers_count; }
   void set_state(const int new_state) const { *state = new_state; }

   /// Returns a pointer to this page's data.
   [[nodiscard]] size_t get_id() const { return *id; }
   [[nodiscard]] size_t get_lsn() const { return *lsn; }
   [[nodiscard]] bool is_exclusive() const { return *exclusive; }
   [[nodiscard]] int get_readers_count() const { return *readers_count; }
   [[nodiscard]] int get_state() const { return *state; }

   // Get the raw data pointer
   [[nodiscard]] char* get_data() const { return data; }
   [[nodiscard]] char* get_data_with_header() { return reinterpret_cast<char*>(readers_count); }

   // custom latch
   CustomReadWriteLock custom_latch;

   private:
   friend class Segment;

   int* readers_count = nullptr;
   int* state = nullptr;
   size_t* id = nullptr;
   size_t* lsn = nullptr;
   bool* exclusive = nullptr;
   char* data = nullptr;
   size_t data_size = 0;

};

class FileMapper {
   static constexpr uint64_t maxMapSize = 10LL << 30; // 10GB
   static constexpr uint64_t maxMmapStep = 1LL << 30; // 1GB

   public:
   static constexpr uint32_t headerSize = 2 * sizeof(size_t) + 1;
   static constexpr uint32_t lockDataSize = 2 * sizeof(int);
   std::string filename_;

   FileMapper(std::string filename, size_t page_size, uint64_t initial_file_size);

   ~FileMapper();

   [[nodiscard]] std::shared_ptr<Page> get_page(size_t page_number, bool is_exclusive);

   [[nodiscard]] void* get_page_address_in_mapped_region(size_t page_number) const;

   [[nodiscard]] int is_memory_resident(size_t page_offset);

   void make_no_op(size_t page_offset);

   void release_page(std::shared_ptr<Page> page);

   [[nodiscard]] size_t get_page_size() const { return page_size_; }

   [[nodiscard]] size_t get_data_size() const { return page_size_ - headerSize - lockDataSize; } // id, lsn, exclusive, readers_count, state

   void write_to_file(const void* data, size_t size) const;

   void append_to_wal_file(const char *data, size_t size);

   void msync_file(uint64_t page_offset, uint64_t write_size) const;

   void sync_file_range_with_disk(uint64_t page_offset_in_file, uint64_t write_size) const;

   private:
   size_t page_size_;
   uint64_t file_size_;
   uint64_t minMmapFileSize;

   /// Max number of pages that the current file can hold.
   /// (e.g.) a file with size of 32KB can hold (32KB / page_size) entries.
   size_t num_pages_;
   void* mmap_ptr_;
   /// File Descriptor
   int fd;
   [[nodiscard]] uint64_t calculate_file_size(uint64_t oldFileSize) const;

   void map_file(const uint64_t minSize) {
      fd = open(filename_.c_str(), O_RDWR | O_CREAT | O_APPEND,
                          static_cast<mode_t>(0664));

      if (fd == -1) {
         perror("Failed to open file");
         return;
      }

      struct stat fileInfo {};
      if (stat(filename_.c_str(), &fileInfo) == -1) {
         perror("stat");
         exit(1);
      }

      uint64_t size = minSize;
      if (fileInfo.st_size > minSize) {
         size = fileInfo.st_size;
      }

      size_t oldFileSize = fileInfo.st_size;
      size_t newFileSize = calculate_file_size(size);
      file_size_ = newFileSize;

      if (file_size_ < minMmapFileSize) {
         file_size_ = minMmapFileSize;
      }

      if (ftruncate(fd, file_size_) == -1) {
         perror("Failed to set file size");
         close(fd);
         return;
      }
      num_pages_ = file_size_ / page_size_;

      if (mmap_ptr_) {
         void* new_mmap_ptr = mremap(mmap_ptr_, oldFileSize, newFileSize, 0);
         if (new_mmap_ptr == MAP_FAILED) {
            close(fd);
            std::cerr << strerror(errno) << "\n";
            perror("Failed to mmap file");
            return;
         }
         mmap_ptr_ = new_mmap_ptr;

         // if (msync(mmap_ptr_, file_size_, MS_ASYNC) == -1) {
         //    perror("msync failed");
         //    return nullptr;
         // }
         //
         // if (munmap(mmap_ptr_, file_size_) == -1) {
         //    perror("Failed to unmap file");
         //    return nullptr;
         // }
         // mmap_ptr_ = nullptr;
      }else {
         mmap_ptr_ = mmap(nullptr, file_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
         if (mmap_ptr_ == MAP_FAILED) {
            close(fd);
            std::cerr << strerror(errno) << "\n";
            perror("Failed to mmap file");
            return;
         }
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
