#include "moderndbs/buffer_manager.h"
#include "moderndbs/file.h"

using namespace std;

namespace moderndbs {
////////////////////////////////////////// ADDED FUNCTIONS //////////////////////////////////////////

// check if buffer manager is full
bool BufferManager::is_buffer_manager_full() const {
   return (fifo_list.size() + lru_list.size()) == page_count;
}

std::shared_ptr<BufferFrame> BufferManager::find_buffer_frame_by_page_id(uint64_t page_id) {
   auto output_it = hashtable.find(page_id);
   if (output_it == hashtable.end()) {
      return nullptr;
   }

   return output_it->second;
}

// void BufferManager::write_buffer_frame_to_file(uint64_t pageNo, char* data) const {
//    auto segment_id = get_segment_id(pageNo);
//    try {
//       std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(),
//                                                    File::Mode::WRITE);
//       size_t file_offset = get_segment_page_id(pageNo) * page_size;
//
//       // Check if the offset exceeds the file size before resizing
//       if (file_offset + this->page_size > file->size()) {
//          file->resize(file_offset + this->page_size);
//       }
//
//       // File size is the offset of the new page
//       file->write_block(data, file_offset, this->page_size);
//       // Create an unordered map to save the offset of each page.
//       // We know segment id from the function, so we only need to save the offset.
//    } catch (const std::exception& e) {
//       std::cout << "File Not Found!! \n";
//    }
// }

void BufferManager::write_buffer_frame_to_file(uint64_t pageNo, char* data) const {
   auto segment_id = get_segment_id(pageNo);

   std::unique_ptr<File>& file = file_handles[segment_id]; // Direct access to file handle

   {
      // Lock the cache to ensure thread-safe access
      std::lock_guard<std::mutex> lock(file_cache_mutex);

      if (!file) {
         // File is not open; open it now in WRITE mode
         std::string file_name = std::to_string(segment_id) + ".txt";
         file = File::open_file(file_name.c_str(), File::Mode::WRITE);
      }
   }

   size_t file_offset = get_segment_page_id(pageNo) * page_size;

   // Check if the offset exceeds the file size before resizing
   if (file_offset + this->page_size > file->size()) {
      file->resize(file_offset + this->page_size);
   }

   // Write the data to the file at the calculated offset
   file->write_block(data, file_offset, this->page_size);
}

// void BufferManager::read_buffer_frame_from_file(uint64_t page_id, BufferFrame& bf) const {
//    auto segment_id = get_segment_id(page_id);
//    try {
//       std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(),
//                                                    File::Mode::READ);
//       size_t page_offset_in_file = get_segment_page_id(page_id) * this->page_size;
//
//       file->read_block(page_offset_in_file, this->page_size, bf.get_data_with_locks());
//    } catch (const std::exception& e) {
//       std::cerr << "Error: " << e.what() << "\n";
//    }
// }

void BufferManager::read_buffer_frame_from_file(uint64_t page_id, BufferFrame& bf) const {
   auto segment_id = get_segment_id(page_id);
   std::unique_ptr<File>& file = file_handles[segment_id]; // Direct access to file handle

   {
      // Lock the cache for thread-safe access
      std::lock_guard<std::mutex> lock(file_cache_mutex);

      if (!file) {
         // File is not open; open it now
         std::string file_name = std::to_string(segment_id) + ".txt";
         file = File::open_file(file_name.c_str(), File::Mode::WRITE);
      }
   }

   // Perform the read operation
   size_t page_offset_in_file = get_segment_page_id(page_id) * this->page_size;
   file->read_block(page_offset_in_file, this->page_size, bf.get_data_with_locks());
}

void BufferManager::evict_page() {
   // check fifo queue -> if not empty, evict first element in fifo queue
   int64_t evicted_page_id = -1;

   // Check FIFO queue for a candidate
   for (auto it = fifo_list.begin(); it != fifo_list.end(); ++it) {
      auto bf_it = hashtable.find(*it);
      if (bf_it == hashtable.end()) {
         continue; // Skip if page is not found in hashtable
      }

      const auto bf = bf_it->second;
      if (bf->num_fixed == 0 && !bf->is_dirty && !bf->is_evicted) {
         evicted_page_id = *it; // Non-dirty page found
         break;
      }
      if (bf->num_fixed == 0 && !bf->is_evicted) {
         evicted_page_id = *it; // Dirty page fallback
         break;
      }
   }

   // Check LRU queue if no candidate found in FIFO
   if (evicted_page_id == -1) {
      for (auto it = lru_list.begin(); it != lru_list.end(); ++it) {
         auto bf_it = hashtable.find(*it);
         if (bf_it == hashtable.end()) {
            continue; // Skip if page is not found in hashtable
         }

         const auto bf = bf_it->second;
         if (bf->num_fixed == 0 && !bf->is_dirty && !bf->is_evicted) {
            evicted_page_id = *it; // Non-dirty page found
            break;
         }
         if (bf->num_fixed == 0 && !bf->is_evicted) {
            evicted_page_id = *it; // Dirty page fallback
            break;
         }
      }
   }

   // std::cout << "evict!! " << evicted_page_id <<std::endl;

   //  Update Hashtable
   auto ito = this->hashtable.find(evicted_page_id);
   if (ito != this->hashtable.end()) {
      std::shared_ptr<BufferFrame> bf_to_be_deleted = ito->second;
      bf_to_be_deleted->is_evicted = true;

      // If page is dirty, write it to disk:
      if (bf_to_be_deleted->is_dirty) {
         // We unlock directory (buffer_manager_mutex) here to avoid holding the lock during an expensive operation (Writing to disk)
         buffer_manager_mutex.unlock();
         bf_to_be_deleted->custom_latch.lock_exclusive();
         const auto pageNo = bf_to_be_deleted->pageNo;
         const auto data = bf_to_be_deleted->get_data_with_locks();

         // Write the readers_count and state to the first bytes of data using atomic_ref
         int& readers_count_ref = *reinterpret_cast<int*>(data);
         std::atomic<int> readers_count_atomic(readers_count_ref);
         readers_count_atomic.store(static_cast<int>(bf_to_be_deleted->custom_latch.readers_count.load()));

         // Store state in the next bytes of the data buffer using atomic_ref
         int& state_ref = *reinterpret_cast<int*>(data + sizeof(int));
         std::atomic<int> state_atomic(state_ref);
         state_atomic.store(static_cast<int>(bf_to_be_deleted->custom_latch.state.load()));

         // Write the Buffer Frame to disk
         // auto segment_id = get_segment_id(pageNo);
         // std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(),
         //                                              File::Mode::WRITE);
         // size_t file_offset = get_segment_page_id(pageNo) * page_size;
         // file->resize(file->size() + this->page_size);
         // // File size is the offset of the new page
         // file->write_block(data, file_offset, this->page_size);

         write_buffer_frame_to_file(pageNo, data);
         bf_to_be_deleted->custom_latch.unlock_exclusive();
         // bf_to_be_deleted->latch.unlock();
         buffer_manager_mutex.lock();
      }

      if (bf_to_be_deleted->num_fixed == 0) {
         if (fifo_map.find(evicted_page_id) != fifo_map.end()) {
            // Page is in FIFO queue
            fifo_list.erase(fifo_map[evicted_page_id]); // O(1)
            fifo_map.erase(evicted_page_id); // O(1)
            //                    fifo_set.erase(evicted_page_id);            // Clean up set
         } else if (lru_map.find(evicted_page_id) != lru_map.end()) {
            // Page is in LRU queue
            lru_list.erase(lru_map[evicted_page_id]); // O(1)
            lru_map.erase(evicted_page_id); // O(1)
            //                    lru_set.erase(evicted_page_id);             // Clean up set
         }
         // Finally, remove from hashtable
         hashtable.erase(evicted_page_id);
      }
   }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

char* BufferFrame::get_data() const {
   // Skip the first 2 * sizeof(int) bytes, which are reserved for readers_count and state
   return data.get() + 2 * sizeof(int);
}

char* BufferFrame::get_data_with_locks() const {
   return data.get();
}

BufferManager::BufferManager(size_t page_size, size_t page_count) {
   num_fixed_pages = 0;
   this->page_size = page_size;
   this->page_count = page_count;
   this->hashtable.reserve(page_count);
}

BufferManager::~BufferManager() {
   // Group pages by segment ID
   std::unordered_map<uint64_t, std::vector<std::pair<size_t, char*>>> segment_pages;

   for (auto& i : this->hashtable) {
      if (i.second->is_dirty) {
         uint64_t segment_id = get_segment_id(i.second->pageNo);
         size_t file_offset = get_segment_page_id(i.second->pageNo) * page_size;
         segment_pages[segment_id].emplace_back(file_offset, i.second->get_data_with_locks());
      }
   }

   // Process each segment
   for (auto& [segment_id, pages] : segment_pages) {
      try {
         // std::cout << " Writing Segment: " << segment_id << " Number of pages: " << pages.size() << std::endl;
         // Open the file for the segment
         std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(), File::Mode::WRITE);

         // Sort dirty pages by offset to identify contiguous regions
         ranges::sort(pages);

         size_t batch_start = 0; // Start of the current batch
         size_t batch_end = 0; // End of the current batch
         std::vector<char> batch_buffer; // Temporary buffer for batched writes

         size_t progress_threshold = std::max<size_t>(1, pages.size() / 10); // 10% threshold, at least 1 page
         size_t next_progress = progress_threshold; // First progress checkpoint

         for (size_t i = 0; i < pages.size(); ++i) {
            size_t offset = pages[i].first;
            char* data = pages[i].second;

            if (batch_buffer.empty()) {
               // Start a new batch
               batch_start = offset;
               batch_end = offset + page_size;
               batch_buffer.resize(page_size);
               std::memcpy(batch_buffer.data(), data, page_size);
            } else if (offset == batch_end) {
               // Extend the current batch
               batch_buffer.resize(batch_buffer.size() + page_size);
               std::memcpy(batch_buffer.data() + (batch_end - batch_start), data, page_size);
               batch_end += page_size;
            } else {
               // Write the current batch to disk
               file->write_block(batch_buffer.data(), batch_start, batch_buffer.size());

               // Start a new batch
               batch_start = offset;
               batch_end = offset + page_size;
               batch_buffer.clear();
               batch_buffer.resize(page_size);
               std::memcpy(batch_buffer.data(), data, page_size);
            }

            // Update progress
            if (i + 1 >= next_progress) {
               size_t percentage = (i + 1) * 100 / pages.size();
               // std::cout << "Segment " << segment_id << ": " << percentage << "% complete (" << (i + 1) << "/" << pages.size() << " pages)." << std::endl;
               next_progress += progress_threshold; // Move to the next 10% checkpoint
            }

         }

         // Write the final batch
         if (!batch_buffer.empty()) {
            file->write_block(batch_buffer.data(), batch_start, batch_buffer.size());
         }
      } catch (const std::exception& e) {
         std::cerr << "Error writing segment " << segment_id << ": " << e.what() << std::endl;
      }
   }


   for (auto& [segment_id, file] : file_handles) {
           file.reset(); // Close the file
       }

   // Above code introduced batched writes.
   // OLD Destructor code, was very slow because it processes each page separately (Above introduced contiguous page writes batched together.).
   // // Process each segment
   // for (auto& [segment_id, pages] : segment_pages) {
   //    try {
   //       // Open the file for the segment
   //       std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(),
   //                                                    File::Mode::WRITE);
   //
   //       for (const auto& [offset, data] : pages) {
   //          // Write only the dirty page to the corresponding offset
   //          // file->write_block(data, offset, page_size);
   //
   //       }
   //    } catch (const std::exception& e) {
   //       std::cerr << "Error writing segment " << segment_id << ": " << e.what() << std::endl;
   //    }
   // }



}

std::shared_ptr<BufferFrame> BufferManager::fix_page(uint64_t page_id, bool exclusive) {
   // lock mutex
   std::unique_lock bf_lock(buffer_manager_mutex);
   if (this->num_fixed_pages == this->page_count) {
      throw buffer_full_error{};
   }

   // Check if buffer manager is full
   if (this->is_buffer_manager_full()) {
      // EVICT PAGE TO DISK
      evict_page();
   }

   num_fixed_pages.fetch_add(1ul);

   // update FIFO queue if needed and // 5. update LRU queue if needed
   if (fifo_map.find(page_id) != fifo_map.end()) {
      // Page exists in FIFO queue; move it to LRU queue
      auto it = fifo_map[page_id];
      fifo_list.erase(it); // Remove from FIFO list (O(1))
      fifo_map.erase(page_id); // Remove from FIFO map (O(1))

      lru_list.push_back(page_id); // Add to back of LRU list (O(1))
      lru_map[page_id] = --lru_list.end(); // Update LRU map (O(1))
   } else if (lru_map.find(page_id) != lru_map.end()) {
      // Page already in LRU queue; move it to the back
      auto it = lru_map[page_id];
      lru_list.erase(it); // Remove from current position in LRU list (O(1))
      lru_list.push_back(page_id); // Add to back of LRU list (O(1))
      lru_map[page_id] = --lru_list.end(); // Update LRU map (O(1))
   } else {
      // Page is new; add it to FIFO queue
      fifo_list.push_back(page_id); // Add to back of FIFO list (O(1))
      fifo_map[page_id] = --fifo_list.end(); // Update FIFO map (O(1))
   }

   std::shared_ptr<BufferFrame> buffer_frame = this->find_buffer_frame_by_page_id(page_id);
   if (buffer_frame != nullptr) {
      buffer_frame->num_fixed += 1;
      buffer_frame->is_evicted = false;
      bf_lock.unlock();
      if (exclusive) {
         buffer_frame->custom_latch.lock_exclusive();
         buffer_frame->is_exclusive = true;
      } else {
         buffer_frame->custom_latch.lock_shared();
         buffer_frame->is_exclusive = false;
      }

      return buffer_frame;
   }

   // Page with page_id not found
   std::shared_ptr<BufferFrame> new_frame = std::make_shared<BufferFrame>();
   // Update Hashtable
   hashtable.insert({page_id, new_frame});
   new_frame->num_fixed = 1;
   new_frame->data = std::make_unique<char[]>(this->page_size);
   new_frame->pageNo = page_id;
   new_frame->is_dirty = false;
   new_frame->is_evicted = false;
   bf_lock.unlock();

   // Always acquire an exclusive lock when performing a disk read
   new_frame->custom_latch.lock_exclusive();
   new_frame->is_exclusive = true;

   // Perform disk read after acquiring the exclusive lock
   // reading into frame is a write operation, hence exclusive lock is needed.
   // Lock is reduced to shared lock if request is shared mode
   read_buffer_frame_from_file(page_id, *new_frame);

   // Downgrade the lock to shared if the operation was not exclusive
   if (!exclusive) {
      new_frame->custom_latch.unlock_exclusive();
      new_frame->custom_latch.lock_shared();
      new_frame->is_exclusive = false;
   }

   // OLD CODE, reading into frame is a write operation, hence exclusive lock is needed.
   // Lock is reduced to shared lock if request is shared mode
   // if (exclusive) {
   //    new_frame->custom_latch.lock_exclusive();
   //    new_frame->is_exclusive = true;
   // } else {
   //    new_frame->custom_latch.lock_shared();
   //    new_frame->is_exclusive = false;
   // }
   //
   // // load page from disk if it is not in memory(NOT in Hashtable). Use segment id to read from disk & update data field in the BufferFrame
   // read_buffer_frame_from_file(page_id, *new_frame);

   // Initialize Locks from file.
   // int& readers_count_ref = *reinterpret_cast<int*>(new_frame->get_data_with_locks());
   // const std::atomic_ref<int> readers_count_atomic(readers_count_ref);
   // new_frame->custom_latch.readers_count.store(readers_count_atomic.load()); // Use atomic_ref to load the readers count atomically
   // // Interpret the next sizeof(int) bytes of the data buffer as state
   // int& state_ref = *reinterpret_cast<int*>(new_frame->get_data_with_locks() + sizeof(int));
   // std::atomic_ref<int> state_atomic(state_ref);
   // new_frame->custom_latch.state.store(static_cast<CustomReadWriteLock::State>(state_atomic.load()));
   return new_frame;
}

void BufferManager::unfix_page(std::shared_ptr<BufferFrame> page, const bool is_dirty) {
   std::unique_lock bf_lock(buffer_manager_mutex);
   page->is_dirty = page->is_dirty || is_dirty;
   page->num_fixed -= 1;
   num_fixed_pages.fetch_sub(1ul);

   bf_lock.unlock();
   if (page->is_exclusive) {
      page->custom_latch.unlock_exclusive();
   } else {
      page->custom_latch.unlock_shared();
   }
}

std::vector<uint64_t> BufferManager::get_fifo_list() const {
   std::unique_lock lock(buffer_manager_mutex); // Ensure thread safety
   // Convert fifo_list (std::list) to std::vector
   return std::vector<uint64_t>(fifo_list.begin(), fifo_list.end());
}

std::vector<uint64_t> BufferManager::get_lru_list() const {
   std::unique_lock lock(buffer_manager_mutex); // Ensure thread safety
   // Convert lru_list (std::list) to std::vector
   return std::vector<uint64_t>(lru_list.begin(), lru_list.end());
}

} // namespace moderndbs
