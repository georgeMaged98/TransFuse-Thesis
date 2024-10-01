#include "moderndbs/buffer_manager.h"
#include "moderndbs/file.h"

using namespace std;

namespace moderndbs {
////////////////////////////////////////// ADDED FUNCTIONS //////////////////////////////////////////

// check if buffer manager is full
bool BufferManager::is_buffer_manager_full() const {
   return (fifo_queue.size() + lru_queue.size()) == page_count;
}

std::shared_ptr<BufferFrame> BufferManager::find_buffer_frame_by_page_id(uint64_t page_id) {
   auto output_it = hashtable.find(page_id);
   if (output_it == hashtable.end()) {
      return nullptr;
   }

   return output_it->second;
}

void BufferManager::write_buffer_frame_to_file(uint64_t pageNo, char* data) const {
   auto segment_id = get_segment_id(pageNo);
   try {
      std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(), File::Mode::WRITE);
      size_t file_offset = get_segment_page_id(pageNo) * page_size;
      file->resize(file->size() + this->page_size);
      // File size is the offset of the new page
      file->write_block(data, file_offset, this->page_size);
      // Create an unordered map to save the offset of each page.
      // We know segment id from the function, so we only need to save the offset.
   } catch (const std::exception& e) {
      std::cout << "File Not Found!! \n";
   }
}

void BufferManager::read_buffer_frame_from_file(uint64_t page_id, BufferFrame& bf) const {
   auto segment_id = get_segment_id(page_id);
   try {
      std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(),
                                                   File::Mode::READ);
      size_t page_offset_in_file = get_segment_page_id(page_id) * this->page_size;

      file->read_block(page_offset_in_file, this->page_size, bf.get_data_with_locks());
   } catch (const std::exception& e) {
      std::cout << "File Not Found!! \n";
   }
}

void BufferManager::evict_page() {
   // check fifo queue -> if not empty, evict first element in fifo queue
   int64_t evicted_page_id = -1;
   vector<uint64_t>::iterator pos;

   for (auto it = this->fifo_queue.begin(); it != this->fifo_queue.end(); ++it) {
      auto bf_it = this->hashtable.find(*it);
      if (bf_it == hashtable.end()) {
         continue; // Skip if page is not found in hashtable
      }
      const auto bf = bf_it->second;
      if (bf->num_fixed == 0 && !bf->is_dirty) {
         evicted_page_id = *it;
         pos = it;
         break;
      }
      // Above condition is to prioritize non-dirty pages, however if all pages are dirty, this conditions chooses first one to evict and write to disk.
      if (bf->num_fixed == 0) {
         evicted_page_id = *it;
         pos = it;
         break;
      }
   }
   if (evicted_page_id != -1) {
      this->fifo_queue.erase(pos);
      this->fifo_set.erase(evicted_page_id);

   } else { // EVICT first element in LRU queue
      for (auto it = this->lru_queue.begin(); it != this->lru_queue.end(); ++it) {
         auto bf_it = this->hashtable.find(*it);
         if (bf_it == hashtable.end()) {
            continue; // Skip if page is not found in hashtable
         }

         const auto bf = bf_it->second;
         if (bf->num_fixed == 0 && !bf->is_dirty) {
            evicted_page_id = *it;
            pos = it;
            break;
         }
         // Above condition is to prioritize non-dirty pages, however if all pages are dirty, this conditions chooses first one to evict and write to disk.
         if (bf->num_fixed == 0) {
            evicted_page_id = *it;
            pos = it;
         }
      }

      // what if we don't have an unfixed one?  -> buffer_manager_full_error is thrown earlier.
      this->lru_queue.erase(pos);
      this->lru_set.erase(evicted_page_id);
   }

   //  Update Hashtable
   auto ito = this->hashtable.find(evicted_page_id);
   if (ito != this->hashtable.end()) {
      std::shared_ptr<BufferFrame> bf_to_be_deleted = ito->second;
      this->hashtable.erase(ito);
      // If page is dirty, write it to disk:
      if (bf_to_be_deleted->is_dirty) {
         const auto pageNo = bf_to_be_deleted->pageNo;
         const auto data = bf_to_be_deleted->get_data_with_locks();
         // Write the readers_count and state to the first bytes of data using atomic_ref
         int& readers_count_ref = *reinterpret_cast<int*>(data);
         std::atomic_ref<int> readers_count_atomic(readers_count_ref);
         readers_count_atomic.store(bf_to_be_deleted->custom_latch.readers_count.load());

         // Store state in the next bytes of the data buffer using atomic_ref
         int& state_ref = *reinterpret_cast<int*>(data + sizeof(int));
         std::atomic_ref<int> state_atomic(state_ref);
         state_atomic.store(static_cast<int>(bf_to_be_deleted->custom_latch.state.load()));
         cout << " STATEEE BEFORE WRITING " << static_cast<int>(bf_to_be_deleted->custom_latch.state.load()) << "  " << bf_to_be_deleted->pageNo << "\n";

         // Write the Buffer Frame to disk
         // buffer_manager_mutex.unlock();
         // We unlock directory (buffer_manager_mutex) here to avoid holding the lock during an expensive operation (Writing to disk)
         write_buffer_frame_to_file(pageNo, data);
         // bf_to_be_deleted->latch.unlock();
         // buffer_manager_mutex.lock();
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
   // Writes all dirty pages to disk.
   for (auto& i : this->hashtable) {
      // ADD IS_DIRTY check
      if (i.second->is_dirty) {
         const auto pageNo = i.second->pageNo;
         const auto data = i.second->get_data_with_locks();
         // Write the readers_count and state to the first bytes of data using atomic_ref
         int& readers_count_ref = *reinterpret_cast<int*>(data);
         std::atomic_ref<int> readers_count_atomic(readers_count_ref);
         readers_count_atomic.store(i.second->custom_latch.readers_count.load());

         // Store state in the next bytes of the data buffer using atomic_ref
         int& state_ref = *reinterpret_cast<int*>(data + sizeof(int));
         std::atomic_ref<int> state_atomic(state_ref);
         state_atomic.store(static_cast<int>(i.second->custom_latch.state.load()));

         write_buffer_frame_to_file(i.second->pageNo, i.second->get_data_with_locks());
      }
   }
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
   if (fifo_set.contains(page_id)) {
      auto posToBeDeleted = find(fifo_queue.begin(), fifo_queue.end(), page_id);
      fifo_queue.erase(posToBeDeleted);
      fifo_set.erase(page_id);

      lru_queue.push_back(page_id);
      lru_set.insert(page_id);
   } else {
      // ONLY add it to FIFO queue if it is new.
      if (lru_set.contains(page_id)) { // If it is already in LRU queue, move it to the back of the queue.
         auto posToBeDeleted = find(lru_queue.begin(), lru_queue.end(), page_id);
         lru_queue.erase(posToBeDeleted);
         lru_queue.push_back(page_id);
      } else {
         fifo_queue.push_back(page_id);
         fifo_set.insert(page_id);
      }
   }

   std::shared_ptr<BufferFrame> buffer_frame = this->find_buffer_frame_by_page_id(page_id);
   if (buffer_frame != nullptr) {
      buffer_frame->num_fixed += 1;
      buffer_frame->num_fixed_exc += (exclusive ? 1 : 0);
      bf_lock.unlock();
      if(exclusive) {
         std::cout << "Trying lock exc: \n";
         // buffer_frame->custom_latch.lock_exclusive();
         // if (buffer_frame->custom_latch.state.load() == CustomReadWriteLock::State::LOCKED_EXCLUSIVE) {
         //    std::cout << "Page already locked exclusively. BF id: " << page_id << "\n";
         // } else {
            buffer_frame->custom_latch.lock_exclusive();
         buffer_frame->is_exclusive = true;
         // }
      }else {
         std::cout << "Trying lock shared: \n";
         // if(buffer_frame->custom_latch.readers_count.load() > 0) {
         //    std::cout << "\n";
         // }
         buffer_frame->custom_latch.lock_shared();
         buffer_frame->is_exclusive = false;
      }
      // if (exclusive) {
      //    buffer_frame->latch.lock();
      // } else {
      //    buffer_frame->latch.lock_shared();
      // }
      return buffer_frame;
   }

   // Page with page_id not found
   std::shared_ptr<BufferFrame> new_frame = std::make_shared<BufferFrame>();
   // Update Hashtable
   hashtable.insert({page_id, new_frame});
   // auto frame = hashtable[page_id].get();
   new_frame->pageNo = page_id;
   new_frame->is_dirty = false;
   new_frame->data = std::make_unique<char[]>(this->page_size);
   // new_frame->is_exclusive = exclusive;
   new_frame->num_fixed = 1;
   new_frame->num_fixed_exc += (exclusive ? 1 : 0);
   // load page from disk if it is not in memory(NOT in Hashtable). Use segment id to read from disk & update data field in the BufferFrame
   read_buffer_frame_from_file(page_id, *new_frame);

   // Initialize Locks from file.
   int& readers_count_ref = *reinterpret_cast<int*>(new_frame->get_data_with_locks());
   const std::atomic_ref<int> readers_count_atomic(readers_count_ref);
   new_frame->custom_latch.readers_count.store(readers_count_atomic.load());  // Use atomic_ref to load the readers count atomically
   // Interpret the next sizeof(int) bytes of the data buffer as state
   int& state_ref = *reinterpret_cast<int*>(new_frame->get_data_with_locks() + sizeof(int));
   std::atomic_ref<int> state_atomic(state_ref);
   new_frame->custom_latch.state.store(static_cast<CustomReadWriteLock::State>(state_atomic.load()));

   bf_lock.unlock();
   // if (new_frame->custom_latch.state.load() == CustomReadWriteLock::State::LOCKED_EXCLUSIVE) {
   //    std::cout << "Page already locked exclusively. BF id: " << page_id << "\n";
   // } else {
   //    new_frame->custom_latch.lock_exclusive();
   // }

   if (exclusive) {
      // new_frame->custom_latch.lock_exclusive();
      // if (new_frame->custom_latch.state.load() == CustomReadWriteLock::State::LOCKED_EXCLUSIVE) {
      //    std::cout << "Page already locked exclusively. BF id: " << page_id << "\n";
      // } else {
      new_frame->custom_latch.lock_exclusive();
      new_frame->is_exclusive = true;
      // }
   } else {
      new_frame->custom_latch.lock_shared();
      new_frame->is_exclusive = false;
   }
   // if (new_frame->is_exclusive) {
   //    new_frame->latch.lock();
   // } else {
   //    new_frame->latch.lock_shared();
   // }

   return new_frame;
}

void BufferManager::unfix_page(std::shared_ptr<BufferFrame> page, const bool is_dirty) {
   std::unique_lock bf_lock(buffer_manager_mutex);
   page->is_dirty = is_dirty;
   page->num_fixed -= 1;
   page->num_fixed_exc -= (page->is_exclusive ? 1 : 0);
   num_fixed_pages.fetch_sub(1ul);

   bf_lock.unlock();

   if (page->is_exclusive) {
      page->custom_latch.unlock_exclusive();
      // page->is_exclusive = false;
   } else {
      page->custom_latch.unlock_shared();
   }

   // if (page->is_exclusive) {
   //    page->latch.unlock();
   // } else {
   //    page->latch.unlock_shared();
   // }
}

std::vector<uint64_t> BufferManager::get_fifo_list() const {
   std::unique_lock lock(buffer_manager_mutex);
   return fifo_queue;
}

std::vector<uint64_t> BufferManager::get_lru_list() const {
   std::unique_lock lock(buffer_manager_mutex);
   return lru_queue;
}

} // namespace moderndbs
