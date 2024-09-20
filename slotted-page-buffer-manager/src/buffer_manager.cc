#include "moderndbs/buffer_manager.h"
#include "moderndbs/file.h"

using namespace std;

namespace moderndbs {
////////////////////////////////////////// ADDED FUNCTIONS //////////////////////////////////////////

// check if buffer manager is full
bool BufferManager::is_buffer_manager_full() const {
   return (fifo_queue.size() + lru_queue.size()) == page_count;
}

BufferFrame* BufferManager::find_buffer_frame_by_page_id(uint64_t page_id) {
   auto output_it = hashtable.find(page_id);
   if (output_it == hashtable.end()) {
      return nullptr;
   }

   return output_it->second.get();
}

void BufferManager::write_buffer_frame_to_file(BufferFrame* page) const {
   const auto segment_id = get_segment_id(page->pageNo);
   const size_t file_offset = get_segment_page_id(page->pageNo) * page_size;
   auto data = page->get_data();
   // buffer_manager_mutex.unlock();
   try {
      std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(), File::Mode::WRITE);
      auto size = file->size() + this->page_size;
      file->resize(size);
      // std::string bytes_hex_str = hex_dump_str(reinterpret_cast<std::byte*>(page->get_data()), this->page_size);
      // File size is the offset of the new page
      file->write_block(data, file_offset, this->page_size);
      // Create an unordered map to save the offset of each page.
      // We know segment id from the function, so we only need to save the offset.
   } catch (const std::exception& e) {
      std::cout << "File Not Found!! \n";
   }
   // buffer_manager_mutex.lock();
}

void BufferManager::read_buffer_frame_from_file(uint64_t page_id, BufferFrame& bf) const {
   auto segment_id = get_segment_id(page_id);
   try {
      std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(), File::Mode::READ);
      size_t page_offset_in_file = get_segment_page_id(page_id) * this->page_size;
      file->read_block(page_offset_in_file, this->page_size, bf.get_data());
   } catch (const std::exception& e) {
      std::cout << "File Not Found!! \n";
   }
}

void BufferManager::evict_page() {
   // check fifo queue -> if not empty, evict first element in fifo queue
   size_t evicted_page_id = -1;
   vector<uint64_t>::iterator it, pos;
   for (it = this->fifo_queue.begin(); it != this->fifo_queue.end(); ++it) {
      const auto bf = this->hashtable[*it].get();
      if (bf != nullptr && !bf->is_fixed && !bf->is_dirty) {
         evicted_page_id = *it;
         pos = it;
         break;
      }
      if (bf != nullptr && !bf->is_fixed) {
         evicted_page_id = *it;
         pos = it;
      }
   }
   if (evicted_page_id != -1) {
      this->fifo_queue.erase(pos);
      this->fifo_set.erase(evicted_page_id);

   } else { // EVICT first element in LRU queue
      for (it = this->lru_queue.begin(); it != this->lru_queue.end(); ++it) {
         const auto bf = this->hashtable[*it].get();
         if (!bf->is_fixed && !bf->is_dirty) {
            evicted_page_id = *it;
            pos = it;
            break;
         }
         if (!bf->is_fixed) {
            evicted_page_id = *it;
            pos = it;
         }
      }

      // what if we don't have an unfixed one?????/
      this->lru_queue.erase(pos);
      this->lru_set.erase(evicted_page_id);
   }

   //  Update Hashtable
   auto ito = this->hashtable.find(evicted_page_id);
   if (ito != this->hashtable.end()) {
      auto bf_to_be_deleted = ito->second.get();
      // If page is dirty, write it to disk:
      if (bf_to_be_deleted->is_dirty) {
         // buffer_manager_mutex.unlock();
         // Write the Buffer Frame to disk
         write_buffer_frame_to_file(bf_to_be_deleted);
         // buffer_manager_mutex.lock();
      }

      this->hashtable.erase(ito);
   }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

char* BufferFrame::get_data() {
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
         write_buffer_frame_to_file(i.second.get());
      }
   }
}

BufferFrame& BufferManager::fix_page(uint64_t page_id, bool exclusive) {
   // lock mutex
   buffer_manager_mutex.lock();
   if (this->num_fixed_pages == this->page_count) {

      buffer_manager_mutex.unlock();
      throw buffer_full_error{};
   }

   // Check if buffer manager is full
   if (this->is_buffer_manager_full()) {
      // EVICT PAGE TO DISK
      evict_page();
   }

   num_fixed_pages += 1;

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

   BufferFrame* buffer_frame = this->find_buffer_frame_by_page_id(page_id);
   //buffer_manager_mutex.unlock();

   if (buffer_frame != nullptr) {
      buffer_frame->is_exclusive = exclusive;
      buffer_manager_mutex.unlock();

      if (exclusive) {
         buffer_frame->latch.lock();
      } else {
         buffer_frame->latch.lock_shared();
      }
      return *buffer_frame;
   }

   // Page with page_id not found
   std::unique_ptr<BufferFrame> new_frame(new BufferFrame());
   new_frame->pageNo = page_id;
   new_frame->is_dirty = false;
   new_frame->data = std::make_unique<char[]>(this->page_size);
   new_frame->is_exclusive = exclusive;
   new_frame->is_fixed = true;
   // load page from disk if it is not in memory(NOT in Hashtable). Use segment id to read from disk & update data field in the BufferFrame
   read_buffer_frame_from_file(page_id, *new_frame);

   //buffer_manager_mutex.lock();
   // Update Hashtable
   hashtable.insert({page_id, std::move(new_frame)});
   auto frame = hashtable[page_id].get();

   buffer_manager_mutex.unlock();
   if (exclusive) {
      frame->latch.lock();
   } else {
      frame->latch.lock_shared();
   }
   return *frame;
}

void BufferManager::unfix_page(BufferFrame& page, bool is_dirty) {
   if (page.is_exclusive) {
      page.latch.unlock();
   } else {
      page.latch.unlock_shared();
   }
   page.is_dirty = is_dirty;
   page.is_fixed = false;
   std::unique_lock lock(buffer_manager_mutex);

   num_fixed_pages -= 1;
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
