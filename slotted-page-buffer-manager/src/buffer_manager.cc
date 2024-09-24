#include "moderndbs/buffer_manager.h"
#include "moderndbs/file.h"

using namespace std;

namespace moderndbs {
////////////////////////////////////////// ADDED FUNCTIONS //////////////////////////////////////////

// check if buffer manager is full
    bool BufferManager::is_buffer_manager_full() const {
        return (fifo_queue.size() + lru_queue.size()) == page_count;
    }

    BufferFrame *BufferManager::find_buffer_frame_by_page_id(uint64_t page_id) {
        auto output_it = hashtable.find(page_id);
        if (output_it == hashtable.end()) {
            return nullptr;
        }

        return output_it->second.get();
    }

    void BufferManager::write_buffer_frame_to_file(BufferFrame *page) const {
        auto segment_id = get_segment_id(page->pageNo);
        try {
            std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(),
                                                         File::Mode::WRITE);
            size_t file_offset = get_segment_page_id(page->pageNo) * page_size;
            file->resize(file->size() + this->page_size);

            // File size is the offset of the new page
            file->write_block(page->get_data(), file_offset, this->page_size);
            // Create an unordered map to save the offset of each page.
            // We know segment id from the function, so we only need to save the offset.
        } catch (const std::exception &e) {
            std::cout << "File Not Found!! " << std::endl;
        }
    }

    void BufferManager::read_buffer_frame_from_file(uint64_t page_id, BufferFrame &bf) const {
        auto segment_id = get_segment_id(page_id);
        try {
            std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(),
                                                         File::Mode::READ);
            size_t page_offset_in_file = get_segment_page_id(page_id) * this->page_size;

            file->read_block(page_offset_in_file, this->page_size, bf.get_data());
        } catch (const std::exception &e) {
            std::cout << "File Not Found!! " << std::endl;
        }
    }

    void BufferManager::evict_page() {
        // check fifo queue -> if not empty, evict first element in fifo queue
        size_t evicted_page_id = -1;
        vector<uint64_t>::iterator pos;
        for (auto it = this->fifo_queue.begin(); it != this->fifo_queue.end(); it++) {
            auto bf = this->hashtable[*it].get();
            if (bf->num_fixed == 0 && !bf->is_dirty) {
                evicted_page_id = *it;
                pos = it;
                break;
            }
            if (bf->num_fixed == 0) {
                evicted_page_id = *it;
                pos = it;
            }
        }
        if (evicted_page_id != -1) {
            this->fifo_queue.erase(pos);
            this->fifo_set.erase(evicted_page_id);

        } else { // EVICT first element in LRU queue
            for (auto it = this->lru_queue.begin(); it != this->lru_queue.end(); it++) {
                auto bf = this->hashtable[*it].get();
                if (bf->num_fixed == 0 && !bf->is_dirty) {
                    evicted_page_id = *it;
                    pos = it;
                    break;
                }
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
            auto bf_to_be_deleted = ito->second.get();
            // If page is dirty, write it to disk:
            if (bf_to_be_deleted->is_dirty) {
                // Write the Buffer Frame to disk
                buffer_manager_mutex.unlock();
                // Note: The BufferFrame is locked at this point and no shared resource is used inside write_buffer_frame_to_file() function.
                // We unlock directory (buffer_manager_mutex) here to avoid holding the lock during an expensive operation (Writing to disk)
                write_buffer_frame_to_file(bf_to_be_deleted);
                buffer_manager_mutex.lock();
            }
            this->hashtable.erase(ito);
        }
    }
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    char *BufferFrame::get_data() {
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
        for (auto &i: this->hashtable) {
            // ADD IS_DIRTY check
            if (i.second->is_dirty) {
                write_buffer_frame_to_file(i.second.get());
            }
        }
    }

    BufferFrame &BufferManager::fix_page(uint64_t page_id, bool exclusive) {
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

        BufferFrame *buffer_frame = this->find_buffer_frame_by_page_id(page_id);
        if (buffer_frame != nullptr) {
            buffer_frame->num_fixed += 1;
            bf_lock.unlock();
            if (exclusive) {
                buffer_frame->latch.lock();
            } else {
                buffer_frame->latch.lock_shared();
            }
            return *buffer_frame;
        }

        // Page with page_id not found
        std::shared_ptr<BufferFrame> new_frame = std::make_shared<BufferFrame>();
        // Update Hashtable
        hashtable.insert({page_id, new_frame});
        auto frame = hashtable[page_id].get();
        new_frame->pageNo = page_id;
        new_frame->is_dirty = false;
        new_frame->data = std::make_unique<char[]>(this->page_size);
        new_frame->is_exclusive = exclusive;
        new_frame->num_fixed = 1;
        bf_lock.unlock();
        if (exclusive) {
            frame->latch.lock();
        } else {
            frame->latch.lock_shared();
        }
        // load page from disk if it is not in memory(NOT in Hashtable). Use segment id to read from disk & update data field in the BufferFrame
        read_buffer_frame_from_file(page_id, *new_frame);
        return *frame;
    }

    void BufferManager::unfix_page(BufferFrame &page, const bool is_dirty) {
        std::unique_lock bf_lock(buffer_manager_mutex);
        num_fixed_pages.fetch_sub(1ul);
        page.is_dirty = is_dirty;
        page.num_fixed -= 1;
        if (page.is_exclusive) {
            page.latch.unlock();
        } else {
            page.latch.unlock_shared();
        }
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
