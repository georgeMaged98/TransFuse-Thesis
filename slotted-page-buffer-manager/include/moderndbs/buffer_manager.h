//#ifndef INCLUDE_MODERNDBS_BUFFER_MANAGER_H
//#define INCLUDE_MODERNDBS_BUFFER_MANAGER_H
//
//#include <algorithm>
//#include <atomic>
//#include <cstddef>
//#include <cstdint>
//#include <exception>
//#include <iostream>
//#include <memory>
//#include <mutex>
//#include <queue>
//#include <shared_mutex>
//#include <string>
//#include <unordered_map>
//#include <vector>
//#include <set>
//#include <moderndbs/hex_dump.h>
//
//namespace moderndbs {
//
//    class BufferFrame {
//    private:
//        friend class BufferManager;
//
//        // TODO: add your implementation here
//        uint64_t pageNo;
//        // latch
//        std::shared_mutex latch;
//        //  LSN
//        bool is_dirty;
//        bool is_exclusive;
//        uint64_t num_fixed;
//
//        std::unique_ptr<char[]> data;
//
//    public:
//        /// Returns a pointer to this page's data.
//        char *get_data();
//    };
//
//    class buffer_full_error
//            : public std::exception {
//    public:
//        [[nodiscard]] const char *what() const noexcept override {
//            return "buffer is full";
//        }
//    };
//
//    class BufferManager {
//    private:
//        // mutex to lock shared resources in buffer manager
//        mutable std::mutex buffer_manager_mutex;
//        // hashtable of buffer frames
//        std::unordered_map<uint64_t, std::unique_ptr<BufferFrame>> hashtable;
//        // FIFO queue -> Double-ended queue to erase elements because it is not possible to remove element form middle of queue
//        std::vector<uint64_t> fifo_queue;
//        std::set<uint64_t> fifo_set;
//
//        // LRU queue
//        std::vector<uint64_t> lru_queue;
//        std::set<uint64_t> lru_set;
//
//        // number of fixed pages
//        size_t num_fixed_pages;
//        // Maximum number of pages that should reside in memory at the same time.
//        size_t page_count;
//        // Size in bytes that all pages will have.
//        size_t page_size;
//
//    public:
//        BufferManager(const BufferManager &) = delete;
//
//        BufferManager(BufferManager &&) = delete;
//
//        BufferManager &operator=(const BufferManager &) = delete;
//
//        BufferManager &operator=(BufferManager &&) = delete;
//
//        /// Constructor.
//        /// @param[in] page_size  Size in bytes that all pages will have.
//        /// @param[in] page_count Maximum number of pages that should reside in
//        //                        memory at the same time.
//        BufferManager(size_t page_size, size_t page_count);
//
//        /// Destructor. Writes all dirty pages to disk.
//        ~BufferManager();
//
//        /// Returns size of a page
//        [[nodiscard]] size_t get_page_size() const { return page_size; }
//
//
//        /// Returns a reference to a `BufferFrame` object for a given page id. When
//        /// the page is not loaded into memory, it is read from disk. Otherwise the
//        /// loaded page is used.
//        /// When the page cannot be loaded because the buffer is full, throws the
//        /// exception `buffer_full_error`.
//        /// Is thread-safe w.r.t. other concurrent calls to `fix_page()` and
//        /// `unfix_page()`.
//        /// @param[in] page_id   Page id of the page that should be loaded.
//        /// @param[in] exclusive If `exclusive` is true, the page is locked
//        ///                      exclusively. Otherwise it is locked
//        ///                      non-exclusively (shared).
//        BufferFrame &fix_page(uint64_t page_id, bool exclusive);
//
//        /// Takes a `BufferFrame` reference that was returned by an earlier call to
//        /// `fix_page()` and unfixes it. When `is_dirty` is / true, the page is
//        /// written back to disk eventually.
//        void unfix_page(BufferFrame &page, bool is_dirty);
//
//        /// Takes a `BufferFrame` reference and writes it to disk
//        void write_buffer_frame_to_file(BufferFrame *page) const;
//
//        /// Read BufferFrame From disk
//        void read_buffer_frame_from_file(uint64_t page_id, BufferFrame &bf) const;
//
//        /// Returns true if the buffer manager is full of entries and we have to evict a page
//        /// False, otherwise
//        [[nodiscard]] bool is_buffer_manager_full() const;
//
//        /// Returns the BufferFrame that corresponds to the input page_id
//        /// Otherwise, it returns nullptr
//        [[nodiscard]] BufferFrame *find_buffer_frame_by_page_id(uint64_t page_id);
//
//        /// This function is called when BufferManager is full
//        /// to decide which page should be evicted to disk according to 2Q algorithm.
//        /// Returns the BufferFrame the should be written to disk
//        void evict_page();
//
//        /// Returns the page ids of all pages (fixed and unfixed) that are in the
//        /// FIFO list in FIFO order.
//        /// Is not thread-safe.
//        [[nodiscard]] std::vector<uint64_t> get_fifo_list() const;
//
//        /// Returns the page ids of all pages (fixed and unfixed) that are in the
//        /// LRU list in LRU order.
//        /// Is not thread-safe.
//        [[nodiscard]] std::vector<uint64_t> get_lru_list() const;
//
//        /// Returns the segment id for a given page id which is contained in the 16
//        /// most significant bits of the page id.
//        static constexpr uint16_t get_segment_id(uint64_t page_id) {
//            return page_id >> 48;
//        }
//
//        /// Returns the page id within its segment for a given page id. This
//        /// corresponds to the 48 least significant bits of the page id.
//        static constexpr uint64_t get_segment_page_id(uint64_t page_id) {
//            return page_id & ((1ull << 48) - 1);
//        }
//    };
//
//} // namespace moderndbs
//
//#endif

#ifndef INCLUDE_MODERNDBS_BUFFER_MANAGER_H
#define INCLUDE_MODERNDBS_BUFFER_MANAGER_H

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <set>

namespace moderndbs {

    class BufferFrame {
    private:
        friend class BufferManager;
        // TODO: add your implementation here
        uint64_t pageNo;
        // latch
        std::shared_mutex latch;
        //  LSN
        bool is_dirty;
        bool is_exclusive;
        int64_t num_fixed;

        std::unique_ptr<char[]> data;

    public:
        /// Returns a pointer to this page's data.
        char* get_data();
    };

    class buffer_full_error
            : public std::exception {
    public:
        [[nodiscard]] const char* what() const noexcept override {
            return "buffer is full";
        }
    };

    class BufferManager {
    private:
        // mutex to lock shared resources in buffer manager
        mutable std::mutex buffer_manager_mutex;
        // hashtable of buffer frames
        std::unordered_map<uint64_t, std::shared_ptr<BufferFrame>> hashtable;
        // FIFO queue -> Double-ended queue to erase elements because it is not possible to remove element form middle of queue
        std::vector<uint64_t> fifo_queue;
        std::set<uint64_t> fifo_set;

        // LRU queue
        std::vector<uint64_t> lru_queue;
        std::set<uint64_t> lru_set;

        // number of fixed pages
        std::atomic<size_t> num_fixed_pages;
        // Maximum number of pages that should reside in memory at the same time.
        size_t page_count;
        // Size in bytes that all pages will have.
        size_t page_size;

    public:
        BufferManager(const BufferManager&) = delete;
        BufferManager(BufferManager&&) = delete;
        BufferManager& operator=(const BufferManager&) = delete;
        BufferManager& operator=(BufferManager&&) = delete;
        /// Constructor.
        /// @param[in] page_size  Size in bytes that all pages will have.
        /// @param[in] page_count Maximum number of pages that should reside in
        //                        memory at the same time.
        BufferManager(size_t page_size, size_t page_count);

        /// Destructor. Writes all dirty pages to disk.
        ~BufferManager();

        /// Returns a reference to a `BufferFrame` object for a given page id. When
        /// the page is not loaded into memory, it is read from disk. Otherwise the
        /// loaded page is used.
        /// When the page cannot be loaded because the buffer is full, throws the
        /// exception `buffer_full_error`.
        /// Is thread-safe w.r.t. other concurrent calls to `fix_page()` and
        /// `unfix_page()`.
        /// @param[in] page_id   Page id of the page that should be loaded.
        /// @param[in] exclusive If `exclusive` is true, the page is locked
        ///                      exclusively. Otherwise it is locked
        ///                      non-exclusively (shared).
        BufferFrame& fix_page(uint64_t page_id, bool exclusive);

        /// Takes a `BufferFrame` reference that was returned by an earlier call to
        /// `fix_page()` and unfixes it. When `is_dirty` is / true, the page is
        /// written back to disk eventually.
        void unfix_page(BufferFrame& page, bool is_dirty);

        /// Takes a `BufferFrame` reference and writes it to disk
        void write_buffer_frame_to_file(BufferFrame* page) const;

        /// Read BufferFrame From disk
        void read_buffer_frame_from_file(uint64_t page_id, BufferFrame& bf) const;

        /// Returns size of a page
        [[nodiscard]] size_t get_page_size() const { return page_size; }

        /// Returns true if the buffer manager is full of entries and we have to evict a page
        /// False, otherwise
        [[nodiscard]] bool is_buffer_manager_full() const;

        /// Returns the BufferFrame that corresponds to the input page_id
        /// Otherwise, it returns nullptr
        [[nodiscard]] BufferFrame* find_buffer_frame_by_page_id(uint64_t page_id);

        /// This function is called when BufferManager is full
        /// to decide which page should be evicted to disk according to 2Q algorithm.
        /// Returns the BufferFrame the should be written to disk
        void evict_page();

        /// Returns the page ids of all pages (fixed and unfixed) that are in the
        /// FIFO list in FIFO order.
        /// Is not thread-safe.
        [[nodiscard]] std::vector<uint64_t> get_fifo_list() const;

        /// Returns the page ids of all pages (fixed and unfixed) that are in the
        /// LRU list in LRU order.
        /// Is not thread-safe.
        [[nodiscard]] std::vector<uint64_t> get_lru_list() const;

        /// Returns the segment id for a given page id which is contained in the 16
        /// most significant bits of the page id.
        static constexpr uint16_t get_segment_id(uint64_t page_id) {
            return page_id >> 48;
        }

        /// Returns the page id within its segment for a given page id. This
        /// corresponds to the 48 least significant bits of the page id.
        static constexpr uint64_t get_segment_page_id(uint64_t page_id) {
            return page_id & ((1ull << 48) - 1);
        }
    };

} // namespace moderndbs

#endif
