//
// Created by George Maged on 03.06.24.
//
#pragma once
#ifndef PROOF_OF_FAILURE_MMAP_MANAGER_H
#define PROOF_OF_FAILURE_MMAP_MANAGER_H

#include <cstdint>
#include "file_mapper.h"
#include <thread>
#include <chrono>
#include <atomic>
#include <shared_mutex>
#include "assert.h"

namespace dbms {

    class Tuple {
        /// A Tuple would be (8 bytes * 3 attributes) = 24 bytes
    public:
        Tuple() = default;

        Tuple(uint64_t tid, uint64_t a, uint64_t b) : _tid(tid), _a(a), _b(b) {};

        [[nodiscard]] uint64_t getTid() const { return _tid; }

        [[nodiscard]] uint64_t getA() const { return _a; }

        [[nodiscard]] uint64_t getB() const { return _b; }

        void setFromTuple(Tuple *t);

        void print() const;

    private:
        /// tid = page id + (offset or slot).
        uint64_t _tid;
        uint64_t _a;
        uint64_t _b;
    };

    enum TransactionState {
        BEGIN,
        RUNNING,
        COMMIT,
        ABORT
    };

    struct LogRecord {
    public:

        TransactionState state;
        uint64_t lsn;
        uint64_t transactionId;
        Tuple *oldTuple;
        Tuple *newTuple;

        LogRecord(uint64_t transactionId, TransactionState state, Tuple *oldTuple, Tuple *newTuple);

    };

    class Header {
    public:
        explicit Header(uint64_t pageLSN) : tuple_count(0), pageLSN(pageLSN) {};

        void print() const;

        /// Number of tuples.
        uint64_t tuple_count;
        /// The LSN of the latest update to the page
        uint64_t pageLSN;
    };

    class Page {
    private:
        std::vector<Tuple *> tuples;

        Header *header;
    public:
        explicit Page(uint64_t pageLSN) : header(new Header(pageLSN)) {};

        explicit Page(const char *data);

        Header *getHeader() { return header; }

        void insert_tuple(Tuple *t);

        [[nodiscard]] std::pair<char *, size_t> serialize() const;

        Tuple *getTupleFromPage(uint64_t tid);

        void print();

        void updateLSN(uint64_t lsn);
    };

    class WalManager {

    public:

        std::shared_mutex latch;

        // FlushedLSN is the LSN of the latest log record in the WAL on disk.
        // MMapManager class is responsible for fetching it from
        explicit WalManager(size_t pageSizeInBytes);

        char *flushedLSNFilePath = "./flushedLSN.txt";

        // Returns the LSN of this record
        uint64_t appendRecord(LogRecord *logRecord);

        void print();

        std::pair<char *, size_t> serialize();

        void flushWal();

        void runEveryXSeconds(uint64_t seconds, std::atomic<bool> &running) {
            while (running) {
                std::this_thread::sleep_for(std::chrono::seconds(seconds));
                if (running) {
                    flushWal();
                }
            }
        }

        uint64_t flushedLSN;

    private:
        std::vector<LogRecord *> records;
        uint64_t LSN;
        //Last LSN in log on disk
        FileMapper *fileMapper;

        uint64_t nextLSN();
    };

    class MmapManager {

    private:
        size_t pageSizeInBytes = sysconf(_SC_PAGE_SIZE);
        FileMapper *fileMapper;
        uint64_t transactionId;


    public:
        char *dbFilePath = "./database.txt";

        MmapManager();

        // Destructor
        ~MmapManager();

        Tuple *read_tuple(uint64_t tid);

        Page *get_page_by_tuple_id(uint64_t tid);

        Page *get_page(uint64_t pid);

        void update_tuple(Tuple *tuple);

        uint64_t get_tuple_size(){
            return sizeof(Tuple);
        }

        uint64_t get_num_tuples_per_page(){
            return (pageSizeInBytes - get_header_size() / get_tuple_size());
        }

        uint64_t get_header_size(){
            return sizeof(Header);
        }

        uint64_t get_num_pages(){
            return fileMapper->get_file_size(dbFilePath) / pageSizeInBytes;
        }

        WalManager *walManager;

    };
};


#endif //PROOF_OF_FAILURE_MMAP_MANAGER_H
