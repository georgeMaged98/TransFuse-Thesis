#include <string>
#include "mmap_manager.h"
#include "iostream"
#include "file_mapper.h"
#include <cassert>

using namespace dbms;
using namespace std;

void populateDB() {
    const uint64_t fileSize = 1 * 1024 * 1024; // 1MB
    const uint64_t pageSize = sysconf(_SC_PAGE_SIZE); // 4KB buffer

    /// A Tuple would be (8 bytes * 3 attributes) = 24 bytes
    const uint64_t tupleSize = 24; // 3 bytes
    const uint64_t headerSize = 8; // 2 uint_64 -> 8 bytes

    const uint64_t tuplesPerPage = (pageSize - headerSize) / tupleSize;

    const uint64_t numPages = fileSize / pageSize;

    std::cout << "Tuples Per Page: " << tuplesPerPage << std::endl;
    std::cout << "Number of Pages: " << numPages << std::endl;

    for (uint64_t i = 0; i < numPages; i++) {
        Page p(i);
        for (int j = 0; j < tuplesPerPage; ++j) {
            auto *t = new Tuple{j + (i * j), 1, 1};
            p.insert_tuple(t);
        }

        std::pair<char *, size_t> pagePair = p.serialize();
        char *filePath = "./database.txt";
        FileMapper::append_to_file(filePath, pagePair.first, pagePair.second);
    }
}

void setMemoryLimit() {

//    struct rlimit limit{};
//
//    // Set the soft and hard limits for the address space (memory) to 64 KB
//    long bytes_len = 64 * 1024 * 1024;
//    limit.rlim_cur = bytes_len; // Soft limit
//    limit.rlim_max = bytes_len; // Hard limit
//
//    if (setrlimit(RLIMIT_AS, &limit) != 0) {
//        std::cerr << "Error setting memory limit: " << strerror(errno) << std::endl;
//        return 1;
//    }
//    std::cout << " Memory limit set to 64KB" << std::endl;
//    // Get new limits
//    if (getrlimit(RLIMIT_AS, &limit) != 0) {
//        std::cerr << "Error getting new memory limit: " << strerror(errno) << std::endl;
//        return 1;
//    }
//
//    std::cout << "New memory limit: " << limit.rlim_cur << " (soft), " << limit.rlim_max << " (hard)" << std::endl;

//        populateDB();

}


void simulate_crash() {
    std::cout << "Simulating crash!" << std::endl;
    abort();  // Or use std::abort() or _exit(1) to simulate a crash
}

void performUpdateOperation(MmapManager *manager, std::atomic<bool> &running) {
    for (int i = 1; i <= 10 && running; ++i) {

        int tId = i * 100;
        Tuple *t = manager->read_tuple(tId);
        Page *p = manager->get_page_by_tuple_id(tId);
        cout << "Page LSN " << p->getHeader()->pageLSN << endl;
        cout << "Latest Flushed LSN " << manager->walManager->flushedLSN << endl;
        t->print();

        auto *nT = new Tuple(tId, 12, 13);
        manager->update_tuple(nT);
        std::this_thread::sleep_for(std::chrono::seconds(2)); // Simulating processing

    }
    simulate_crash();
    running = false;
}

int main() {
//    populateDB();
    std::atomic<bool> running(true);

    MmapManager manager;
    std::thread walThread(&WalManager::runEveryXSeconds, manager.walManager, 7, std::ref(running));

    std::thread normalOpThread(performUpdateOperation, &manager, std::ref(running));

    while (running) {
        std::this_thread::sleep_for(std::chrono::seconds(1)); // Simulating processing
    }

    walThread.join();
    normalOpThread.join();

    return 0;
}

