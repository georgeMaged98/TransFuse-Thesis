// #include "moderndbs/database.h"
// #include "moderndbs/file.h"

#include <barrier>
#include <cassert>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <random>
#include <unordered_set>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <moderndbs/database.h>
#include <sys/mman.h>

#include "moderndbs/file.h"
#include "moderndbs/file_mapper.h"
#include "moderndbs/segment.h"

namespace moderndbs {
std::unique_ptr<schema::Schema> getTPCHSchemaLight() {
   std::vector<schema::Table> tables{
      schema::Table(
         "customer",
         {
            schema::Column("c_custkey", schema::Type::Integer()),
            schema::Column("c_name", schema::Type::Char(25)),
            schema::Column("c_address", schema::Type::Char(40)),
            schema::Column("c_nationkey", schema::Type::Integer()),
            schema::Column("c_phone", schema::Type::Char(15)),
            schema::Column("c_acctbal", schema::Type::Integer()),
            schema::Column("c_mktsegment", schema::Type::Char(10)),
            schema::Column("c_comment", schema::Type::Char(117)),
         },
         {"c_custkey"},
         10, 11),
      schema::Table(
         "nation",
         {
            schema::Column("n_nationkey", schema::Type::Integer()),
            schema::Column("n_name", schema::Type::Char(25)),
            schema::Column("n_regionkey", schema::Type::Integer()),
            schema::Column("n_comment", schema::Type::Char(152)),
         },
         {"n_nationkey"},
         20, 21),
      schema::Table(
         "region",
         {
            schema::Column("r_regionkey", schema::Type::Integer()),
            schema::Column("r_name", schema::Type::Char(25)),
            schema::Column("r_comment", schema::Type::Char(152)),
         },
         {"r_regionkey"},
         30, 31),
   };
   auto schema = std::make_unique<schema::Schema>(std::move(tables));
   return schema;
}

std::unique_ptr<schema::Schema> getTPCHOrderSchema() {
   std::vector<schema::Table> tables{
      schema::Table(
         "order",
         {
            schema::Column("o_orderkey", schema::Type::Integer()),
            schema::Column("o_custkey", schema::Type::Integer()),
            schema::Column("o_orderstatus", schema::Type::Char(1)),
            schema::Column("o_totalprice", schema::Type::Integer()),
            schema::Column("o_shippriority", schema::Type::Integer()),
         },
         {"o_orderkey"},
         50, 51,
         0)};
   auto schema = std::make_unique<schema::Schema>(std::move(tables));
   return schema;
}

}

template <typename T>
void readLine(T& v);

template <>
void readLine(std::string& v) {
   std::getline(std::cin, v);
}

template <>
void readLine(int& v) {
   std::string line;
   std::getline(std::cin, line);
   try {
      v = std::stoi(line);
   } catch (...) {}
}


// OLD listenForErrors
// void listenForErrors() {
//    int server_sock, client_sock;
//    struct sockaddr_un addr;
//    char buffer[128];
//
//    server_sock = socket(AF_UNIX, SOCK_STREAM, 0);
//    addr.sun_family = AF_UNIX;
//    strcpy(addr.sun_path, "/tmp/db_socket");
//    bind(server_sock, (struct sockaddr*)&addr, sizeof(addr));
//    listen(server_sock, 5);
//
//    // client_sock = accept(server_sock, nullptr, nullptr);
//    // read(client_sock, buffer, sizeof(buffer));
//    // std::cerr << "Error from libfuse: " << buffer << std::endl;
//    //
//    // close(client_sock);
//    while (true) {  // Keep listening indefinitely
//       // Accept a connection from a client
//       client_sock = accept(server_sock, nullptr, nullptr);
//       if (client_sock < 0) {
//          std::cerr << "Failed to accept connection: " << strerror(errno) << std::endl;
//          continue;  // Skip to the next iteration if an error occurs
//       }
//
//       // Read error message from the client
//       ssize_t bytes_read = read(client_sock, buffer, sizeof(buffer) - 1);
//       if (bytes_read > 0) {
//          buffer[bytes_read] = '\0';  // Null-terminate the string
//          std::cerr << "Error from libfuse: " << buffer << std::endl;
//       } else {
//          std::cerr << "Failed to read error message: " << strerror(errno) << std::endl;
//       }
//
//       // Close the client socket after reading
//       close(client_sock);
//    }
//
//    close(server_sock);
// }

std::condition_variable cv;
std::mutex cv_m;
bool is_server_ready = false;

// unordered_set<uint64_t> fsi_pages;
// unordered_set<uint64_t> schema_pages;
// unordered_set<uint64_t> sp_pages;

/// Shutdown Requested for listeningForErrors function.

std::atomic<int> threads_remaining(0);
std::mutex m_shutdown;
std::atomic shutdown_requested{false};

void removeExistingSocket(const char* socket_path) {
   if (access(socket_path, F_OK) == 0) {
      if (unlink(socket_path) == -1) {
         perror("Failed to remove existing socket file");
         exit(EXIT_FAILURE);
      }
   }
}


int createAndBindSocket(const char* socket_path, struct sockaddr_un& addr) {
    int server_sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (server_sock < 0) {
        std::cerr << "Failed to create socket: " << strerror(errno) << "\n";
        exit(EXIT_FAILURE);
    }

    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socket_path, sizeof(addr.sun_path) - 1);

    if (bind(server_sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Failed to bind socket: " << strerror(errno) << "\n";
        close(server_sock);
        exit(EXIT_FAILURE);
    }

    return server_sock;
}

void setSocketNonBlocking(int server_sock) {
    int flags = fcntl(server_sock, F_GETFL, 0);
    if (flags == -1 || fcntl(server_sock, F_SETFL, flags | O_NONBLOCK) == -1) {
        std::cerr << "Failed to set socket to non-blocking: " << strerror(errno) << "\n";
        close(server_sock);
        exit(EXIT_FAILURE);
    }
}

void handleClientConnection(int client_sock, moderndbs::Database& db, size_t& invalid_message_count) {
    char buffer[128];
    ssize_t bytes_read = read(client_sock, buffer, sizeof(buffer) - 1);

    if (bytes_read < 0) {
        std::cerr << "Failed to read error message: " << strerror(errno) << "\n";
    } else if (bytes_read == 0) {
        std::cerr << "Client disconnected unexpectedly.\n";
    } else {
        buffer[bytes_read] = '\0'; // Null-terminate the string
        std::string error_message(buffer);

        size_t first_hyphen = error_message.find('-');
        size_t second_hyphen = error_message.find('-', first_hyphen + 1);

        if (first_hyphen != std::string::npos && second_hyphen != std::string::npos) {
            std::string segment_name = error_message.substr(0, first_hyphen);
            uint64_t page_offset_in_file = std::stoull(error_message.substr(first_hyphen + 1, second_hyphen - first_hyphen - 1));
            uint64_t write_size = std::stoull(error_message.substr(second_hyphen + 1));

            // std::cout << "First part: " << segment_name << '\n';
            // std::cout << "Page offset: " << page_offset_in_file << '\n';
            // std::cout << "Write size: " << write_size << '\n';

            // Perform specific actions based on the extracted data
            moderndbs::FileMapper* file_mapper = nullptr;

            if (segment_name == "/sp_segment.txt") {
                file_mapper = &db.sp_file_mapper;
            } else if (segment_name == "/fsi_segment.txt") {
                file_mapper = &db.fsi_file_mapper;
            } else if (segment_name == "/schema_segment.txt") {
                file_mapper = &db.schema_file_mapper;
            } else {
                std::cerr << "Unknown segment: " << segment_name << "\n";
                return;
            }
            invalid_message_count++;
            // Check if the memory region is resident
            int resident_status = file_mapper->is_memory_resident(page_offset_in_file);
            if (resident_status == 1) {
                std::cout << "Page is resident in memory.\n";
            } else if (resident_status == 0) {
                std::cout << "Page is not resident in memory.\n";
            } else {
                std::cerr << "Error checking memory residency.\n";
            }
            std::cout << "About to call make_no_op with offset: " << page_offset_in_file << "\n";
            if(resident_status == 1) {
                file_mapper->make_no_op(page_offset_in_file);
            }
        } else {
            std::cerr << "Invalid error message format: " << error_message << "\n";
        }
    }

    close(client_sock);
}


void listenForErrors(moderndbs::Database& db) {
   struct sockaddr_un addr;
   auto socket_path = "/tmp/db_socket";

   removeExistingSocket(socket_path);


   int server_sock = createAndBindSocket(socket_path, addr);

   if (listen(server_sock, 5) < 0) {
      std::cerr << "Failed to listen on socket: " << strerror(errno) << "\n";
      close(server_sock);
      exit(EXIT_FAILURE);
   }

   setSocketNonBlocking(server_sock);
   std::cout << "Socket bound to " << socket_path << ".\n";

    // Signal that the server is ready
    {
       std::lock_guard lk(cv_m);
       is_server_ready = true;
    }
    cv.notify_all();
    size_t invalid_message_count = 0;

   while (!shutdown_requested.load()) {
       int client_sock = accept(server_sock, nullptr, nullptr);
      if (client_sock < 0) {
         if (errno == EAGAIN || errno == EWOULDBLOCK) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
         }
         std::cerr << "Failed to accept connection: " << strerror(errno) << "\n";
         continue;
      }

      std::cout << "Connection accepted.\n";
      handleClientConnection(client_sock, db, invalid_message_count);
   }

    printf("Error Count: %lu \n", invalid_message_count);

    // Cleanup: close the server socket
    close(server_sock);

    // Optionally, remove the socket file
    if (unlink(socket_path) == -1) {
        std::cerr << "Failed to remove socket file: " << strerror(errno) << "\n";
    }
    std::cout << "Server shutdown complete.\n";

}


/// DEFAULT MAIN
int main() {
    using moderndbs::File;
    for (const auto *segment_file: std::vector{
             "/tmp/transfuse_mnt/fsi_segment.txt", "/tmp/transfuse_mnt/sp_segment.txt",
             "/tmp/transfuse_mnt/schema_segment.txt", "/tmp/transfuse_mnt/wal_segment.txt"
         }) {
        auto file = File::open_file(segment_file, File::Mode::WRITE);
        file->resize(0);
    }

    // Create the errorListener thread before joining the other threads
    auto db = moderndbs::Database();

    std::thread errorListener([&db]() { listenForErrors(db); });

    {
        std::unique_lock<std::mutex> lk(cv_m);
        cv.wait(lk, [] { return is_server_ready; });
    }

    std::cout << "Error listener is ready, now starting worker threads...\n";

    {
        moderndbs::FileMapper schema_file_mapper("/tmp/transfuse_mnt/schema_segment.txt", (sysconf(_SC_PAGESIZE)));
        moderndbs::SchemaSegment schema_segment(49, schema_file_mapper);
        schema_segment.set_schema(moderndbs::getTPCHOrderSchema());
        schema_segment.write();
    }

    db.load_schema(49);
    auto &table = db.get_schema().tables[0];

    /// INSERTIONS -> INSERT OrderRecords for 20 pages.
    for (uint64_t i = 0; i < 409600; ++i) {
        moderndbs::OrderRecord order = {i, i * 2, i * 100, i % 5, (i % 2 == 0 ? 'G' : 'H')};
        auto transactionID = db.transaction_manager.startTransaction();
        db.insert(table, order, transactionID);
        db.transaction_manager.commitTransaction(transactionID);
    }

    constexpr int num_threads = 4;
    threads_remaining.store(num_threads);
    std::vector<std::thread> workers;
    auto start = std::chrono::high_resolution_clock::now();

    for (size_t thread = 0; thread < num_threads; ++thread) {
        workers.emplace_back([thread, &table, &db] {
            std::mt19937_64 engine{thread};
            // 5% of queries are scans.
            std::bernoulli_distribution scan_distr{0};
            // Number of pages accessed by a point query is geometrically distributed.
            std::geometric_distribution<size_t> num_pages_distr{0.5};
            // 60% of point queries are reads.
            std::bernoulli_distribution reads_distr{0.5};
            std::cout << " READS: " << 0.5 << "\n";

            // Pages and Slots
            // Out of 20 accesses, 10 are from page 0, 4 from page 1, 2 from page 2, 1 from page 3, and 3 from page 4.
            std::uniform_int_distribution<uint16_t> page_distr{0, 19};
            std::uniform_int_distribution<uint16_t> slot_distr{0, 79};

            for (size_t j = 0; j < 800; ++j) {
                // if (scan_distr(engine)) {
                //     /// READ two full segments
                //     auto start_page = page_distr(engine);
                //     auto end_page = start_page + 2;
                //     for (uint16_t pg = start_page; pg < end_page && pg < 20; ++pg) {
                //         for (uint16_t sl = 0; sl < 79; ++sl) {
                //             std::cout << " Page: " << pg << " SLOT: " << sl << " \n";
                //             moderndbs::TID tid{pg, sl};
                //             std::cout << " Reading Tuple with TID: " << tid.get_value() << " \n";
                //             auto transactionID = db.transaction_manager.startTransaction();
                //             db.read_tuple(table, tid, transactionID);
                //             db.transaction_manager.commitTransaction(transactionID);
                //         }
                //     }
                // } else {
                    if (reads_distr(engine)) {
                        auto page = page_distr(engine);
                        auto slot = slot_distr(engine);
                        // std::cout << " Page: " << page << " SLOT: " << slot << " \n";
                        moderndbs::TID tid{page, slot};
                        // std::cout << " Reading Tuple with TID: " << tid.get_value() << " \n";
                        auto transactionID = db.transaction_manager.startTransaction();
                        db.read_tuple(table, tid, transactionID);
                        db.transaction_manager.commitTransaction(transactionID);
                    } else {
                        moderndbs::OrderRecord order = {j, j * 2, j * 100, j % 5, (j % 2 == 0 ? 'G' : 'H')};
                        auto transactionID = db.transaction_manager.startTransaction();
                        db.insert(table, order, transactionID);
                        db.transaction_manager.commitTransaction(transactionID);
                    }
                }
            // }

            if (--threads_remaining == 0) {
                std::lock_guard<std::mutex> lk(m_shutdown);
                /// WAIT FOR ANY new Messages yet to arrive from libfuse.
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                shutdown_requested.store(true);
            }
        });
    }

    for (auto &t: workers) {
        t.join();
    }

    std::cout << "FINITO\n";
    errorListener.join();
    std::cout << "Error listener terminated. Exiting...\n";

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Time taken: " << duration.count() << " milliseconds" << std::endl;

    return 0;
}


// TESTING MEMORY PRESSURE
// int main() {
//     // std::cout << "Setting memory limit...\n";
//     // struct rlimit limit;
//     // limit.rlim_cur = 100 * 1024 * 1024; // Set current limit to MAX
//     // limit.rlim_max = 100 * 1024 * 1024; // Set maximum limit to MAX
//     //
//     // if (setrlimit(RLIMIT_AS, &limit) == -1) {
//     //    std::cerr << "Failed to set memory limit: " << strerror(errno) << '\n';
//     //    return 1;
//     // }
//     // std::cout << "Memory limit set successfully.\n";
//     //
//     // struct rlimit new_limit;
//     // if (getrlimit(RLIMIT_AS, &new_limit) == 0) {
//     //    std::cout << "Current limit: " << new_limit.rlim_cur << " MB\n";
//     // } else {
//     //     std::cerr << "Failed to get memory limit: " << strerror(errno) << std::endl;
//     // }
//
//     int fd = open("myfile1.txt", O_RDWR | O_CREAT);
//     std::cout << "myfile1.txt\n";
//     if (fd < 0) {
//         perror("open");
//         return 1;
//     }
//
//     size_t file_size = 300 * 1024 * 1024;
//     // std::cout << "FILE SIZE: " << file_size << '\n';
//     // if(ftruncate(fd, file_size) == -1 ) {
//     //     perror("ftruncate");
//     //     return 1;
//     // }
//
//     char* memory = static_cast<char *>(mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
//     if (memory == MAP_FAILED) {
//         std::cout<<"ERROR \n";
//         perror("mmap");
//         close(fd);
//         return 1;
//     }
//
//     //
//     //
//     //
//     // limit.rlim_cur = 10 * 1024; // Set current limit to 256 MB
//     // limit.rlim_max = 10 * 1024; // Set maximum limit to 256 MB
//     //
//     // if (setrlimit(RLIMIT_AS, &limit) == -1) {
//     //     std::cerr << "Failed to set memory limit: " << strerror(errno) << '\n';
//     //     return 1;
//     // }
//     // std::cout << "Memory limit set successfully.\n";
//     //
//     // if (getrlimit(RLIMIT_AS, &new_limit) == 0) {
//     //     std::cout << "Current limit: " << new_limit.rlim_cur << " MB\n";
//     // } else {
//     //     std::cerr << "Failed to get memory limit: " << strerror(errno) << '\n';
//     // }
//     // Use the mapped memory...
//
//
//
//     size_t page_size = sysconf(_SC_PAGESIZE);
//     size_t num_pages = (file_size / page_size) - 1;
//     std::cout << num_pages << "\n";
//
//     // Sequential access to touch all pages
//     for (size_t i = 0; i < file_size; i += page_size) {
//         memory[i] = 'A';
//     }
//
//     // Random access to increase memory pressure
//     for (int j = 0; j < 3; ++j) {
//         std::srand(std::time(nullptr));
//         for (size_t i = 0; i < num_pages; ++i) {
//             size_t page_index = std::rand() % num_pages;
//             memory[page_index * page_size] = 'B';
//         }
//     }
//
//     std::cout << "All pages touched. Holding memory...\n";
//
//     // Keep the program running to maintain memory pressure
//     // sleep(10);
//
//
//     munmap(memory, file_size);
//     close(fd);
//     return 0;
// }


// Testing WAL commit, by waiting (in debug mode) until page is written to test invalid pages are not flushed.
// int main() {
//
//    auto db = moderndbs::Database();
//    std::thread errorListener([&db]() { listenForErrors(db); });
//
//    {
//       moderndbs::FileMapper schema_file_mapper("/tmp/transfuse_mnt/schema_segment.txt", (sysconf(_SC_PAGESIZE)));
//       moderndbs::SchemaSegment schema_segment(49, schema_file_mapper);
//       schema_segment.set_schema(moderndbs::getTPCHOrderSchema());
//       schema_segment.write();
//    }
//    db.load_schema(49);
//    auto& table = db.get_schema().tables[0];
//    auto transactionID = db.transaction_manager.startTransaction();
//    moderndbs::OrderRecord order = {10,20,30,40,'D'};
//    db.insert(table, order, transactionID);
//    db.insert(table, order, transactionID);
//    db.insert(table, order, transactionID);
//    db.transaction_manager.commitTransaction(transactionID);
//
//    transactionID = db.transaction_manager.startTransaction();
//    order = {10,20,30,40,'D'};
//    db.insert(table, order, transactionID);
//    db.transaction_manager.commitTransaction(transactionID);
//
//    errorListener.join();
// }

// int main() {
// std::cout << "Setting memory limit..." << std::endl;
//
// struct rlimit limit;
// limit.rlim_cur = 512 * 1024 * 1024; // Set current limit to 256 MB
// limit.rlim_max = 512 * 1024 * 1024; // Set maximum limit to 256 MB
//
// if (setrlimit(RLIMIT_AS, &limit) == -1) {
//    std::cerr << "Failed to set memory limit: " << strerror(errno) << std::endl;
//    return 1; // Return a non-zero value to indicate failure
// }
//
// std::cout << "Memory limit set successfully." << std::endl;
//
// struct rlimit new_limit;
// if (getrlimit(RLIMIT_AS, &new_limit) == 0) {
//    std::cout << "Current limit: " << new_limit.rlim_cur << " MB\n";
// } else {
//    std::cerr << "Failed to get memory limit: " << strerror(errno) << std::endl;
// }


// Function to Restrict Memory -> NOT USED
// void restrict_memory(size_t DATA_memory_limit_bytes, size_t AS_memory_limit_bytes) {
//     // struct rlimit rl;
//     // rl.rlim_cur = DATA_memory_limit_bytes;
//     // rl.rlim_max = DATA_memory_limit_bytes;
//
//     ///RLIMIT_AS: Limits the total address space a process can use, including heap, stack, and memory-mapped regions.
//     ///RLIMIT_DATA:  Specifically limits the size of the data segment(heap)
//     // if (setrlimit(RLIMIT_DATA, &rl) == -1) {
//     //    perror("setrlimit");
//     //    exit(1);
//     // }
//
//     // printf("Memory limit set to %zu bytes\n", DATA_memory_limit_bytes);
//
//     struct rlimit rl2;
//     rl2.rlim_cur = AS_memory_limit_bytes;
//     rl2.rlim_max = RLIM_INFINITY;
//
//     if (setrlimit(RLIMIT_AS, &rl2) == -1) {
//         perror("setrlimit");
//         exit(1);
//     }
//     printf("AS Memory limit set to %zu bytes\n", AS_memory_limit_bytes);
//
//
//     struct rlimit stack_limit;
//     stack_limit.rlim_cur = 100 * 1024 * 1024; // 100 MB
//     stack_limit.rlim_max = 100 * 1024 * 1024; // Hard limit
//     if (setrlimit(RLIMIT_STACK, &stack_limit) == -1) {
//         perror("setrlimit (stack)");
//     }
// }

//
// bool is_page_dirty(void *addr) {
//     // Page size (commonly 4 KB)
//     size_t page_size = sysconf(_SC_PAGE_SIZE);
//
//     // Open /proc/self/pagemap
//     std::ifstream pagemap("/proc/self/pagemap", std::ios::in | std::ios::binary);
//     if (!pagemap.is_open()) {
//         perror("Failed to open /proc/self/pagemap");
//         return false;
//     }
//
//     // Calculate the page index
//     uintptr_t page_index = (uintptr_t) addr / page_size;
//
//     // Seek to the page entry
//     pagemap.seekg(page_index * sizeof(uint64_t), std::ios::beg);
//
//     // Read the page entry
//     uint64_t entry;
//     pagemap.read(reinterpret_cast<char *>(&entry), sizeof(uint64_t));
//
//     // Check the dirty flag (bit 55)
//     bool is_dirty = (entry & (1ULL << 55)) != 0;
//
//     pagemap.close();
//     return is_dirty;
// }

//
// bool is_page_dirty2(void *addr) {
//     uintptr_t virtual_address = reinterpret_cast<uintptr_t>(addr);
//     size_t page_size = getpagesize();
//
//     // Open /proc/self/pagemap
//     std::ifstream pagemap("/proc/self/pagemap", std::ios::binary);
//     if (!pagemap) {
//         perror("Failed to open /proc/self/pagemap");
//         return false;
//     }
//
//     // Calculate offset in pagemap file
//     size_t offset = (virtual_address / page_size) * sizeof(uint64_t);
//     pagemap.seekg(offset);
//
//     uint64_t entry;
//     pagemap.read(reinterpret_cast<char *>(&entry), sizeof(entry));
//     if (!pagemap) {
//         perror("Failed to read from /proc/self/pagemap");
//         return false;
//     }
//
//     // Extract the physical page number (PFN)
//     if (!(entry & (1ULL << 63))) {
//         // Check if the page is present
//         std::cerr << "Page is not present in memory" << std::endl;
//         return false;
//     }
//     uint64_t pfn = entry & ((1ULL << 55) - 1);
//
//     // Open /proc/kpageflags
//     std::ifstream kpageflags("/proc/kpageflags", std::ios::binary);
//     if (!kpageflags) {
//         perror("Failed to open /proc/kpageflags");
//         return false;
//     }
//
//     // Seek to the PFN entry in kpageflags
//     kpageflags.seekg(pfn * sizeof(uint64_t));
//     uint64_t flags;
//     kpageflags.read(reinterpret_cast<char *>(&flags), sizeof(flags));
//     if (!kpageflags) {
//         perror("Failed to read from /proc/kpageflags");
//         return false;
//     }
//
//     // Check the PageDirty flag (bit 4 in kpageflags)
//     return flags & (1ULL << 4); // PageDirty is bit 4
// }

// Testing that invalid writes don't reach disk with reading with O_DIRECT flag.
// int main() {
//
//    // restrict_memory(10 * 1024 * 1024, 1 * 1024  * 1024 * 1024);
//    // struct rlimit rl;
//    // getrlimit(RLIMIT_DATA, &rl);
//    // printf("RLIMIT_DATA: soft=%lu, hard=%lu\n", rl.rlim_cur, rl.rlim_max);
//    //
//    // struct rlimit rl2;
//    // getrlimit(RLIMIT_AS, &rl2);
//    // printf("RLIMIT_AS: soft=%lu, hard=%lu\n", rl2.rlim_cur, rl2.rlim_max);
//
//    const char* filename = "/tmp/transfuse_mnt/test_mmap_direct.txt";
//
//
//     // Perform a direct read to see what's on disk
//     char buffer[4096];
//     int direct_fd = open(filename, O_RDWR | O_CREAT | O_DIRECT, 0644);
//
//     if (direct_fd == -1) {
//         perror("open O_DIRECT");
//         // munmap(mmap_ptr, file_size);
//         close(direct_fd);
//     }
//
//    // Ensure the file is large enough
//    size_t file_size = 4096;
//    if (ftruncate(direct_fd, file_size) == -1) {
//       perror("ftruncate");
//       close(direct_fd);
//    }
//
//     // Initialize file with some data
//    char zero_buf[4096] = {0};
//    if (write(direct_fd, zero_buf, 4096) == -1) {
//       perror("write");
//       close(direct_fd);
//    }
//
//     // O_DIRECT requires aligned buffers
//     void* aligned_buf;
//     if (posix_memalign(&aligned_buf, 4096, 4096) != 0) {
//         perror("posix_memalign");
//         close(direct_fd);
//         // munmap(mmap_ptr, file_size);
//         close(direct_fd);
//     }
//
//    // Read the file directly from disk
//    lseek(direct_fd, 0, SEEK_SET);
//     ssize_t bytes_read = read(direct_fd, aligned_buf, 4096);
//     if (bytes_read == -1) {
//         perror("read O_DIRECT");
//        std::cerr << errno << '\n';
//     } else {
//        int* int_buf = static_cast<int*>(aligned_buf); // Interpret the buffer as int*
//        std::cout << "Read from disk with O_DIRECT: "
//                  << "First int = " << int_buf[0] << ", "
//                  << "Second int = " << int_buf[1] << '\n';
//     }
//
//    // Open the file for reading and writing
//    int mmap_fd = open(filename, O_RDWR | O_CREAT, 0644);
//    if (mmap_fd == -1) {
//        perror("open");
//    }
//
//    // Map the file into memory
//    char* mmap_ptr = static_cast<char*>(mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, mmap_fd, 0));
//    if (mmap_ptr == MAP_FAILED) {
//        perror("mmap");
//        close(mmap_fd);
//    }
//
//    // Read FROM the mmaped region
//    int* mmap_int_ptr = reinterpret_cast<int*>(mmap_ptr);
//    std::cout << "Read from MMAP: "
//                  << "First int = " << mmap_int_ptr[0] << ", "
//                  << "Second int = " << mmap_int_ptr[1] << '\n';
//    // Modify mmap region
//    int values_to_copy[2] = {4, 2};
//    memcpy(mmap_int_ptr, values_to_copy, sizeof(values_to_copy));
//
//    std::cout << "Read from MMAP AFTER MODIFICATION: "
//                  << "First int = " << mmap_int_ptr[0] << ", "
//                  << "Second int = " << mmap_int_ptr[1] << '\n';
//
//     // Perform another direct read
//     lseek(direct_fd, 0, SEEK_SET);
//     bytes_read = read(direct_fd, aligned_buf, 4096);
//     if (bytes_read == -1) {
//         perror("read O_DIRECT after flush");
//     } else {
//        int* int_buf = static_cast<int*>(aligned_buf); // Interpret the buffer as int*
//        std::cout << "Read from disk with O_DIRECT after mmap INCORRECT modification: "
//                  << "First int = " << int_buf[0] << ", "
//                  << "Second int = " << int_buf[1] << '\n';
//     }
//
//    values_to_copy[0] = 44;
//    values_to_copy[1] = 33;
//    memcpy(mmap_int_ptr, values_to_copy, sizeof(values_to_copy));
//
//
//    // Flush the mmaped region to disk
//    if (msync(mmap_ptr, file_size, MS_SYNC) == -1) {
//       perror("msync");
//       std::cerr << errno << '\n';
//    } else {
//       std::cout << "Flushed mmaped changes to disk.\n";
//    }
//
//    // Perform another direct read
//    lseek(direct_fd, 0, SEEK_SET);
//    bytes_read = read(direct_fd, aligned_buf, 4096);
//    if (bytes_read == -1) {
//       perror("read O_DIRECT after flush");
//    } else {
//       int* int_buf = static_cast<int*>(aligned_buf); // Interpret the buffer as int*
//       std::cout << "Read from disk with O_DIRECT after mmap CORRECT modification: "
//                 << "First int = " << int_buf[0] << ", "
//                 << "Second int = " << int_buf[1] << '\n';
//    }
//
//     // Clean up
//     free(aligned_buf);
//     // munmap(mmap_ptr, file_size);
//     close(direct_fd);
//     // close(fd);
//
//    return 0;
// }


// // Testing Correctness of IPC comm using mincore.
// int main() {
//     using moderndbs::File;
//     for (const auto *segment_file: std::vector{
//              "/tmp/transfuse_mnt/fsi_segment.txt", "/tmp/transfuse_mnt/sp_segment.txt",
//              "/tmp/transfuse_mnt/schema_segment.txt", "/tmp/transfuse_mnt/wal_segment.txt"
//          }) {
//         auto file = File::open_file(segment_file, File::Mode::WRITE);
//         file->resize(0);
//     }
//     auto file = File::open_file("/tmp/transfuse_mnt/sp_segment.txt", File::Mode::WRITE);
//     file->resize(0);
//     auto db = moderndbs::Database();
//
//     std::thread errorListener([&db]() { listenForErrors(db); });
//     {
//         std::unique_lock lk(cv_m);
//         cv.wait(lk, [] { return is_server_ready; });
//     }
//
//
//     std::vector<std::shared_ptr<moderndbs::Page>> vp;
//     vp.reserve(1500); // Reserve capacity upfront to avoid reallocations
//
//     // moderndbs::FileMapper test_mapper("/tmp/transfuse_mnt/sp_segment.txt", (sysconf(_SC_PAGESIZE)));
//     std::random_device rd;  // Seed for random number generator
//     std::mt19937 gen(rd()); // Mersenne Twister random number generator
//     std::uniform_int_distribution<> dis(1, 50); // Random number between 1 and 10
//
//     // Sequential access to touch all pages
//     for (size_t i = 0; i < 1500; ++i) {
//         auto p = db.sp_file_mapper.get_page(i, true);
//         // db.schema_file_mapper.release_page(p);
//         vp.push_back(p);
//         int wait_time = dis(gen);
//         std::this_thread::sleep_for(std::chrono::milliseconds(wait_time));
//
//         db.sp_file_mapper.release_page(p);
//     }
//
//     //std::this_thread::sleep_for(std::chrono::milliseconds(5000));
//
//     // Explicitly release all pages after usage
//     // for (const auto& p : vp) {
//     //     // db.sp_file_mapper.make_no_op(p->get_id() * 4096);
//     //     // char* data = p->get_data_with_header();
//     //     // int readers_count_ref = *reinterpret_cast<const int *>(data);
//     //     // int state = *reinterpret_cast<const int*>(data + sizeof(int));
//     //     // printf("First int: %d, Second int: %d\n", readers_count_ref, state);
//     //     // printf("INDEX: %lu\n", p->get_id() * (sysconf(_SC_PAGESIZE)));
//     //
//     //     db.sp_file_mapper.release_page(p);
//     // }
//
//     // std::cout << "Finitoooo ...\n";
//     // std::this_thread::sleep_for(std::chrono::milliseconds(500));
//     shutdown_requested.store(true);
//     errorListener.join();
//
//     std::cout << "Error listener terminated. Exiting...\n";
//     return 0;
// }
