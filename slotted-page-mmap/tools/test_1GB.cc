//
// Created by george-elfayoumi on 11/25/24.
//


#include <iomanip>
#include <iostream>
#include <memory>
#include <ostream>
#include <moderndbs/database.h>
#include <random>
#include <sys/socket.h>
#include <sys/un.h>


std::condition_variable cv;
std::mutex cv_m;
bool is_server_ready = false;

/// Shutdown Requested for listeningForErrors function.

std::atomic<int> threads_remaining(0);
std::mutex m_shutdown;
std::atomic shutdown_requested{false};
std::atomic<bool> reset_requested{false};

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

void handleClientConnection(int client_sock, moderndbs::Database& db, size_t& wal_error_count, size_t& lock_error_count) {
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
        size_t third_hyphen = error_message.find('-', second_hyphen + 1);

        if (first_hyphen != std::string::npos && second_hyphen != std::string::npos) {
            std::string segment_name = error_message.substr(0, first_hyphen);
            uint64_t page_offset_in_file = std::stoull(error_message.substr(first_hyphen + 1, second_hyphen - first_hyphen - 1));
            uint64_t write_size = std::stoull(error_message.substr(second_hyphen + 1, third_hyphen - second_hyphen - 1));
           uint64_t error_type = std::stoull(error_message.substr(third_hyphen + 1));

            // std::cout << "ErrorType: " << error_type << '\n';
            wal_error_count += (error_type == 2); // INVALID_WAL error_type = 2
            lock_error_count += (error_type == 1); // INVALID_LOCK error_type = 1

            // Perform specific actions based on the extracted data
            moderndbs::FileMapper* file_mapper = nullptr;

            if (segment_name.find("_sp.txt") != std::string::npos) {
                file_mapper = &db.sp_file_mapper;
            } else if (segment_name.find("_fsi.txt") != std::string::npos) {
                file_mapper = &db.fsi_file_mapper;
            } else if (segment_name.find("_schema.txt") != std::string::npos) {
                file_mapper = &db.schema_file_mapper;
            } else {
                std::cerr << "Unknown segment: " << segment_name << "\n";
                return;
            }

            // Check if the memory region is resident
            int resident_status = file_mapper->is_memory_resident(page_offset_in_file);
            if (resident_status == 1) {
                // std::cout << "Page is resident in memory.\n";
            } else if (resident_status == 0) {
                std::cout << "Page is not resident in memory.\n";
            } else {
                // std::cerr << "Error checking memory residency.\n";
            }
            // std::cout << "About to call make_no_op with offset: " << page_offset_in_file << "\n";
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
    size_t wal_error_count = 0;
    size_t lock_error_count = 0;

   while (!shutdown_requested.load()) {

      // Check if reset signal is true
      if (reset_requested.load()) {
         // Print and reset error counts
         printf("WAL Error Count: %lu \n", wal_error_count);
         printf("LOCK Error Count: %lu \n", lock_error_count);

         wal_error_count = 0;
         lock_error_count = 0;

         reset_requested.store(false); // Reset the signal
      }

       int client_sock = accept(server_sock, nullptr, nullptr);
      if (client_sock < 0) {
         if (errno == EAGAIN || errno == EWOULDBLOCK) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
         }
         std::cerr << "Failed to accept connection: " << strerror(errno) << "\n";
         continue;
      }

      // std::cout << "Connection accepted.\n";
      handleClientConnection(client_sock, db, wal_error_count, lock_error_count);
   }
   std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    printf("WAL Error Count: %lu \n", wal_error_count);
    printf("LOCK Error Count: %lu \n", lock_error_count);

    // Cleanup: close the server socket
    close(server_sock);

    // Optionally, remove the socket file
    if (unlink(socket_path) == -1) {
        std::cerr << "Failed to remove socket file: " << strerror(errno) << "\n";
    }
    std::cout << "Server shutdown complete.\n";

}








static std::string sp_name = "/tmp/transfuse_mnt/1GB_sp.txt";
static std::string fsi_name = "/tmp/transfuse_mnt/1GB_fsi.txt";
static std::string schema_name = "/tmp/transfuse_mnt/1GB_schema.txt";
static std::string wal_name = "/tmp/transfuse_mnt/1GB_wal.txt";
static std::string log_file_name = "02transfuse_hot_cold_operations_1GB_";


// static std::string sp_name = "1GB_sp.txt";
// static std::string fsi_name = "1GB_fsi.txt";
// static std::string schema_name = "1GB_schema.txt";
// static std::string wal_name = "1GB_wal.txt";
// static std::string log_file_name = "NO_transfuse_NO_MLOCK_hot_cold_operations_1GB_";

static uint64_t mmapSize = 1ULL * 1024 * 1024 * 1024;

// constexpr static uint64_t max_num_records = 20971520; // For Reference
constexpr static uint64_t max_num_pages = 262100;


static std::vector<uint64_t> max_num_records = {1000, 7500, 10000};

static std::vector<double> memory_percent_vec = {1, 0.8, 0.5};
static std::vector<double> point_query_vs_scan_percent_vec = {0.2, 0.5, 0.8};

constexpr static double cold_data_percent = 0.2;
constexpr static double read_data_percent_hot_cold_test = 0.8;

// Generate a unique log file name with timestamp
std::ofstream initialize_log_file() {
   auto now = std::chrono::system_clock::now();
   auto now_time = std::chrono::system_clock::to_time_t(now);
   std::ostringstream file_name;
   file_name << log_file_name
             << std::put_time(std::localtime(&now_time), "%Y-%m-%d_%H-%M-%S") << ".log";
   std::ofstream log_file(file_name.str());
   if (!log_file.is_open()) {
      throw std::runtime_error("Unable to open log file!");
   }
   return log_file;
}

// Global log file and mutex for thread-safe logging
std::ofstream log_file = initialize_log_file();
std::mutex log_mutex;



namespace moderndbs {
std::unique_ptr<schema::Schema> getTPCHOrderSchema() {
   std::vector tables{
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
         0, 1,
         0)};
   auto schema = std::make_unique<schema::Schema>(std::move(tables));
   return schema;
}

}

using namespace std;

void initialize_schema() {
   auto schema = moderndbs::getTPCHOrderSchema();
   moderndbs::FileMapper schema_file_mapper(schema_name, (sysconf(_SC_PAGESIZE)), mmapSize);
   moderndbs::SchemaSegment schema_segment(49, schema_file_mapper);
   schema_segment.set_schema(moderndbs::getTPCHOrderSchema());
   schema_segment.write();
}

void initialize_DB(moderndbs::Database& db, const uint64_t records_count) {
   auto& table = db.get_schema().tables[0];
   const uint64_t progress_step = records_count / 10; // Calculate step for 10% progress
   uint64_t next_progress = progress_step;

   for (uint64_t i = 0; i < records_count; ++i) {
      moderndbs::OrderRecord order = {i, i * 2, i * 100, i % 5, (i % 2 == 0 ? 'G' : 'H')};
      auto transactionID = db.transaction_manager.startTransaction();
      db.insert(table, order, transactionID);
      db.transaction_manager.commitTransaction(transactionID);
      if (i + 1 == next_progress || i + 1 == records_count) { // Ensure progress is printed for the last record
         std::cout << "Progress: " << (100 * (i + 1) / records_count) << "% completed.\n";
         next_progress += progress_step;
      }
   }
}

void scan_DB(moderndbs::Database& db, const uint64_t start_page, const uint64_t end_page) {
   // std::cout << " Scan: start_page " << start_page << " end_page " << end_page << std::endl;
   auto& table = db.get_schema().tables[0];
   constexpr int num_threads = 4; // Number of threads
   threads_remaining.store(num_threads);
   std::vector<std::thread> workers;
   uint64_t total_pages = end_page - start_page;
   uint64_t pages_per_thread = total_pages / num_threads;

   // Start the timer
   auto start = std::chrono::high_resolution_clock::now();

   for (size_t thread = 0; thread < num_threads; ++thread) {
      uint64_t thread_start_page = start_page + thread * pages_per_thread;
      uint64_t thread_end_page = (thread == num_threads - 1) ? end_page : thread_start_page + pages_per_thread;

      workers.emplace_back([thread, &table, &db, thread_start_page, thread_end_page] {
         // std::cout << "Thread " << thread
         //           << " processing pages " << thread_start_page
         //           << " to " << thread_end_page - 1 << std::endl;

         for (uint64_t pg = thread_start_page; pg < thread_end_page; ++pg) {
            for (uint16_t sl = 0; sl < 79; ++sl) {
               // std::cout << "Reading Page " << pg << " SLOT: " << sl << std::endl;
               const moderndbs::TID tid{pg, sl};
               db.read_tuple(table, tid, 1); // Transaction ID is not used in reads
            }
         }


         if (--threads_remaining == 0) {
             std::lock_guard<std::mutex> lk(m_shutdown);
             /// WAIT FOR ANY new Messages yet to arrive from libfuse.
             shutdown_requested.store(true);
         }
      });
   }

   // Wait for all threads to complete
   for (auto& worker : workers) {
      worker.join();
   }

   auto end = std::chrono::high_resolution_clock::now();
   auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
   std::cout << "FINITO SCAN - Time taken: " << duration.count() << " seconds" << std::endl;
}

void random_operations_DB(moderndbs::Database& db, const uint64_t operations_num, const double read_percent) {
   auto& table = db.get_schema().tables[0];
   constexpr int num_threads = 4;
   std::vector<std::thread> workers;
   int thread_portion = operations_num / num_threads;

   /// Start the timer
   auto start = std::chrono::high_resolution_clock::now();

   for (size_t thread = 0; thread < num_threads; ++thread) {
      workers.emplace_back([thread, &table, &db, read_percent, thread_portion] {
         size_t progress_threshold = thread_portion / 10; // 10% progress threshold
         size_t next_progress = progress_threshold; // Next progress checkpoint

         /// Each thread has its own random engine (std::mt19937_64 engine{thread};) initialized with a unique seed (thread),
         /// ensuring each thread generates different random values.
         std::mt19937_64 engine{thread};
         // Pages and Slots
         std::uniform_int_distribution<uint64_t> page_distr{0, max_num_pages};
         std::uniform_int_distribution<uint16_t> slot_distr{0, 79};

         /// Read Percentage
         std::bernoulli_distribution reads_distr{read_percent};

         for (size_t j = 0; j < thread_portion; ++j) {
            auto page = page_distr(engine);
            auto slot = slot_distr(engine);
            moderndbs::TID tid{page, slot};

            /// Make a random read
            if (reads_distr(engine)) {
               // std::cout << " TID " << tid.get_value() <<  " Reading Page: " << page << " SLOT: " << slot << std::endl;

               db.read_tuple(table, tid, 1);
            } else {
               // std::cout << " Inserting Record: " << std::endl;

               moderndbs::OrderRecord order = {j, j * 2, j * 100, j % 5, (j % 2 == 0 ? 'G' : 'H')};
               auto transactionID = db.transaction_manager.startTransaction();
               db.update_tuple(table, tid, order, transactionID);
               // db.insert(table, order, transactionID);
               db.transaction_manager.commitTransaction(transactionID);
            }

            if (j + 1 >= next_progress) {
               std::cout << "Thread " << thread << ": "
                         << ((j + 1) * 100 / thread_portion) << "% complete ("
                         << (j + 1) << "/" << thread_portion << " operations)."
                         << std::endl;
               next_progress += progress_threshold;
            }
         }
         // std::cout << " THREAD FINISHED \n";
      });
   }

   for (auto& t : workers) {
      t.join();
   }

   auto end = std::chrono::high_resolution_clock::now();
   auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
   std::cout << "FINITO RANDOM - Time taken: " << duration.count() << " seconds" << std::endl;
}


void perform_scan_single_thread(moderndbs::Database& db, const uint64_t start_page, const uint64_t end_page) {
   auto& table = db.get_schema().tables[0];
   for (uint64_t pg = start_page; pg < end_page; ++pg) {
      for (uint16_t sl = 0; sl < 79; ++sl) {

         const moderndbs::TID tid{pg, sl};
         db.read_tuple(table, tid, 1); // Transaction ID is not used in reads
      }
   }
}


void hot_cold_operations_DB(moderndbs::Database& db, const double mem_percent, const uint64_t operations_num, const double point_query_percent, const double cold_data_percent, const double read_percent) {
   /// Shared set for tracking unique pages and its mutex
   std::atomic<uint64_t> total_pages_touched{0};

   auto& table = db.get_schema().tables[0];
   constexpr int num_threads = 4;
   threads_remaining.store(num_threads);

   std::vector<std::thread> workers;
   uint64_t thread_portion = operations_num / num_threads;

   /// Start the timer
   auto start = std::chrono::high_resolution_clock::now();

   for (size_t thread = 0; thread < num_threads; ++thread) {
      workers.emplace_back([thread, &table, &db, cold_data_percent, thread_portion, read_percent, point_query_percent, &total_pages_touched
                            /*, &upages_touched, &unique_pages_mutex*/] {
         size_t progress_threshold = thread_portion / 10; // 10% progress threshold
         size_t next_progress = progress_threshold; // Next progress checkpoint

         // Each thread has a unique `std::mt19937_64` engine seeded by its thread ID for distinct random values.
         std::mt19937_64 engine{thread};

         /// POINT query vs SCAN %
        std::bernoulli_distribution point_query_distr{point_query_percent};

         /// Read %
         std::bernoulli_distribution reads_distr{read_percent};

         /// Hold vs COld Data %
         std::bernoulli_distribution cold_distr{cold_data_percent};

         // Hot Pages are in the last 20% of pages
         uint64_t hot_data_start_page = max_num_pages * 80 / 100;
         std::uniform_int_distribution<uint64_t> hot_data_page_distr{hot_data_start_page, max_num_pages};

         // Cold data in the first 80% of pages
         uint64_t cold_pages_end = max_num_pages * 80 / 100 - 1; // End of the cold pages
         std::uniform_int_distribution<uint64_t> cold_data_page_distr{0, cold_pages_end};

         // Slot Distribution
         std::uniform_int_distribution<uint16_t> slot_distr{0, 79};

         for (size_t j = 0; j < thread_portion; ++j) {
            // int64_t touched_page = -1;

            /// is POINT query?
            if (point_query_distr(engine)) {

               auto page = hot_data_page_distr(engine);
               auto slot = slot_distr(engine);
               moderndbs::TID tid{page, slot};

               /// Make a random read
               if (reads_distr(engine)) {
                  db.read_tuple(table, tid, 1);

               } else {

                  // DO an Update
                  moderndbs::OrderRecord order = {j, j * 2, j * 100, j % 5, (j % 2 == 0 ? 'G' : 'H')};
                  auto transactionID = db.transaction_manager.startTransaction();
                  db.update_tuple(table, tid, order, transactionID);
                  db.transaction_manager.commitTransaction(transactionID);
               }

               total_pages_touched.fetch_add(1, std::memory_order_relaxed);

            } else { /// Analytical Query

               /// Scan HOT data ?
               if (cold_data_page_distr(engine)) {

                   // Scan Cold Data: Larger range
                   auto start_page = cold_data_page_distr(engine);
                   auto num_pages = std::uniform_int_distribution<uint64_t>{100, 500}(engine);
                   auto end_page = std::min(start_page + num_pages, cold_pages_end);

                   perform_scan_single_thread(db, start_page, end_page);

                  total_pages_touched.fetch_add(end_page - start_page + 1, std::memory_order_relaxed);

               } else { /// Scan Cold DATA

                  // Scan Hot Data: Smaller range (10-50) pages
                 auto start_page = hot_data_page_distr(engine);
                 auto num_pages = std::uniform_int_distribution<uint64_t>{10, 50}(engine);
                 auto end_page = std::min(start_page + num_pages, max_num_pages);

                 perform_scan_single_thread(db, start_page, end_page);

                  total_pages_touched.fetch_add(end_page - start_page + 1, std::memory_order_relaxed);
               }

            }
         }

      });
   }


   for (auto& t : workers) {
      t.join();
   }

   auto end = std::chrono::high_resolution_clock::now();
   auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
   std::cout << "FINITO HOT COld - Time taken: " << duration.count()
            << " seconds - Num Operations: " << operations_num
             << " point query % " << point_query_percent
            << " Read % " << read_data_percent_hot_cold_test
            << " cold data % " << cold_data_percent
            << " num pages " << total_pages_touched.load(std::memory_order_relaxed)
            << " Mem_X: " << std::fixed << std::setprecision(2)
            << static_cast<double>(total_pages_touched.load(std::memory_order_relaxed)) / (mem_percent * max_num_pages) << "\n";
   {
      std::lock_guard<std::mutex> lock(log_mutex);
      log_file << "FINITO HOT COLD - Time taken: " << duration.count()
               << " seconds - Num Operations: " << operations_num
               << " point query % " << point_query_percent
               << " Read % " << read_percent
               << " cold data % " << cold_data_percent
               << " total pages touched: " << total_pages_touched.load(std::memory_order_relaxed)
               << " Mem_X: " << std::fixed << std::setprecision(2)
               << static_cast<double>(total_pages_touched.load(std::memory_order_relaxed)) / (mem_percent * max_num_pages) << "\n";

      log_file.flush();
   }

}


void reset_cache() {
   int result = system("sudo sh -c \"echo 3 > /proc/sys/vm/drop_caches\"");
   if (result == 0) {
      std::cout << "Caches Deleted Successfully!" << std::endl;
   } else {
      std::cerr << "Failed to execute command." << std::endl;
      exit(1);
   }
}

int main(int argc, char* argv[]) {
   cout << "Testing 1GB - NO MLOCK" << endl;
   if (argc < 2) {
      std::cerr << "Usage: " << argv[0] << " <memory_size_in_bytes>" << std::endl;
      return 1;
   }

   double memory_percent = std::stod(argv[1]);
   log_file << "------------------------------------ " << memory_percent * 100 << "% ----------------------------\n";

   auto db = moderndbs::Database(schema_name, fsi_name, sp_name, wal_name, mmapSize);
   db.load_schema(49);

   // Starting socket server thread
   std::thread errorListener([&db]() { listenForErrors(db); });
   {
      std::unique_lock<std::mutex> lk(cv_m);
      cv.wait(lk, [] { return is_server_ready; });
   }


   for (uint64_t cur_op : max_num_records) {

      /// 2. run hot and cold access pattern.
      for (double point_percent: point_query_vs_scan_percent_vec) {
         std::cout << " Running hot Cold table operation Test with memory %: " << memory_percent * 100
                  << " and Operations_NUM " << cur_op
                  << " and Read % " << read_data_percent_hot_cold_test
                  << " Point % " << point_percent
                   << " Cold Data % " << cold_data_percent << std::endl;

         hot_cold_operations_DB(db, memory_percent, cur_op , point_percent, cold_data_percent, read_data_percent_hot_cold_test);
         reset_requested.store(true);
         // reset_cache();
      }
   }

   shutdown_requested.store(true);
   errorListener.join();
   std::cout << "Error listener terminated. Exiting...\n";
   log_file.close();
   return 0;
}