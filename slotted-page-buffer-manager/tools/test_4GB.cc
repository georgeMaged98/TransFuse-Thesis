//
// Created by george-elfayoumi on 11/20/24.
//

#include <iomanip>
#include <iostream>
#include <memory>
#include <ostream>
#include <random>
#include <moderndbs/database.h>

static int16_t sp_name = 4012;
static int16_t fsi_name = 4013;
static int16_t schema_name = 4014;
static int16_t wal_name = 4015;

/// The BM size that should hold all the data without evicting any pages
/// Let's consider 4 GB RAM for 4 GB data. (4 GB = 4 * 1024 * 1024 KB) divided by 4 (KB)
/// to get number of buffer frame pages that cover whole dataset.
constexpr static int max_BM_size = (4 * 1024 * 1024) / 4;
// constexpr static uint64_t max_num_records = 20971520; /// For Reference
static std::vector<uint64_t> max_num_records = {4 * 1000, 4 * 7500, 4 * 10000};
constexpr static uint64_t max_num_pages = 1048570;

static std::vector<double> memory_percent_vec = {1, 0.8, 0.5};
static std::vector<double> point_query_vs_scan_percent_vec = {0.2, 0.5, 0.8};

constexpr static double cold_data_percent = 0.2;
constexpr static double read_data_percent_hot_cold_test = 0.8;

// Generate a unique log file name with timestamp
std::ofstream initialize_log_file() {
   auto now = std::chrono::system_clock::now();
   auto now_time = std::chrono::system_clock::to_time_t(now);
   std::ostringstream file_name;
   file_name << "hot_cold_operations_4GB_"
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
         sp_name, fsi_name,
         0)};
   auto schema = std::make_unique<schema::Schema>(std::move(tables));
   return schema;
}

}

using namespace std;

void initialize_schema() {
   auto schema = moderndbs::getTPCHOrderSchema();
   moderndbs::BufferManager buffer_manager(4096, 1000);
   moderndbs::SchemaSegment schema_segment(schema_name, buffer_manager);
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
         // std::cout << "Progress: " << (100 * (i + 1) / records_count) << "% completed.\n";
         next_progress += progress_step;
      }
   }
}

void scan_DB(moderndbs::Database& db, const uint64_t start_page, const uint64_t end_page) {
   // std::cout << " Scan: start_page " << start_page << " end_page " << end_page << std::endl;
   auto& table = db.get_schema().tables[0];
   constexpr int num_threads = 4; // Number of threads
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

            if (j + 1 >= next_progress && thread == 0) {
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
   std::cout << "FINITO RANDOM - Time taken: " << duration.count() << " seconds - Read % "
             << read_percent << " Num Operations: " << operations_num << std::endl;
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

            if (j + 1 >= next_progress && thread == 0) {
               next_progress += progress_threshold;
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
            << " num pages " << total_pages_touched.load(std::memory_order_relaxed) << std::endl;
   {
      std::lock_guard<std::mutex> lock(log_mutex);
      log_file << "FINITO HOT COLD - Time taken: " << duration.count()
               << " seconds - Num Operations: " << operations_num
               << " point query % " << point_query_percent
               << " Read % " << read_percent
               << " cold data % " << cold_data_percent
               << " total pages touched: " << total_pages_touched.load(std::memory_order_relaxed)
               << " BM_X: " << std::fixed << std::setprecision(2)
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

int main() {
   cout << "Testing 4GB" << endl;
   //
   // {
   //    auto db = moderndbs::Database(0.8 * max_BM_size, wal_name);
   //    db.load_schema(schema_name);
   //    scan_DB(db,0, max_num_pages);
   //    // random_operations_DB(db, 300000, 0.5);
   //
   //    // moderndbs::TID tid{0, 0};
   //    // moderndbs::OrderRecord order = {5, 5 * 2, 5 * 100, 5 % 5, (5 % 2 == 0 ? 'G' : 'H')};
   //    // scan_DB(db, 0, max_num_pages);
   //    // auto& table = db.get_schema().tables[0];
   //    // db.read_tuple(table, tid, 1);
   //
   //    // hot_cold_operations_DB(db, 100000, cold_data_percent, read_data_percent_hot_cold_test);
   // }

   // Iterate over all memory sizes
   for (double memory_percent : memory_percent_vec) {
      /// Set BM Size
      const uint64_t localBMSize = memory_percent * max_BM_size;
      log_file << "--------------------------  Memory Percent " << memory_percent << " % -------------------- " << std::endl;
      /// First perform scan:

      /// 1. Reset
      // reset_cache();

      /// 2. Run the Scan
      // std::cout << " ------------------------ memory %: " << memory_percent * 100 << " ------------------------ " << std::endl;
      // for (int i = 0; i < 3; ++i) {
      //    std::cout << "RUN " << i << std::endl;
      //    auto db = moderndbs::Database(localBMSize, wal_name);
      //    db.load_schema(schema_name);
      //    scan_DB(db, 0, max_num_pages);
      // }


      //
      for (uint64_t cur_op : max_num_records) {
      //    // std::cout << " Num Records: " << cur_op << std::endl;
      //
      //    // /// Perform Random Reads
      //    for (double read_percent : read_percent_vev) {
      //       // std::cout << " Running random table operation with memory %: "
      //       //           << memory_percent << " and Read % " << read_percent << std::endl;
      //
      //       /// 1. Reset
      //       // reset_cache();
      //
      //       /// 2. Run Random Operations
      //       {
      //          auto db = moderndbs::Database(localBMSize, wal_name);
      //          db.load_schema(schema_name);
      //          random_operations_DB(db, cur_op, read_percent);
      //       }
      //    }
      //
      //    /// HOT and Cold Access Pattern
      //
      //    /// 1. Reset
      //    // reset_cache();

         /// 2. run hot and cold access pattern.
         for (double point_percent: point_query_vs_scan_percent_vec) {
            std::cout << " Running hot Cold table operation Test with memory %: " << memory_percent * 100
                     << " and Operations_NUM " << cur_op
                     << " and Read % " << read_data_percent_hot_cold_test
                     << " Point % " << point_percent
                      << " Cold Data % " << cold_data_percent << std::endl;
            {
               auto db = moderndbs::Database(localBMSize, wal_name);
               db.load_schema(schema_name);
               hot_cold_operations_DB(db, memory_percent, cur_op , point_percent, cold_data_percent, read_data_percent_hot_cold_test);
            }
         }

      }
   }

   log_file.close();

   return 0;
}