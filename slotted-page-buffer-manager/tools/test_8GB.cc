//
// Created by george-elfayoumi on 11/20/24.
//

#include <iostream>
#include <memory>
#include <ostream>
#include <random>
#include <moderndbs/database.h>

static int16_t sp_name = 8012;
static int16_t fsi_name = 8013;
static int16_t schema_name = 8014;
static int16_t wal_name = 8015;

/// The BM size that should hold all the data without evicting any pages
/// Let's consider 600 MB for 512 MB data. (600 MB = 600 * 1024 KB) divided by 4 (KB)
/// to get number of buffer frame pages that cover whole dataset.
constexpr static int max_BM_size = (8 * 1024 * 1024) / 4;
static std::vector<uint64_t> max_num_records = {100000, 1000000}; // 100,000  - 1,000,000
constexpr static uint64_t max_num_pages = 2097148;

static std::vector<double> memory_percent_vec = {0.5, 0.8, 1};
static std::vector<double> read_percent_vev = {0.2, 0.5, 0.8};

constexpr static double cold_data_percent = 0.1;
constexpr static double read_data_percent_hot_cold_test = 0.8;

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
         std::cout << "Progress: " << (100 * (i + 1) / records_count) << "% completed.\n";
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
         /// Each thread has its own random engine (std::mt19937_64 engine{thread};) initialized with a unique seed (thread),
         /// ensuring each thread generates different random values.
         std::mt19937_64 engine{thread};
         // Pages and Slots
         std::uniform_int_distribution<uint64_t> page_distr{0, max_num_pages};
         std::uniform_int_distribution<uint16_t> slot_distr{0, 79};

         /// Read Percentage
         std::bernoulli_distribution reads_distr{read_percent};

         for (size_t j = 0; j < thread_portion; ++j) {
            /// Make a random read
            if (reads_distr(engine)) {
               auto page = page_distr(engine);
               auto slot = slot_distr(engine);

               moderndbs::TID tid{page, slot};
               // std::cout << " TID " << tid.get_value() <<  " Reading Page: " << page << " SLOT: " << slot << std::endl;

               db.read_tuple(table, tid, 1);
            } else {
               // std::cout << " Inserting Record: " << std::endl;

               moderndbs::OrderRecord order = {j, j * 2, j * 100, j % 5, (j % 2 == 0 ? 'G' : 'H')};
               auto transactionID = db.transaction_manager.startTransaction();
               db.insert(table, order, transactionID);
               db.transaction_manager.commitTransaction(transactionID);
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

void hot_cold_operations_DB(moderndbs::Database& db, const uint64_t operations_num, const double cold_data_percent, const double read_percent) {
   auto& table = db.get_schema().tables[0];
   constexpr int num_threads = 4;
   std::vector<std::thread> workers;
   int thread_portion = operations_num / num_threads;

   /// Start the timer
   auto start = std::chrono::high_resolution_clock::now();

   for (size_t thread = 0; thread < num_threads; ++thread) {
      workers.emplace_back([thread, &table, &db, cold_data_percent, thread_portion, read_percent] {
         /// Each thread has its own random engine (std::mt19937_64 engine{thread};) initialized with a unique seed (thread),
         /// ensuring each thread generates different random values.
         std::mt19937_64 engine{thread};

         /// Read %
         std::bernoulli_distribution reads_distr{read_percent};

         /// Hold vs COld Data %
         std::bernoulli_distribution cold_distr{cold_data_percent};

         // Hot Pages are towards the end (let's say last 50 pages)
         uint64_t hot_data_start_page = max_num_pages - (max_num_pages / 10);

         std::uniform_int_distribution<uint64_t> hot_data_page_distr{hot_data_start_page, max_num_pages};

         // Cold data in first 90% of pages
         uint64_t cold_pages_end = max_num_pages * 90 / 100;

         std::uniform_int_distribution<uint64_t> cold_data_page_distr{0, cold_pages_end};

         // Slot Distribution
         std::uniform_int_distribution<uint16_t> slot_distr{0, 79};

         for (size_t j = 0; j < thread_portion; ++j) {
            /// Is Cold data
            if (cold_distr(engine)) {
               auto page = cold_data_page_distr(engine);
               auto slot = slot_distr(engine);
               moderndbs::TID tid{page, slot};

               /// Make a random read
               if (reads_distr(engine)) {
                  // std::cout << " COLD Reading Page: " << page << " SLOT: " << slot << std::endl;

                  db.read_tuple(table, tid, 1);
               } else {
                  // Do an UPDATE
                  // std::cout << " COLD Updating Record:  " << page << " Slot: " << slot << std::endl;

                  moderndbs::OrderRecord order = {j, j * 2, j * 100, j % 5, (j % 2 == 0 ? 'G' : 'H')};
                  auto transactionID = db.transaction_manager.startTransaction();
                  db.update_tuple(table, tid, order, transactionID);
                  db.transaction_manager.commitTransaction(transactionID);
               }
            } else {
               // Hot Data
               auto page = hot_data_page_distr(engine);
               auto slot = slot_distr(engine);
               moderndbs::TID tid{page, slot};

               /// Make a random read
               if (reads_distr(engine)) {
                  // std::cout << " HOT Reading Page: " << page << " SLOT: " << slot << std::endl;

                  db.read_tuple(table, tid, 1);
               } else {
                  // DO an Update
                  // std::cout << " HOT Updating Record:  " << page << " Slot: " << slot << std::endl;
                  moderndbs::OrderRecord order = {j, j * 2, j * 100, j % 5, (j % 2 == 0 ? 'G' : 'H')};
                  auto transactionID = db.transaction_manager.startTransaction();
                  db.update_tuple(table, tid, order, transactionID);
                  db.transaction_manager.commitTransaction(transactionID);
               }
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
   std::cout << "FINITO HOT COld - Time taken: " << duration.count() << " seconds" << std::endl;
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
   cout << "Testing 8GB" << endl;

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

      /// First perform scan:

      /// 1. Reset
      // reset_cache();

      /// 2. Run the Scan
      std::cout << " ------------------------ memory %: " << memory_percent * 100 << " ------------------------ " << std::endl;
      for (int i = 0; i < 3; ++i) {
         std::cout << "RUN " << i << std::endl;
         auto db = moderndbs::Database(localBMSize, wal_name);
         db.load_schema(schema_name);
         scan_DB(db, 0, max_num_pages);
      }
      //
      // for (uint64_t cur_op : max_num_records) {
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
      //
      //    /// 2. run hot and cold access pattern.
      //    // std::cout << " Running hot Cold table operation Test with memory %: "
      //    //           << memory_percent << " and Read % " << read_data_percent_hot_cold_test
      //    //           << " Cold Data % " << cold_data_percent << std::endl;
      //    {
      //       auto db = moderndbs::Database(localBMSize, wal_name);
      //       db.load_schema(schema_name);
      //       hot_cold_operations_DB(db, cur_op, cold_data_percent, read_data_percent_hot_cold_test);
      //    }
      // }
   }



   // // initialize_schema();
   // scan_DB(db, 0, 2);
   //
   // auto db = moderndbs::Database(max_BM_size, wal_name);
   // uint64_t num_rows = max_num_pages * 80;
   // uint64_t round2 = num_rows / 2;
   // std::cout << "INSERTING " << round2 << " rows." << std::endl;
   // // initialize_schema();
   // db.load_schema(schema_name);
   // initialize_DB(db, round2);
   return 0;
}
