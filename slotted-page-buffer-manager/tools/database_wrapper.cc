#include "moderndbs/database.h"
#include "moderndbs/file.h"

#include <cassert>
#include <iostream>
#include <random>

namespace moderndbs {
class File;
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

//int main() {
//    auto db = moderndbs::Database();
//
//    int choice = 0;
//    do {
//        std::cout << "(1) Load schema from segment\n";
//        std::cout << "(2) Load TPCH-like schema\n";
//        std::cout << "> " << std::flush;
//        readLine(choice);
//    } while (choice != 1 && choice != 2);
//    if (choice == 1) {
//        // only support schema in segment 0 for now
//        db.load_schema(0);
//    } else if (choice == 2) {
//        db.load_new_schema(moderndbs::getTPCHSchemaLight());
//    }
//
//    while (true) {
//        do {
//            std::cout << "(1) insert\n";
//            std::cout << "(2) read\n";
//            std::cout << "> " << std::flush;
//            readLine(choice);
//        } while (choice != 1 && choice != 2);
//        if (choice == 1) {
//            do {
//                std::cout << "Select table:\n";
//                for (size_t i = 0; i < db.get_schema().tables.size(); ++i) {
//                    std::cout << "(" << i << ") " << db.get_schema().tables[i].id << "\n";
//                }
//                std::cout << "> " << std::flush;
//                readLine(choice);
//            } while (choice < 0 || size_t(choice) >= db.get_schema().tables.size());
//            auto &table = db.get_schema().tables[choice];
//            auto values = std::vector<std::string>();
//            for (const auto &column : table.columns) {
//                std::cout << "Value for " << column.id << "(" << column.type.name() << ")";
//                std::cout << " > " << std::flush;
//                std::string value;
//                readLine(value);
//                values.emplace_back(move(value));
//            }
//            db.insert(table, values);
//        } else if (choice == 2) {
//            do {
//                std::cout << "Select table:\n";
//                for (size_t i = 0; i < db.get_schema().tables.size(); ++i) {
//                    std::cout << "(" << i << ") " << db.get_schema().tables[i].id << "\n";
//                }
//                std::cout << "> " << std::flush;
//                readLine(choice);
//            } while (choice < 0 || size_t(choice) >= db.get_schema().tables.size());
//            auto &table = db.get_schema().tables[choice];
//            std::cout << "Enter TID:\n";
//            std::cout << "> " << std::flush;
//            readLine(choice);
//            db.read_tuple(table, moderndbs::TID(choice));
//        }
//    }

//} // namespace moderndbs

void compareVectors(const std::vector<std::string> &vector1, const std::vector<std::string> &vector2) {
    // First, check if the sizes are the same
    assert(vector1.size() == vector2.size() && "Vectors are of different sizes");

    // Then, compare each element with detailed debug output
    for (size_t i = 0; i < vector1.size(); ++i) {
        if (vector1[i] != vector2[i]) {
            std::cerr << "Mismatch at index " << i << ": "
                      << "vector1[" << i << "] = \"" << vector1[i] << "\", "
                      << "vector2[" << i << "] = \"" << vector2[i] << "\"\n";
            assert(false && "Vector elements are not equal");
        }
    }
    // std::cout << "Vectors are equal!" << std::endl;
}

// int main() {
//     auto db = moderndbs::Database();
//     {
//         auto schema = moderndbs::getTPCHOrderSchema();
//         moderndbs::BufferManager buffer_manager(sysconf(_SC_PAGESIZE), 1000);
//         moderndbs::SchemaSegment schema_segment(49, buffer_manager);
//         schema_segment.set_schema(moderndbs::getTPCHOrderSchema());
//         schema_segment.write();
//     }
//     db.load_schema(49);
//     auto &table = db.get_schema().tables[0];
//
//     std::vector<moderndbs::TID> tids;
//     // Insert into table and read from it immediately
//     for (uint64_t i = 0; i < 1500; ++i) {
//         auto values = std::vector<std::string>{std::to_string(i), std::to_string(i * 2), (i % 2 == 0 ? "G" : "H"), std::to_string(i * 2), std::to_string(i * 2)};
//         auto tid = db.insert(table, values);
//         tids.push_back(tid);
//         auto result = db.read_tuple(table, tid);
//         // if(result.has_value())
//         //     compareVectors(values, result.value());
//     }
//
//    // Now read inserted tids again
//    for (uint64_t i = 0;i < 1500; ++i) {
//       auto expected_values = std::vector<std::string>{std::to_string(i), std::to_string(i * 2), (i % 2 == 0 ? "G" : "H"), std::to_string(i * 2), std::to_string(i * 2)};
//       auto result = db.read_tuple(table, tids[i]);
//       // ASSERT_TRUE(result);
//       // ASSERT_TRUE(compareVectors(expected_values, result.value()));
//    }
//

   // // moderndbs::TID tid = moderndbs::TID(0, 0);
   // // std::cout << tid.get_value() << " Page: " << tid.get_page_id(table.sp_segment) << " Page: " << tid.get_slot() << std::endl;
   // std::mt19937_64 engine{0};
   // std::uniform_int_distribution<uint64_t> page{0, 39};
   // std::uniform_int_distribution<uint64_t> slot{0, 37};
   // for (int i = 0; i < 50; ++i) {
   //    uint64_t random_page = page(engine);
   //    uint64_t random_slot = slot(engine);
   //    auto tid = moderndbs::TID(random_page, random_slot);
   //    // Alternatively, you could print them directly:
   //    // std::cout << "RAND: " << random_value << " " << random_value2  << " \n";
   //    // std::cout << "TID: " << tid.get_page_id(table.sp_segment) << " Slot: " <<tid.get_slot() << std::endl;
   //    auto result = db.read_tuple(table, tid);
   // }
// }


/// DEFAULT COMPREHENSIVE MAIN:
int main() {
   for (const auto *segment_file: std::vector<const char *>{"49.txt", "50.txt", "51.txt"}) {
      auto file = moderndbs::File::open_file(segment_file, moderndbs::File::Mode::WRITE);
      file->resize(0);
   }
   {
      auto file = moderndbs::File::open_file("55.txt", moderndbs::File::Mode::WRITE);
      file->resize(sizeof(uint64_t));
      uint64_t initial_lsn = 0;
      file->write_block(reinterpret_cast<const char*>(&initial_lsn), 0, sizeof(initial_lsn));

   }

   auto db = moderndbs::Database();

   {
      auto schema = moderndbs::getTPCHOrderSchema();
      moderndbs::BufferManager buffer_manager(sysconf(_SC_PAGESIZE), 1000);
      moderndbs::SchemaSegment schema_segment(49, buffer_manager);
      schema_segment.set_schema(moderndbs::getTPCHOrderSchema());
      schema_segment.write();
   }
   db.load_schema(49);
   auto& table = db.get_schema().tables[0];

   // INSERTIONS -> INSERT OrderRecords for 20 pages.
   for (uint64_t i = 0; i < 102400; ++i) {
       moderndbs::OrderRecord order = {i, i * 2, i * 100, i % 5, (i % 2 == 0 ? 'G' : 'H')};
       auto transactionID = db.transaction_manager.startTransaction();
       db.insert(table, order, transactionID);
       db.transaction_manager.commitTransaction(transactionID);
   }


   constexpr int num_threads = 4;
   std::vector<std::thread> workers;
   auto start = std::chrono::high_resolution_clock::now();

   for (size_t thread = 0; thread < num_threads; ++thread) {
      workers.emplace_back([thread, &table, &db] {
         /// Each thread has its own random engine (std::mt19937_64 engine{thread};) initialized with a unique seed (thread),
         /// ensuring each thread generates different random values.
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
         std::uniform_int_distribution<uint16_t> page_distr{0, 10};
         std::uniform_int_distribution<uint16_t> slot_distr{0, 79};

         for (size_t j = 0; j < 1000; ++j) {
            // if (scan_distr(engine)) {
            //    /// READ two full segments
            //    auto start_page = page_distr(engine);
            //    auto end_page = start_page + 2;
            //    for (uint16_t pg = start_page; pg < end_page && pg < 20; ++pg) {
            //       for (uint16_t sl = 0; sl < 79; ++sl) {
            //          std::cout << " Page: " << pg << " SLOT: " << sl << " \n";
            //          moderndbs::TID tid{pg, sl};
            //          std::cout << " Reading Tuple with TID: " << tid.get_value() << " \n";
            //          auto transactionID = db.transaction_manager.startTransaction();
            //          db.read_tuple(table, tid, transactionID);
            //          db.transaction_manager.commitTransaction(transactionID);
            //       }
            //    }
            //    std::cout << " SCANS\n";
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
            // }

         }
         std::cout << " THREAD FINISHED \n";
      });
   }

   for (auto& t : workers) {
      t.join();
   }

   std::cout << "FINITO\n";

   auto end = std::chrono::high_resolution_clock::now();
   auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
   std::cout << "Time taken: " << duration.count() << " milliseconds" << std::endl;

   return 0;
}
