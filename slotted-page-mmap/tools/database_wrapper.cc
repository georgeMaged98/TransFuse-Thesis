#include "moderndbs/database.h"
#include <cassert>
#include <iostream>
#include <random>

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
                        {
                                "c_custkey"
                        },
                        10, 11
                ),
                schema::Table(
                        "nation",
                        {
                                schema::Column("n_nationkey", schema::Type::Integer()),
                                schema::Column("n_name", schema::Type::Char(25)),
                                schema::Column("n_regionkey", schema::Type::Integer()),
                                schema::Column("n_comment", schema::Type::Char(152)),
                        },
                        {
                                "n_nationkey"
                        },
                        20, 21
                ),
                schema::Table(
                        "region",
                        {
                                schema::Column("r_regionkey", schema::Type::Integer()),
                                schema::Column("r_name", schema::Type::Char(25)),
                                schema::Column("r_comment", schema::Type::Char(152)),
                        },
                        {
                                "r_regionkey"
                        },
                        30, 31
                ),
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
                        {
                                "o_orderkey"
                        },
                        50, 51,
                        0
                )
        };
        auto schema = std::make_unique<schema::Schema>(std::move(tables));
        return schema;
    }

}

template<typename T>
void readLine(T &v);

template<>
void readLine(std::string &v) {
    std::getline(std::cin, v);
}

template<>
void readLine(int &v) {
    std::string line;
    std::getline(std::cin, line);
    try {
        v = std::stoi(line);
    }
    catch (...) {}
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
    std::cout << "Vectors are equal!" << std::endl;
}

int main() {
   auto db = moderndbs::Database();
   {
      moderndbs::FileMapper schema_file_mapper("schema_segment.txt", 1024);
      moderndbs::SchemaSegment schema_segment(49, schema_file_mapper);
      schema_segment.set_schema(moderndbs::getTPCHOrderSchema());
      schema_segment.write();
   }

   db.load_schema(49);
   auto &table = db.get_schema().tables[0];
   std::vector<moderndbs::TID> tids;
   // Insert into table and read from it immediately
   for (uint64_t i = 0;i < 50; ++i) {
      auto values = std::vector<std::string>{std::to_string(i), std::to_string(i * 2), (i % 2 == 0 ? "G" : "H"), std::to_string(i * 2), std::to_string(i * 2)};
      auto tid = db.insert(table, values);
      tids.push_back(tid);
      auto result = db.read_tuple(table, tid);
      // ASSERT_TRUE(result);
      // ASSERT_TRUE(compareVectors(values, result.value()));
   }
   // Now read inserted tids again
   for (uint64_t i = 0;i < 50; ++i) {
      auto expected_values = std::vector<std::string>{std::to_string(i), std::to_string(i * 2), (i % 2 == 0 ? "G" : "H"), std::to_string(i * 2), std::to_string(i * 2)};
      auto result = db.read_tuple(table, tids[i]);
      // ASSERT_TRUE(result);
      // ASSERT_TRUE(compareVectors(expected_values, result.value()));
   }

   // std::mt19937_64 engine{0};
   // std::uniform_int_distribution<uint64_t> page{0, 39};
   // std::uniform_int_distribution<uint64_t> slot{0, 37};
   // for (int i = 0; i < 1000; ++i) {
   //    uint64_t random_page = page(engine);
   //    uint64_t random_slot = slot(engine);
   //    auto tid = moderndbs::TID(random_page, random_slot);
   //    // Alternatively, you could print them directly:
   //    // std::cout << "RAND: " << random_value << " " << random_value2  << " \n";
   //    // std::cout << "TID: " << tid.get_page_id(table.sp_segment) << " Slot: " <<tid.get_slot() << std::endl;
   //    auto result = db.read_tuple(table, tid);
   // }


}
