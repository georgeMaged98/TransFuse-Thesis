//
// Created by George Maged on 11.09.24.
//
#include "moderndbs/buffer_manager.h"
#include "moderndbs/file.h"
#include "moderndbs/hex_dump.h"
#include "moderndbs/segment.h"
#include "moderndbs/database.h"
#include <cstdint>
#include <cstring>
#include <exception>
#include <random>
#include <utility>
#include <vector>
#include <gtest/gtest.h>

using BufferManager = moderndbs::BufferManager;
using FSISegment = moderndbs::FSISegment;
using SPSegment = moderndbs::SPSegment;
using SchemaSegment = moderndbs::SchemaSegment;
using SlottedPage = moderndbs::SlottedPage;
using TID = moderndbs::TID;

namespace schema = moderndbs::schema;

namespace {
    bool compareVectors(const std::vector<std::string> &vector1, const std::vector<std::string> &vector2) {
        // First, check if the sizes are the same
        assert(vector1.size() == vector2.size() && "Vectors are of different sizes");

        // Then, compare each element with detailed debug output
        for (size_t i = 0; i < vector1.size(); ++i) {
            if (vector1[i] != vector2[i]) {
                std::cerr << "Mismatch at index " << i << ": "
                          << "vector1[" << i << "] = \"" << vector1[i] << "\", "
                          << "vector2[" << i << "] = \"" << vector2[i] << "\"\n";
                return false;
            }
        }
//        std::cout << "Vectors are equal!" << std::endl;
        return true;
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

    struct DatabaseOperationsTest : ::testing::Test {
    protected:
        void SetUp() override {
            using moderndbs::File;
            for (const auto *segment_file: std::vector<const char *>{"0", "1", "10", "11", "20", "21", "30", "31", "49.txt", "50.txt", "51.txt"}) {
                auto file = File::open_file(segment_file, File::Mode::WRITE);
                file->resize(0);
            }
//            File::delete_file("49.txt");
        }
    };


// NOLINTNEXTLINE
TEST_F(DatabaseOperationsTest, WriteReadTest) {
    auto db = moderndbs::Database();
    {
        moderndbs::BufferManager buffer_manager(1024, 10);
        moderndbs::SchemaSegment schema_segment(49, buffer_manager);
        schema_segment.set_schema(getTPCHOrderSchema());
        schema_segment.write();
    }

    db.load_schema(49);
    auto &table = db.get_schema().tables[0];
    std::vector<moderndbs::TID> tids;
    // Insert into table and read from it immediately
    for (uint64_t i = 0;i < 1000; ++i) {
        auto values = std::vector<std::string>{std::to_string(i), std::to_string(i * 2), (i % 2 == 0 ? "G" : "H"), std::to_string(i * 2), std::to_string(i * 2)};
        auto tid = db.insert(table, values);
        tids.push_back(tid);
        auto result = db.read_tuple(table, tid);
        ASSERT_TRUE(result);
        ASSERT_TRUE(compareVectors(values, result.value()));
    }

    // Now read inserted tids again
    for (uint64_t i = 0;i < 1000; ++i) {
        auto expected_values = std::vector<std::string>{std::to_string(i), std::to_string(i * 2), (i % 2 == 0 ? "G" : "H"), std::to_string(i * 2), std::to_string(i * 2)};
        auto result = db.read_tuple(table, tids[i]);
        ASSERT_TRUE(result);
        ASSERT_TRUE(compareVectors(expected_values, result.value()));
    }
}
} // namespace
