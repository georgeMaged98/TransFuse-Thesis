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
#include <thread>
#include <random>
#include <utility>
#include <vector>
#include <gtest/gtest.h>
#include <barrier>


using BufferManager = moderndbs::BufferManager;
using FSISegment = moderndbs::FSISegment;
using SPSegment = moderndbs::SPSegment;
using SchemaSegment = moderndbs::SchemaSegment;
using SlottedPage = moderndbs::SlottedPage;
using TID = moderndbs::TID;

namespace schema = moderndbs::schema;

namespace {
   // Function to print a vector of strings
   void printVector(const std::vector<std::string>& vec) {
      for (const auto& str : vec) {
         std::cout << str << " ";
      }
      std::cout << "\n";
   }

bool compareOrderRecords(const moderndbs::OrderRecord &record1, const moderndbs::OrderRecord &record2) {
      // Compare each field in the OrderRecord and print detailed output if there's a mismatch

      if (record1.o_orderkey != record2.o_orderkey) {
         std::cerr << "Mismatch in o_orderkey: "
                   << "record1.o_orderkey = " << record1.o_orderkey << ", "
                   << "record2.o_orderkey = " << record2.o_orderkey << "\n";
         return false;
      }

      if (record1.o_custkey != record2.o_custkey) {
         std::cerr << "Mismatch in o_custkey: "
                   << "record1.o_custkey = " << record1.o_custkey << ", "
                   << "record2.o_custkey = " << record2.o_custkey << "\n";
         return false;
      }

      if (record1.o_totalprice != record2.o_totalprice) {
         std::cerr << "Mismatch in o_totalprice: "
                   << "record1.o_totalprice = " << record1.o_totalprice << ", "
                   << "record2.o_totalprice = " << record2.o_totalprice << "\n";
         return false;
      }

      if (record1.o_shippriority != record2.o_shippriority) {
         std::cerr << "Mismatch in o_shippriority: "
                   << "record1.o_shippriority = " << record1.o_shippriority << ", "
                   << "record2.o_shippriority = " << record2.o_shippriority << "\n";
         return false;
      }

      if (record1.o_orderstatus != record2.o_orderstatus) {
         std::cerr << "Mismatch in o_orderstatus: "
                   << "record1.o_orderstatus = " << record1.o_orderstatus << ", "
                   << "record2.o_orderstatus = " << record2.o_orderstatus << "\n";
         return false;
      }

      // If all fields are equal, the records are considered equal
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
        }
    };


// NOLINTNEXTLINE
TEST_F(DatabaseOperationsTest, WriteReadTest) {
    auto db = moderndbs::Database();
    {
        moderndbs::BufferManager buffer_manager(sysconf (_SC_PAGESIZE), 10);
        moderndbs::SchemaSegment schema_segment(49, buffer_manager);
        schema_segment.set_schema(getTPCHOrderSchema());
        schema_segment.write();
    }

    db.load_schema(49);
    auto &table = db.get_schema().tables[0];
    std::vector<moderndbs::TID> tids;
    // Insert into table and read from it immediately
    for (uint64_t i = 0;i < 200; ++i) {
       moderndbs::OrderRecord order = {i,i * 2,i * 100,i % 5,(i % 2 == 0 ? 'G' : 'H')};
       auto tid = db.insert(table, order);
       tids.push_back(tid);
       auto result = db.read_tuple(table, tid);
       ASSERT_TRUE(result);
       ASSERT_TRUE(compareOrderRecords(order, result.value()));
    }

    // Now read inserted tids again
    for (uint64_t i = 0;i < 200; ++i) {
       moderndbs::OrderRecord expected_order = {i,i * 2,i * 100,i % 5,(i % 2 == 0 ? 'G' : 'H')};
       auto result = db.read_tuple(table, tids[i]);
        ASSERT_TRUE(result);
        ASSERT_TRUE(compareOrderRecords(expected_order, result.value()));
    }
}

TEST_F(DatabaseOperationsTest, UpdateTupleTest) {
   auto db = moderndbs::Database();
   {
      moderndbs::BufferManager buffer_manager(sysconf (_SC_PAGESIZE), 10);
      moderndbs::SchemaSegment schema_segment(49, buffer_manager);
      schema_segment.set_schema(getTPCHOrderSchema());
      schema_segment.write();
   }

   db.load_schema(49);
   const auto &table = db.get_schema().tables[0];
   std::vector<moderndbs::TID> tids;
   // Insert into table and read from it immediately
   for (uint64_t i = 0;i < 200; ++i) {
       moderndbs::OrderRecord order = {i,i * 2,i * 100,i % 5,(i % 2 == 0 ? 'G' : 'H')};
      auto tid = db.insert(table, order);
      tids.push_back(tid);
      auto result = db.read_tuple(table, tid);
      ASSERT_TRUE(result);
      ASSERT_TRUE(compareOrderRecords(order, result.value()));
   }

   // Update some tuples
   for (uint64_t i = 1; i < 200; i += 2) {
      moderndbs::OrderRecord new_order = {2000, 3000, 4000, 5000, 'U'};
      db.update_tuple(table, tids[i], new_order);
      auto result = db.read_tuple(table, tids[i]);
      compareOrderRecords(new_order, result.value());
   }

   // Read Everything -> Updated tids should have updated values
   for (uint64_t i = 0; i < 200; ++i) {
      moderndbs::OrderRecord order = {i,i * 2,i * 100,i % 5,(i % 2 == 0 ? 'G' : 'H')};
      moderndbs::OrderRecord new_order = {2000, 3000, 4000, 5000, 'U'};
      auto result = db.read_tuple(table, tids[i]);
      ASSERT_TRUE(result);
      const auto& expected_values = (i % 2 == 1) ? new_order : order;
      ASSERT_TRUE(compareOrderRecords(expected_values, result.value()));
   }
}


TEST_F(DatabaseOperationsTest, DeleteTupleTest) {
   auto db = moderndbs::Database();
   {
      moderndbs::BufferManager buffer_manager(sysconf (_SC_PAGESIZE), 10);
      moderndbs::SchemaSegment schema_segment(49, buffer_manager);
      schema_segment.set_schema(getTPCHOrderSchema());
      schema_segment.write();
   }
   db.load_schema(49);
   const auto& table = db.get_schema().tables[0];
   moderndbs::OrderRecord order = {10,20,30,40,'D'};
   const auto tid = db.insert(table, order);
   // make sure tuple is inserted properly
   auto result = db.read_tuple(table, tid);
   ASSERT_TRUE(result);
   ASSERT_TRUE(compareOrderRecords(order, result.value()));

   // delete tuple
   db.delete_tuple(table, tid);
   // tuple should no longer be there
   result = db.read_tuple(table, tid);
   ASSERT_FALSE(result);
}


TEST_F(DatabaseOperationsTest, MultithreadWriters) {
   auto db = moderndbs::Database();
   {
      moderndbs::BufferManager buffer_manager(sysconf (_SC_PAGESIZE), 10);
      moderndbs::SchemaSegment schema_segment(49, buffer_manager);
      schema_segment.set_schema(getTPCHOrderSchema());
      schema_segment.write();
   }
   db.load_schema(49);
   auto &table = db.get_schema().tables[0];
   // Pre-allocate the tids vector to the correct size
   std::vector<moderndbs::TID> tids;
   uint32_t insertions_per_thread = 100;
   tids.reserve(4 * insertions_per_thread); // 4 threads

   std::barrier sync_point(4);
   std::vector<std::thread> threads;
   // std::mutex tids_mutex;
   std::vector<std::vector<moderndbs::TID>> tids_per_thread(4);

   for (size_t thread = 0; thread < 4; ++thread) {
      threads.emplace_back([thread, &sync_point, &table, &db, &tids_per_thread, insertions_per_thread] {
         size_t startValue = thread * insertions_per_thread;
         size_t limit = startValue + insertions_per_thread;
         // Insert values
         for (uint64_t i = 0; i < insertions_per_thread; ++i) {
            // std::lock_guard<std::mutex> lock(tids_mutex);
            moderndbs::OrderRecord order = {i,i * 2,i * 100,i % 5,(i % 2 == 0 ? 'G' : 'H')};
            auto tid = db.insert(table, order);
            tids_per_thread[thread].push_back(tid);
         }

         sync_point.arrive_and_wait();
         // And read them back
         for (uint64_t i = 0; i < insertions_per_thread; ++i) {
            moderndbs::OrderRecord order = {i,i * 2,i * 100,i % 5,(i % 2 == 0 ? 'G' : 'H')};
            auto tid = tids_per_thread[thread][i];
            // printVector(expected_values);
            auto result = db.read_tuple(table, tid);
            // printVector(result.value());
            ASSERT_TRUE(result);
            ASSERT_TRUE(compareOrderRecords(order, result.value()));
         }
      });
   }

   for (auto& t : threads)
      t.join();
}



} // namespace
