//
// Created by george-elfayoumi on 11/20/24.
//

#include <iostream>
#include <memory>
#include <ostream>
#include <moderndbs/database.h>

static int16_t sp_name = 512;
static int16_t fsi_name = 513;
static int16_t schema_name = 514;
static int16_t wal_name = 515;

static int BM_size = 100;

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
   moderndbs::BufferManager buffer_manager(sysconf(_SC_PAGESIZE), 1000);
   moderndbs::SchemaSegment schema_segment(schema_name, buffer_manager);
   schema_segment.set_schema(moderndbs::getTPCHOrderSchema());
   schema_segment.write();
}

void initialize_DB(moderndbs::Database &db, const uint64_t records_count) {
   auto& table = db.get_schema().tables[0];
   for (uint64_t i = 0; i < records_count; ++i) {
      moderndbs::OrderRecord order = {i, i * 2, i * 100, i % 5, (i % 2 == 0 ? 'G' : 'H')};
      auto transactionID = db.transaction_manager.startTransaction();
      db.insert(table, order, transactionID);
      db.transaction_manager.commitTransaction(transactionID);
   }
}

void scan_DB(moderndbs::Database &db, const uint64_t start_page, const uint64_t end_page) {
   std::cout << " Scan: start_page " << start_page << " end_page " << end_page << std::endl;
   auto& table = db.get_schema().tables[0];
   for (uint16_t pg = start_page; pg < end_page; ++pg) {
      for (uint16_t sl = 0; sl < 79; ++sl) {
         const moderndbs::TID tid{pg, sl};
         const auto transactionID = db.transaction_manager.startTransaction();
         db.read_tuple(table, tid, transactionID);
         db.transaction_manager.commitTransaction(transactionID);
      }
   }
}

int main() {
   cout << "Testing 512M" << endl;
   uint64_t num_records = 13421772;
   auto db = moderndbs::Database(100000, wal_name);
   initialize_schema();
   db.load_schema(schema_name);
   // auto& table = db.get_schema().tables[0];
   // moderndbs::BufferManager buffer_manager(sysconf(_SC_PAGESIZE), 1000);
   // moderndbs::FSISegment fsi_segment(513, buffer_manager, table);
   // for (int i = 0; i < 8158; ++i) {
   //    fsi_segment.update(i, 0);
   // }
   // fsi_segment.update(8155, 4096 - 40);
   // auto s = fsi_segment.find(40);
   // if(s.has_value()) {
   //    cout << s.value() << endl;
   // }
   // moderndbs::OrderRecord order = {1, 1 * 2, 1 * 100, 1 % 5, (1 % 2 == 0 ? 'G' : 'H')};
   // auto transactionID = db.transaction_manager.startTransaction();
   // db.insert(table, order, transactionID);
   // db.transaction_manager.commitTransaction(transactionID);
   initialize_DB(db, num_records);
   // scan_DB(db, 0, 2);
   return 0;
}
