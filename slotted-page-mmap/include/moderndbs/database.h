#ifndef INCLUDE_MODERNDBS_DATABASE_H_
#define INCLUDE_MODERNDBS_DATABASE_H_

#include "file_mapper.h"
#include "moderndbs/buffer_manager.h"
#include "moderndbs/schema.h"
#include "moderndbs/segment.h"
#include "moderndbs/transaction_manager.h"
#include "moderndbs/slotted_page.h"

#include <memory>

namespace moderndbs {
struct OrderRecord {
   uint64_t o_orderkey;
   uint64_t o_custkey;
   uint64_t o_totalprice;
   uint64_t o_shippriority;
   char o_orderstatus;
};

class Database {
   public:
   /// Constructor.
   Database() :
      // schema_file_mapper("schema_segment.txt", (sysconf(_SC_PAGESIZE))),
      //  fsi_file_mapper("fsi_segment.txt", (sysconf(_SC_PAGESIZE))),
      //  sp_file_mapper("sp_segment.txt", (sysconf(_SC_PAGESIZE))),
      //  wal_file_mapper("wal_segment.txt", (sysconf(_SC_PAGESIZE))),
       schema_file_mapper("/tmp/transfuse_mnt/schema_segment.txt", (sysconf(_SC_PAGESIZE))),
       fsi_file_mapper("/tmp/transfuse_mnt/fsi_segment.txt", (sysconf(_SC_PAGESIZE))),
       sp_file_mapper("/tmp/transfuse_mnt/sp_segment.txt", (sysconf(_SC_PAGESIZE))),
       wal_file_mapper("/tmp/transfuse_mnt/wal_segment.txt", (sysconf(_SC_PAGESIZE))),
       wal_segment(std::make_unique<WALSegment>(wal_file_mapper)),
       transaction_manager(*wal_segment){ // Pass by reference
}
/// Load a new schema
   void load_new_schema(std::unique_ptr<schema::Schema> schema);

   /// Load schema
   void load_schema(int16_t schema);

   /// Get the currently loaded schema
   schema::Schema& get_schema();

   /// Insert into a table
   TID insert(const schema::Table& table, OrderRecord& order, uint64_t transactionId);

   /// Read a tuple by TID from the table
   std::optional<OrderRecord> read_tuple(const schema::Table& table, TID tid, uint64_t transactionId);

   /// Delete a tuple by TID from the table
   void delete_tuple(const schema::Table& table, TID tid, uint64_t transactionId);

   /// Update a tuple by TID from the table
   void update_tuple(const schema::Table& table, TID tid, OrderRecord &order, uint64_t transactionId);

   // protected:
   /// The schema file mapper
   FileMapper schema_file_mapper;
   /// The fsi file mapper
   FileMapper fsi_file_mapper;
   /// The slotted pages file mapper
   FileMapper sp_file_mapper;
   /// The WAL file mapper
   FileMapper wal_file_mapper;
   /// The segment of the schema
   std::unique_ptr<SchemaSegment> schema_segment;
   /// The segment of the WAL
   std::unique_ptr<WALSegment> wal_segment;
   /// The segments of the schema's table's slotted pages
   std::unordered_map<int16_t, std::unique_ptr<SPSegment>> slotted_pages;
   /// The segment of the schema's free space inventory
   std::unordered_map<int16_t, std::unique_ptr<FSISegment>> free_space_inventory;
   /// Transaction Manager
   TransactionManager transaction_manager;

};

} // namespace moderndbs
#endif
