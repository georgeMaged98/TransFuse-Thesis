#include "moderndbs/database.h"

#include <cstring>

moderndbs::TID moderndbs::Database::insert(const schema::Table& table, OrderRecord& order, uint64_t transactionId) {
   const uint64_t lsn = wal_segment->appendRecord(transactionId, TransactionState::RUNNING, nullptr, &order);
   SPSegment& sp = *slotted_pages.at(table.sp_segment);
   auto* data = reinterpret_cast<std::byte*>(&order);
   auto order_size = sizeof(OrderRecord);
   auto tid = sp.allocate(order_size);
   // Write the data to the segment
   sp.write(tid, data, order_size, lsn);
   std::cout << "Tuple with TID " << tid.get_value() << " Page " << tid.get_page_id(0) << "  SLOT  " << tid.get_slot() << " inserted!\n";
   return tid;
}

void moderndbs::Database::update_tuple(const schema::Table& table, const TID tid, OrderRecord& order, uint64_t transactionId) {
   auto rec = read_tuple(table, tid, transactionId);
   const uint64_t lsn = wal_segment->appendRecord(transactionId, TransactionState::RUNNING, &rec.value(), &order);
   SPSegment& sp = *slotted_pages.at(table.sp_segment);
   auto* data = reinterpret_cast<std::byte*>(&order);
   auto order_size = sizeof(OrderRecord);
   // Write the data to the segment
   sp.write(tid, data, order_size,  lsn, true);
   std::cout << "Tuple with TID " << tid.get_value() << " updated!\n";
}

void moderndbs::Database::load_new_schema(std::unique_ptr<schema::Schema> schema) {
   if (schema_segment) {
      schema_segment->write();
   }
   // Always load it to segmentID 0, should be good enough for now
   schema_segment = std::make_unique<SchemaSegment>(0, schema_file_mapper);
   schema_segment->set_schema(std::move(schema));
   for (auto& table : schema_segment->get_schema()->tables) {
      free_space_inventory.emplace(table.fsi_segment, std::make_unique<FSISegment>(table.fsi_segment, fsi_file_mapper, table));
      slotted_pages.emplace(table.sp_segment, std::make_unique<SPSegment>(table.sp_segment, sp_file_mapper, *schema_segment, *free_space_inventory.at(table.fsi_segment), table));
   }
}

moderndbs::schema::Schema& moderndbs::Database::get_schema() {
   return *schema_segment->get_schema();
}

std::optional<moderndbs::OrderRecord> moderndbs::Database::read_tuple(const schema::Table& table, const TID tid, uint64_t transactionId) {
   auto& sps = *slotted_pages.at(table.sp_segment);
   // Prepare buffer for reading the data
   auto read_buffer = std::vector<std::byte>(sizeof(OrderRecord));
   // Read the data
   auto read_bytes = sps.read(tid, read_buffer.data(), read_buffer.size());
   if (!read_bytes.has_value() || read_bytes.value() < sizeof(OrderRecord)) {
      return std::nullopt;
   }

   // Deserialize the data into OrderRecord
   OrderRecord record;
   auto order_size = sizeof(OrderRecord);
   memcpy(&record, read_buffer.data(), order_size);
   std::cout << "Reading Tuple with TID " << tid.get_value() << " !\n";

   return record;
}

void moderndbs::Database::delete_tuple(const schema::Table& table, const TID tid, uint64_t transactionId) {
   auto rec = read_tuple(table, tid, transactionId);
   const uint64_t lsn = wal_segment->appendRecord(transactionId, TransactionState::RUNNING, &rec.value(), nullptr);
   SPSegment& sp = *slotted_pages.at(table.sp_segment);
   sp.erase(tid, lsn);
   std::cout << "Tuple with TID " << tid.get_value() << " deleted!\n";
}

void moderndbs::Database::load_schema(int16_t schema) {
   if (schema_segment) {
      schema_segment->write();
   }
   schema_segment = std::make_unique<SchemaSegment>(schema, schema_file_mapper);
   schema_segment->read();
   for (auto& table : schema_segment->get_schema()->tables) {
      free_space_inventory.emplace(table.fsi_segment, std::make_unique<FSISegment>(table.fsi_segment, fsi_file_mapper, table));
      slotted_pages.emplace(table.sp_segment, std::make_unique<SPSegment>(table.sp_segment, sp_file_mapper, *schema_segment, *free_space_inventory.at(table.fsi_segment), table));
   }
}
