#include "moderndbs/database.h"
#include <cstring>

moderndbs::TID moderndbs::Database::insert(const schema::Table &table, OrderRecord &order) {
   // Allocate memory for the serialized data
   SPSegment &sp = *slotted_pages.at(table.sp_segment);
   std::byte* data = reinterpret_cast<std::byte*>(&order);
   auto order_size = sizeof(OrderRecord);
   auto tid = sp.allocate(order_size);
   // Write the data to the segment
   sp.write(tid, data, order_size);
   std::cout << "Tuple with TID " << tid.get_value() << " inserted!\n";
   return tid;
}


void moderndbs::Database::update_tuple(const schema::Table &table, const TID tid, OrderRecord &order) {
   // Allocate memory for the serialized data
   SPSegment &sp = *slotted_pages.at(table.sp_segment);
   auto* data = reinterpret_cast<std::byte*>(&order);
   auto order_size = sizeof(OrderRecord);
   // Write the data to the segment
   sp.write(tid, data, order_size, true);
   std::cout << "Tuple with TID " << tid.get_value() << " updated!\n";
}


void moderndbs::Database::load_new_schema(std::unique_ptr<schema::Schema> schema) {
    if (schema_segment) {
        schema_segment->write();
    }
    // Always load it to segmentID 0, should be good enough for now
    schema_segment = std::make_unique<SchemaSegment>(0, buffer_manager);
    schema_segment->set_schema(std::move(schema));
    for (auto &table: schema_segment->get_schema()->tables) {
        free_space_inventory.emplace(table.fsi_segment,
                                     std::make_unique<FSISegment>(table.fsi_segment, buffer_manager, table));
        slotted_pages.emplace(table.sp_segment,
                              std::make_unique<SPSegment>(table.sp_segment, buffer_manager, *schema_segment,
                                                          *free_space_inventory.at(table.fsi_segment), table));
    }
}

moderndbs::schema::Schema &moderndbs::Database::get_schema() {
    return *schema_segment->get_schema();
}

// std::optional<moderndbs::OrderRecord> moderndbs::Database::read_tuple(const moderndbs::schema::Table &table, const moderndbs::TID tid) {
    // auto &sps = *slotted_pages.at(table.sp_segment);
    // auto read_buffer = std::vector<char>(1024);
    // auto read_bytes = sps.read(tid, reinterpret_cast<std::byte *>(read_buffer.data()), read_buffer.size());
    // if (!read_bytes.has_value()) {
    //     return std::nullopt;
    // }
    //
    // auto values = std::vector<std::string>();
    // // Deserialize the data
    // std::cout << "TID " << tid.get_value() << " - Page: " << tid.get_page_id(table.sp_segment) << " - Slot: " << tid.get_slot() << "  :  ";
    // char *current = read_buffer.data();
    // for (auto &column: table.columns) {
    //     int integer;
    //     switch (column.type.tclass) {
    //         case schema::Type::Class::kInteger:
    //             integer = *reinterpret_cast<int *>(current);
    //             std::cout << integer;
    //             values.emplace_back(std::to_string(integer));
    //             current += sizeof(int);
    //             break;
    //         case schema::Type::Class::kChar:
    //             std::string str;
    //             for (size_t j = 0; j < column.type.length; ++j) {
    //                 std::cout << *current;
    //                 str += *current;
    //                 ++current;
    //             }
    //             values.emplace_back(str);
    //             break;
    //     }
    //     if (std::distance(read_buffer.data(), current) > read_bytes.value()) {
    //         break;
    //     }
    //     std::cout << " | ";
    // }
    // std::cout << "\n";
   // auto &sps = *slotted_pages.at(table.sp_segment);
   // auto read_buffer = std::vector<char>(sizeof(OrderRecord));  // Ensure buffer size matches OrderRecord
   // auto read_bytes = sps.read(tid, reinterpret_cast<std::byte *>(read_buffer.data()), read_buffer.size());
   //
   // // If read failed, return std::nullopt
   // if (!read_bytes.has_value()) {
   //    return std::nullopt;
   // }
   //
   // // Cast the buffer to an OrderRecord
   // const OrderRecord *record = reinterpret_cast<const OrderRecord *>(read_buffer.data());
   //
   // // Return a copy of the record
   // return *record;
   //  // return values;
// }

std::optional<moderndbs::OrderRecord> moderndbs::Database::read_tuple(const moderndbs::schema::Table &table, const moderndbs::TID tid) {
   auto &sps = *slotted_pages.at(table.sp_segment);
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
   // std::byte* current = read_buffer.data();

   return record;
}

void moderndbs::Database::delete_tuple(const schema::Table &table, const TID tid){
    SPSegment &sp = *slotted_pages.at(table.sp_segment);
    sp.erase(tid);
    std::cout << "Tuple with TID " << tid.get_value() << " deleted!\n";
}


void moderndbs::Database::load_schema(int16_t schema) {
    if (schema_segment) {
        schema_segment->write();
    }
    schema_segment = std::make_unique<SchemaSegment>(schema, buffer_manager);
    schema_segment->read();
    for (auto &table: schema_segment->get_schema()->tables) {
        free_space_inventory.emplace(table.fsi_segment,
                                     std::make_unique<FSISegment>(table.fsi_segment, buffer_manager, table));
        slotted_pages.emplace(table.sp_segment,
                              std::make_unique<SPSegment>(table.sp_segment, buffer_manager, *schema_segment,
                                                          *free_space_inventory.at(table.fsi_segment), table));
    }
}

