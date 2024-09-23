#include "moderndbs/database.h"

moderndbs::TID moderndbs::Database::insert(const schema::Table &table,
                                                          const std::vector<std::string> &data) {
    if (table.columns.size() != data.size()) {
        throw std::runtime_error("invalid data");
    }
    // Serialize the data
    auto insert_buffer = std::vector<char>();
    for (size_t i = 0; i < data.size(); ++i) {
        const auto &column = table.columns[i];
        const auto &s = data[i];

        int integer = 0;
        switch (column.type.tclass) {
            case schema::Type::Class::kInteger:
                integer = atoi(s.c_str()); // NOLINT
                for (size_t j = 0; j < sizeof(integer); ++j) {
                    insert_buffer.push_back(reinterpret_cast<char *>(&integer)[j]);
                }
                break;
            case schema::Type::Class::kChar:
                for (size_t j = 0; j < column.type.length; ++j) {
                    if (j < s.size()) {
                        insert_buffer.push_back(s[j]);
                    } else {
                        insert_buffer.push_back(' ');
                    }
                }
                break;
        }
    }

    SPSegment &sp = *slotted_pages.at(table.sp_segment);
    auto tid = sp.allocate(insert_buffer.size());
    sp.write(tid, reinterpret_cast<std::byte *>(insert_buffer.data()), insert_buffer.size());
    std::cout << "Tuple with TID " << tid.get_value()  << " Page id: " << tid.get_page_id(table.sp_segment) <<  " Slot: " << tid.get_slot() << " inserted!\n";
    return tid;
}

void moderndbs::Database::update_tuple(const schema::Table &table, const TID tid,
                                       const std::vector<std::string> &data) {
    if (table.columns.size() != data.size()) {
        throw std::runtime_error("invalid data");
    }

    // Serialize the data
    auto insert_buffer = std::vector<char>();
    for (size_t i = 0; i < data.size(); ++i) {
        const auto &column = table.columns[i];
        const auto &s = data[i];

        int integer = 0;
        switch (column.type.tclass) {
            case schema::Type::Class::kInteger:
                integer = atoi(s.c_str()); // NOLINT
                for (size_t j = 0; j < sizeof(integer); ++j) {
                    insert_buffer.push_back(reinterpret_cast<char *>(&integer)[j]);
                }
                break;
            case schema::Type::Class::kChar:
                for (size_t j = 0; j < column.type.length; ++j) {
                    if (j < s.size()) {
                        insert_buffer.push_back(s[j]);
                    } else {
                        insert_buffer.push_back(' ');
                    }
                }
                break;
        }
    }

    SPSegment &sp = *slotted_pages.at(table.sp_segment);
    sp.write(tid, reinterpret_cast<std::byte *>(insert_buffer.data()), insert_buffer.size(), true);
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

std::optional<std::vector<std::string>> moderndbs::Database::read_tuple(const moderndbs::schema::Table &table, const moderndbs::TID tid) {
    auto &sps = *slotted_pages.at(table.sp_segment);
    auto read_buffer = std::vector<char>(1024);
    auto read_bytes = sps.read(tid, reinterpret_cast<std::byte *>(read_buffer.data()), read_buffer.size());
    if (!read_bytes.has_value()) {
        return std::nullopt;
    }

    auto values = std::vector<std::string>();
    // Deserialize the data
    std::cout << "TID " << tid.get_value() << " - Page: " << tid.get_page_id(table.sp_segment) << " - Slot: " << tid.get_slot() << "  :  ";
    char *current = read_buffer.data();
    for (auto &column: table.columns) {
        int integer;
        switch (column.type.tclass) {
            case schema::Type::Class::kInteger:
                integer = *reinterpret_cast<int *>(current);
                std::cout << integer;
                values.emplace_back(std::to_string(integer));
                current += sizeof(int);
                break;
            case schema::Type::Class::kChar:
                std::string str;
                for (size_t j = 0; j < column.type.length; ++j) {
                    std::cout << *current;
                    str += *current;
                    ++current;
                }
                values.emplace_back(str);
                break;
        }
        if (std::distance(read_buffer.data(), current) > read_bytes.value()) {
            break;
        }
        std::cout << " | ";
    }
    std::cout << "\n";
    return values;
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

