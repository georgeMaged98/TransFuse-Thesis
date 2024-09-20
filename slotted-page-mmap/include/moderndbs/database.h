#ifndef INCLUDE_MODERNDBS_DATABASE_H_
#define INCLUDE_MODERNDBS_DATABASE_H_

#include "file_mapper.h"
#include "moderndbs/buffer_manager.h"
#include "moderndbs/schema.h"
#include "segment.h"
#include <memory>

namespace moderndbs {

    class Database {
    public:
        /// Constructor.
        Database(): schema_file_mapper("schema_segment.txt", 1024), fsi_file_mapper("fsi_segment.txt", 1024), sp_file_mapper("sp_segment.txt", 1024) {}

        /// Load a new schema
        void load_new_schema(std::unique_ptr<schema::Schema> schema);

        /// Load schema
        void load_schema(int16_t schema);

        /// Get the currently loaded schema
        schema::Schema &get_schema();

        /// Insert into a table
        TID insert(const schema::Table &table, const std::vector<std::string> &data);

        /// Read a tuple by TID from the table
        std::optional<std::vector<std::string>> read_tuple(const schema::Table &table, TID tid);

        /// Delete a tuple by TID from the table
        void delete_tuple(const schema::Table &table, TID tid);

        /// Update a tuple by TID from the table
        void update_tuple(const schema::Table &table, TID tid, const std::vector<std::string> &data);

    protected:
        /// The schema file mapper
        FileMapper schema_file_mapper;
        /// The fsi file mapper
        FileMapper fsi_file_mapper;
        /// The slotted pages file mapper
        FileMapper sp_file_mapper;
        /// The segment of the schema
        std::unique_ptr<SchemaSegment> schema_segment;
        /// The segments of the schema's table's slotted pages
        std::unordered_map<int16_t, std::unique_ptr<SPSegment>> slotted_pages;
        /// The segment of the schema's free space inventory
        std::unordered_map<int16_t, std::unique_ptr<FSISegment>> free_space_inventory;
        std::shared_mutex latch;
    };

} // namespace moderndbs
#endif
