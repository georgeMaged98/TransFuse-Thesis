#include "moderndbs/schema.h"
#include "moderndbs/segment.h"
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include <algorithm>
#include <cstring>
#include <sstream>

using Segment = moderndbs::Segment;
using SchemaSegment = moderndbs::SchemaSegment;
using Schema = moderndbs::schema::Schema;
using Type = moderndbs::schema::Type;
using Table = moderndbs::schema::Table;
using Column = moderndbs::schema::Column;

namespace json = rapidjson;

namespace {

struct StreamBuffer: std::streambuf {
   explicit StreamBuffer(std::vector<std::byte> &buffer) {
      auto* begin = reinterpret_cast<char*>(buffer.data());
      auto* end = reinterpret_cast<char*>(buffer.data() + buffer.size());
      this->setg(begin, begin, end);
   }
};

// NOLINTNEXTLINE
const std::unordered_map<std::string, Type::Class> types {
   { "char", Type::kChar },
   { "integer", Type::kInteger },
};

}  // namespace

SchemaSegment::SchemaSegment(
   uint16_t segment_id, FileMapper& file_mapper)
   : Segment(segment_id, file_mapper) {
}

SchemaSegment::~SchemaSegment() {
   write(); // NOLINT
}

void SchemaSegment::read() {
   // Load the first page
   // auto& page = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, false);
   auto page = file_mapper.get_page(0, false);
   auto page_size = file_mapper.get_data_size();

   // [0-8[   : Schema string length in #bytes
   auto schema_size = *reinterpret_cast<uint64_t*>(page->get_data());

   // Read schema string into buffer
   auto remaining_bytes = schema_size;
   std::vector<std::byte> buffer;
   buffer.resize(schema_size);
   auto buffer_offset = 0;

   // Read the remainder of the first page
   auto d = std::min<size_t>(remaining_bytes, page_size - 25);
   std::memcpy(&buffer[buffer_offset], page->get_data() + sizeof(uint64_t), d);
   buffer_offset += d;
   remaining_bytes -= d;

   // Release first page
   // buffer_manager.unfix_page(page, false);
   file_mapper.release_page(page);

   // Read all other schema pages
   for (int pid = 1; remaining_bytes > 0; pid++) {
      // auto &page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ pid, false);
      page = file_mapper.get_page(pid, false);
      auto n = std::min<size_t>(remaining_bytes, page_size);
      std::memcpy(&buffer[buffer_offset], page->get_data(), n);
      buffer_offset += n;
      remaining_bytes -= n;
      file_mapper.release_page(page);
      // buffer_manager.unfix_page(page, false);
   }

   // Parse the schema
   json::Document document;
   StreamBuffer sb(buffer);
   std::istream sbi(&sb);
   json::IStreamWrapper isw(sbi);
   document.ParseStream(isw);
   if (document.HasParseError()) {
      std::cerr << "JSON parsing error: " << document.GetParseError() << std::endl;
      return;
   }

   // Parse the schema
   std::vector<Table> tables;
   if (document.HasMember("tables") && document["tables"].IsArray()) {
      for (auto &table : document["tables"].GetArray()) {
         const auto* id = table.HasMember("id") ? table["id"].GetString() : "?";
         auto sp_segment = table.HasMember("sp_segment.txt") ? table["sp_segment.txt"].GetInt() : -1;
         auto fsi_segment = table.HasMember("fsi_segment.txt") ? table["fsi_segment.txt"].GetInt() : -1;
         auto allocated_pages = table.HasMember("allocated_pages") ? table["allocated_pages"].GetInt() : -1;
         std::vector<Column> columns;
         if (table.HasMember("columns") && table["columns"].IsArray()) {
            for (auto &col : table["columns"].GetArray()) {
               std::string id = col.HasMember("id") ? col["id"].GetString() : "?";
               Type t;
               if (col.HasMember("type")) {
                  auto type = col["type"].GetObject();
                  t.length = type.HasMember("length") ? type["length"].GetInt() : 0;
                  t.tclass = Type::Class::kInteger;
                  if (type.HasMember("tclass")) {
                     auto iter = types.find(type["tclass"].GetString());
                     if (iter != types.end()) {
                        t.tclass = iter->second;
                     }
                  }
               }
               columns.emplace_back(id, t);
            }
         }
         std::vector<std::string> primary_key;
         if (table.HasMember("primary_key") && table["primary_key"].IsArray()) {
            for (auto &pk : table["primary_key"].GetArray()) {
               primary_key.emplace_back(pk.GetString());
            }
         }
         tables.emplace_back(id, std::move(columns), std::move(primary_key), sp_segment, fsi_segment, allocated_pages);
      }
   }
   schema = std::make_unique<Schema>(std::move(tables));
}

void SchemaSegment::write() {
   // Load the first page
   // auto& page = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, true);
   auto page = file_mapper.get_page(0, true);
   auto page_size = file_mapper.get_data_size();

   // [0-8[   : Schema string length in #bytes
   if (!schema) {
      *reinterpret_cast<uint64_t*>(page->get_data()) = 0;
      // buffer_manager.unfix_page(page, true);
      file_mapper.release_page(page);
      return;
   }

   // Serialize the schema to json
   json::StringBuffer buffer;
   {
      // Prepare document
      json::Document doc(json::kObjectType);
      auto &allocator = doc.GetAllocator();

      // Write tables
      json::Value tables(json::kArrayType);
      for (auto &table : schema->tables) {
         json::Value t(json::kObjectType);

         // id
         t.AddMember("id", json::StringRef(table.id.c_str()), allocator);
         // sp_segment
         t.AddMember("sp_segment.txt", table.sp_segment, allocator);
         // fsi_segment
         t.AddMember("fsi_segment.txt", table.fsi_segment, allocator);
         // allocated_pages
         t.AddMember("allocated_pages", table.allocated_pages, allocator);

         // Write columns
         json::Value columns(json::kArrayType);
         for (const auto &col: table.columns) {
            // id
            json::Value column(json::kObjectType);
            column.AddMember("id", json::StringRef(col.id.c_str()), allocator);

            // tclass
            json::Value type(json::kObjectType);
            type.AddMember("tclass", json::StringRef(col.type.name()), allocator);

            // length
            type.AddMember("length", col.type.length, allocator);

            column.AddMember("type", type, allocator);
            columns.PushBack(column, allocator);
         }
         t.AddMember("columns", columns, allocator);

         // Write primary key
         json::Value primary_key(json::kArrayType);
         for (const auto &pk: table.primary_key) {
            primary_key.PushBack(json::StringRef(pk.c_str()), allocator);
         }
         t.AddMember("primary_key", primary_key, allocator);

         tables.PushBack(t, allocator);
      }
      doc.AddMember("tables", tables, allocator);

      // Write into buffer
      json::Writer<json::StringBuffer> writer(buffer);
      doc.Accept(writer);
   }

   // Get data
   const char *json_str = buffer.GetString();
   uint64_t schema_size = buffer.GetSize();
   // Set first 8 bytes in data to be schema size
   *reinterpret_cast<uint64_t*>(page->get_data()) = schema_size;

   auto remaining_bytes = schema_size;
   auto buffer_offset = 0;

   // Write the remainder of the first page
   auto d = std::min<size_t>(remaining_bytes, page_size - 25);
   std::memcpy(page->get_data() + sizeof(uint64_t), json_str + buffer_offset, d);
   buffer_offset += d;
   remaining_bytes -= d;

   // Release first page
   // buffer_manager.unfix_page(page, true);
   file_mapper.release_page(page);

   // Write all other schema pages
   for (int pid = 1; remaining_bytes > 0; pid++) {
      // auto &page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ pid, true);
      page = file_mapper.get_page(pid, true);
      auto n = std::min<size_t>(remaining_bytes, page_size);
      std::memcpy(page->get_data(), json_str + buffer_offset, n);

      buffer_offset += n;
      remaining_bytes -= n;
      // buffer_manager.unfix_page(page, true);
      file_mapper.release_page(page);
   }
}
