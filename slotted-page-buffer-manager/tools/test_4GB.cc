//
// Created by george-elfayoumi on 11/20/24.
//

#include <iostream>
#include <memory>
#include <ostream>
#include <moderndbs/database.h>

static int16_t sp_name = 4012;
static int16_t fsi_name = 4013;
static int16_t schema_name = 4014;
static int16_t wal_name = 4015;

static int BM_size = 1441792;

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
    moderndbs::BufferManager buffer_manager(4096, 1000);
    moderndbs::SchemaSegment schema_segment(schema_name, buffer_manager);
    schema_segment.set_schema(moderndbs::getTPCHOrderSchema());
    schema_segment.write();
}

void initialize_DB(moderndbs::Database &db, const uint64_t records_count) {
    auto &table = db.get_schema().tables[0];
    const uint64_t progress_step = records_count / 10; // Calculate step for 10% progress
    uint64_t next_progress = progress_step;

    for (uint64_t i = 0; i < records_count; ++i) {
        moderndbs::OrderRecord order = {i, i * 2, i * 100, i % 5, (i % 2 == 0 ? 'G' : 'H')};
        auto transactionID = db.transaction_manager.startTransaction();
        db.insert(table, order, transactionID);
        db.transaction_manager.commitTransaction(transactionID);
        if (i + 1 == next_progress || i + 1 == records_count) { // Ensure progress is printed for the last record
            std::cout << "Progress: " << (100 * (i + 1) / records_count) << "% completed.\n";
            next_progress += progress_step;
        }
    }
}

void scan_DB(moderndbs::Database &db, const uint64_t start_page, const uint64_t end_page) {
    std::cout << " Scan: start_page " << start_page << " end_page " << end_page << std::endl;
    auto &table = db.get_schema().tables[0];
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
    cout << "Testing 4GB" << endl;
    uint64_t num_records = 83886080;
    auto db = moderndbs::Database(BM_size, wal_name);
    initialize_schema();
    db.load_schema(schema_name);
    initialize_DB(db, num_records);
    return 0;
}
