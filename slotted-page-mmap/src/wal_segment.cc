//
// Created by george-elfayoumi on 10/23/24.
//
#include "moderndbs/segment.h"
#include <moderndbs/database.h>

using WALSegment = moderndbs::WALSegment;
using LogRecord = moderndbs::LogRecord;


uint64_t WALSegment::nextLSN() {
   return ++LSN;
}

WALSegment::WALSegment(FileMapper &file_mapper)
        : Segment(101, file_mapper){
   std::unique_lock lock(wal_mutex);
   std::shared_ptr<Page> page = file_mapper.get_page(0, false);
   auto &latest_flushed_LSN = *reinterpret_cast<uint64_t *>(page->get_data());
   LSN =  latest_flushed_LSN;
   flushedLSN = latest_flushed_LSN;
   /// Initialize txn_no with 0 unless it is initalized from WAL file, it will be modified inside the condition.
   latest_txn_no = 0;
   if(flushedLSN > 0) {
      /// sizeof(uint64_t) -> LSN COUNT
      char* last_record = page->get_data() + sizeof(uint64_t) + (latest_flushed_LSN - 1) * sizeof(LogRecord);
      LogRecord record;
      auto wal_rec_size = sizeof(LogRecord);
      memcpy(&record, last_record, wal_rec_size);
      latest_txn_no = record.transactionId;
   }
   file_mapper.release_page(page);

   flush_thread = std::thread(&WALSegment::runEveryXSeconds, this, flush_interval_ms, std::ref(stop_background_flush));

}

uint64_t WALSegment::appendRecord(const uint64_t transaction_id, const TransactionState state, OrderRecord* old_rec, OrderRecord* new_rec) {
   std::unique_lock lock(wal_mutex);
   auto lsn = nextLSN();
   auto log_record = LogRecord(state, lsn, transaction_id, old_rec, new_rec);
   records.push_back(log_record);
   // printf("Append record - TransactionID: %lu, State:  %d, LSN: %lu\n", transaction_id, state, lsn);
   return log_record.lsn;
}

std::pair<std::vector<char>, size_t> WALSegment::serialize() {
   uint64_t dataSize = records.size() * sizeof(LogRecord);
   std::vector<char> data(dataSize);
   std::memcpy(data.data(), records.data(), dataSize);
   return {std::move(data), dataSize};
}


void WALSegment::flushWal() {
   std::unique_lock lock(wal_mutex);
   auto walData = this->serialize();
   if (walData.second != 0) {
      file_mapper.append_to_wal_file(walData.first.data(), walData.second);
      records.clear();
   }
}

