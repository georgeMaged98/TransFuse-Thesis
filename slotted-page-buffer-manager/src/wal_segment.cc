//
// Created by george-elfayoumi on 10/23/24.
//
#include "moderndbs/file.h"
#include "moderndbs/segment.h"

#include <cstring>
#include <moderndbs/database.h>

using WALSegment = moderndbs::WALSegment;
using LogRecord = moderndbs::LogRecord;

uint64_t WALSegment::nextLSN() {
   return ++LSN;
}

WALSegment::WALSegment(uint16_t segment_id, BufferManager& buffer_manager)
   : Segment(segment_id, buffer_manager) {
   std::unique_lock lock(wal_mutex);

   std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(), File::Mode::WRITE);
   char latest_flushed_lsn_buffer[sizeof(uint64_t)];
   file->read_block(0, sizeof(uint64_t), latest_flushed_lsn_buffer);
   const auto& latest_flushed_LSN = *reinterpret_cast<uint64_t*>(latest_flushed_lsn_buffer);
   LSN = latest_flushed_LSN;
   flushedLSN = latest_flushed_LSN;
   /// Initialize txn_no with 0 unless it is initalized from WAL file, it will be modified inside the condition.
   latest_txn_no = 0;
   if (flushedLSN > 0) {
      /// sizeof(uint64_t) -> LSN COUNT
      auto wal_rec_size = sizeof(LogRecord);
      char last_record_buffer[wal_rec_size];
      size_t file_offset = sizeof(uint64_t) + latest_flushed_LSN * sizeof(LogRecord);
      file->read_block(file_offset, wal_rec_size, last_record_buffer);
      LogRecord record;
      memcpy(&record, last_record_buffer, wal_rec_size);
      latest_txn_no = record.transactionId;
   }
}

uint64_t WALSegment::appendRecord(const uint64_t transaction_id, const TransactionState state, OrderRecord* old_rec, OrderRecord* new_rec) {
   std::unique_lock lock(wal_mutex);
   auto lsn = nextLSN();
   auto log_record = LogRecord(state, lsn, transaction_id, old_rec, new_rec);
   records.push_back(log_record);
   printf("Append record - TransactionID: %lu, State:  %d, LSN: %lu\n", transaction_id, state, lsn);
   return log_record.lsn;
}

std::pair<char*, size_t> WALSegment::serialize() {
   uint64_t dataSize = records.size() * sizeof(LogRecord);
   char* data = new char[dataSize];
   std::memcpy(data, records.data(), dataSize);
   return {data, dataSize};
}

void WALSegment::append_to_WAL_Segment(const char* data, size_t data_size) {
   std::unique_ptr<File> file = File::open_file((std::to_string(segment_id) + ".txt").c_str(), File::Mode::WRITE);
   char* latest_flushed_lsn_buffer = new char[sizeof(uint64_t)];
   file->read_block(0, sizeof(uint64_t), latest_flushed_lsn_buffer);
   const auto& latest_flushed_LSN = *reinterpret_cast<uint64_t*>(latest_flushed_lsn_buffer);
   printf(" BEFORE APPENDING: LATEST FLUSHED LSN: %lu \n", latest_flushed_LSN);
   /// sizeof(uint64_t) -> LSN COUNT
   size_t file_offset = sizeof(uint64_t) + latest_flushed_LSN * sizeof(LogRecord);
   size_t cur_file_size = file->size();
   file->resize(cur_file_size + data_size);

   uint64_t num_records = data_size / sizeof(LogRecord);
   uint64_t new_lsn_count = latest_flushed_LSN + num_records;
   // Ensure that the new LSN count is written to the beginning of the file
   file->write_block(reinterpret_cast<const char*>(&new_lsn_count), 0, sizeof(new_lsn_count));

   printf(" LATEST FLUSHED LSN: %lu \n", new_lsn_count);
   file->write_block(data, file_offset, data_size);
}

void WALSegment::flushWal() {
   std::unique_lock lock(wal_mutex);
   std::pair<char*, size_t> walData = this->serialize();
   if (walData.second != 0) {
      append_to_WAL_Segment(walData.first, walData.second);
      records.clear();
      std::cout << "Flushed LSN " << this->LSN << "\n";
   }
}
