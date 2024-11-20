//
// Created by george-elfayoumi on 10/23/24.
//

#include <iostream>
#include <moderndbs/transaction_manager.h>
#include "moderndbs/segment.h"


using TransactionManager = moderndbs::TransactionManager;

TransactionManager::TransactionManager(WALSegment& wal_segment) : transactionCounter(0), wal_segment(wal_segment) {
   transactionCounter = wal_segment.latest_txn_no;
}

int TransactionManager::startTransaction() {
   auto new_txn_id =  ++transactionCounter;
   wal_segment.appendRecord(new_txn_id, TransactionState::BEGIN, nullptr, nullptr);
   return new_txn_id;
}

void TransactionManager::commitTransaction(int transactionID) {
   wal_segment.appendRecord(transactionID, TransactionState::COMMIT, nullptr, nullptr);
   //wal_segment.flushWal();
   // std::cout << "Transaction " << transactionID << " committed.\n";
}