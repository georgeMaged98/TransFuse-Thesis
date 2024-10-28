//
// Created by george-elfayoumi on 10/23/24.
//

#ifndef TRANSACTIONMANAGER_H
#define TRANSACTIONMANAGER_H
#include <cstdint>

namespace moderndbs {
class WALSegment;

enum class TransactionState : uint8_t {
   BEGIN,
   RUNNING,
   COMMIT,
   ABORT
};

class TransactionManager {
   private:
   uint64_t transactionCounter;
   WALSegment& wal_segment;

   public:

   explicit TransactionManager(WALSegment& wal_segment);

   int startTransaction();

   void commitTransaction(int transactionID);
};
}

#endif //TRANSACTIONMANAGER_H
