//
// Created by george-elfayoumi on 9/27/24.
//

#ifndef CUSTOMREADWRITERLOCK_H
#define CUSTOMREADWRITERLOCK_H

#include <atomic>
#include <condition_variable>
#include <thread>

namespace moderndbs {
class CustomReadWriteLock {
   public:
   enum class State { UNLOCKED = 0,
                      LOCKED_SHARED = 1,
                      LOCKED_EXCLUSIVE = 2 };

   // Acquire shared lock (multiple readers allowed)
   void lock_shared();

   // Release shared lock
   void unlock_shared();

   // Acquire exclusive lock (only one writer allowed)
   void lock_exclusive();

   // Release exclusive lock
   void unlock_exclusive();

   // private:
   std::atomic<State> state{State::UNLOCKED};
   std::atomic<int> readers_count{0};
   std::mutex mutex_;
   std::condition_variable cond_;
   std::atomic<bool> writer_waiting{false};
   std::atomic<int> writers_waiting{0};
   std::atomic<bool> writer_active{false};
};
}
#endif //CUSTOMREADWRITERLOCK_H
