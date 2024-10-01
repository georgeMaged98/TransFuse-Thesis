//
// Created by george-elfayoumi on 9/27/24.
//
#include <iostream>
#include <mutex>
#include <moderndbs/custom_read_writer_lock.h>

void moderndbs::CustomReadWriteLock::lock_shared() {
   // std::cout << " ------------------------------------------------------LOCKING SHARED------------------------------------------------------ \n";
   std::unique_lock<std::mutex> lock(mutex_);
   cond_.wait(lock, [this]() {
      // std::cout <<  std::this_thread::get_id() << " CUR STATE " << static_cast<int>(state.load()) << " readers COunt " << readers_count.load() << " writer waiting " << writers_waiting.load() << " is writer activr: " << writer_active.load() << " looooooooooooooooooooooooooooop SHAREDD \n";
      // return state.load() != State::LOCKED_EXCLUSIVE && writers_waiting.load() == 0 && !writer_active.load();
      return state.load() != State::LOCKED_EXCLUSIVE /* && readers_count.load() > 0 */ && !writer_active.load() /*&& writers_waiting.load() == 0*/;
   });
   if (readers_count.load() == 0) {
      state.store(State::LOCKED_SHARED);
   }
   readers_count.fetch_add(1);
   // std::cout << " END Locking Shared: Readers: " << readers_count.load() << "  State: " << static_cast<int>(state.load()) << "\n";
}


// Release shared lock
void moderndbs::CustomReadWriteLock::unlock_shared() {
   // std::cout << " ------------------------------------------------------UNLOCKING SHARED------------------------------------------------------ \n";
   std::unique_lock<std::mutex> lock(mutex_);
   if (readers_count.load() == 0) {
      throw std::runtime_error("Unlock shared called when no readers are holding the lock!");
   }

   readers_count.fetch_sub(1);
   if (readers_count.load() == 0) {
      state.store(State::UNLOCKED);
      // std::cout << "Last reader released lock. Transitioning to UNLOCKED\n";
      cond_.notify_all(); // Notify waiting writers
   }
   // std::cout << " END UNLocking Shared: Readers: " << readers_count.load() << "  State: " << static_cast<int>(state.load()) << "\n";
}

// Acquire exclusive lock (only one writer allowed)
void moderndbs::CustomReadWriteLock::lock_exclusive() {
   // std::cout << " ------------------------------------------------------LOCKING EXC------------------------------------------------------ \n";
   std::unique_lock<std::mutex> lock(mutex_);
   // std::cout << std::this_thread::get_id() << "   exclusive lock. " << static_cast<int>(state.load()) << "   "<< readers_count.load() << "\n";

   writers_waiting.fetch_add(1);
   cond_.wait(lock, [this]() {
      // std::cout <<  std::this_thread::get_id() << " exc looooooooooooooooooooooooooooop " <<  static_cast<int>(state.load()) << " Readers COunt " << readers_count.load() << "\n";
      return state.load() == State::UNLOCKED && readers_count.load() == 0 && !writer_active.load(); // Wait until no readers or writers
   });
   writers_waiting.fetch_sub(1);
   state.store(State::LOCKED_EXCLUSIVE);
   writer_active.store(true);
   // std::cout << "END Acquire EXC lock. Readers: " << readers_count.load() << "  State: " << static_cast<int>(state.load()) << "\n";;
}

// Release exclusive lock
void moderndbs::CustomReadWriteLock::unlock_exclusive() {
   // std::cout << " ------------------------------------------------------UNLOCKING EXC------------------------------------------------------ \n";
   std::unique_lock<std::mutex> lock(mutex_);
   state.store(State::UNLOCKED);
   writer_active.store(false);
   cond_.notify_all();
   // std::cout << " END UNLocking EXC: Readers: " << readers_count.load() << "  State: " << static_cast<int>(state.load()) << " WRITER ACTIVE " << writer_active.load() << " WAITING WRITERS " << writers_waiting.load()  << "\n";

}