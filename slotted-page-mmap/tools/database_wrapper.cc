#include "moderndbs/database.h"
#include "moderndbs/file.h"

#include <barrier>
#include <cassert>
#include <cstring>
#include <iostream>
#include <random>
#include <unordered_set>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace moderndbs {
std::unique_ptr<schema::Schema> getTPCHSchemaLight() {
   std::vector<schema::Table> tables{
      schema::Table(
         "customer",
         {
            schema::Column("c_custkey", schema::Type::Integer()),
            schema::Column("c_name", schema::Type::Char(25)),
            schema::Column("c_address", schema::Type::Char(40)),
            schema::Column("c_nationkey", schema::Type::Integer()),
            schema::Column("c_phone", schema::Type::Char(15)),
            schema::Column("c_acctbal", schema::Type::Integer()),
            schema::Column("c_mktsegment", schema::Type::Char(10)),
            schema::Column("c_comment", schema::Type::Char(117)),
         },
         {"c_custkey"},
         10, 11),
      schema::Table(
         "nation",
         {
            schema::Column("n_nationkey", schema::Type::Integer()),
            schema::Column("n_name", schema::Type::Char(25)),
            schema::Column("n_regionkey", schema::Type::Integer()),
            schema::Column("n_comment", schema::Type::Char(152)),
         },
         {"n_nationkey"},
         20, 21),
      schema::Table(
         "region",
         {
            schema::Column("r_regionkey", schema::Type::Integer()),
            schema::Column("r_name", schema::Type::Char(25)),
            schema::Column("r_comment", schema::Type::Char(152)),
         },
         {"r_regionkey"},
         30, 31),
   };
   auto schema = std::make_unique<schema::Schema>(std::move(tables));
   return schema;
}

std::unique_ptr<schema::Schema> getTPCHOrderSchema() {
   std::vector<schema::Table> tables{
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
         50, 51,
         0)};
   auto schema = std::make_unique<schema::Schema>(std::move(tables));
   return schema;
}

}

template <typename T>
void readLine(T& v);

template <>
void readLine(std::string& v) {
   std::getline(std::cin, v);
}

template <>
void readLine(int& v) {
   std::string line;
   std::getline(std::cin, line);
   try {
      v = std::stoi(line);
   } catch (...) {}
}

// void listenForErrors() {
//    int server_sock, client_sock;
//    struct sockaddr_un addr;
//    char buffer[128];
//
//    server_sock = socket(AF_UNIX, SOCK_STREAM, 0);
//    addr.sun_family = AF_UNIX;
//    strcpy(addr.sun_path, "/tmp/db_socket");
//    bind(server_sock, (struct sockaddr*)&addr, sizeof(addr));
//    listen(server_sock, 5);
//
//    // client_sock = accept(server_sock, nullptr, nullptr);
//    // read(client_sock, buffer, sizeof(buffer));
//    // std::cerr << "Error from libfuse: " << buffer << std::endl;
//    //
//    // close(client_sock);
//    while (true) {  // Keep listening indefinitely
//       // Accept a connection from a client
//       client_sock = accept(server_sock, nullptr, nullptr);
//       if (client_sock < 0) {
//          std::cerr << "Failed to accept connection: " << strerror(errno) << std::endl;
//          continue;  // Skip to the next iteration if an error occurs
//       }
//
//       // Read error message from the client
//       ssize_t bytes_read = read(client_sock, buffer, sizeof(buffer) - 1);
//       if (bytes_read > 0) {
//          buffer[bytes_read] = '\0';  // Null-terminate the string
//          std::cerr << "Error from libfuse: " << buffer << std::endl;
//       } else {
//          std::cerr << "Failed to read error message: " << strerror(errno) << std::endl;
//       }
//
//       // Close the client socket after reading
//       close(client_sock);
//    }
//
//    close(server_sock);
// }

std::condition_variable cv;
std::mutex cv_m;
bool is_server_ready = false;

unordered_set<uint64_t> fsi_pages;
unordered_set<uint64_t> schema_pages;
unordered_set<uint64_t> sp_pages;

// Global atomic flag to control the error listener thread
std::atomic<bool> should_stop_error_listener{false};

void listenForErrors() {
   int server_sock, client_sock;
   struct sockaddr_un addr;
   char buffer[128];
   const char* socket_path = "/tmp/db_socket";

   // Check if the socket file already exists
   if (access(socket_path, F_OK) == 0) {
      // Remove the socket file
      if (unlink(socket_path) == -1) {
         perror("Failed to remove existing socket file");
         exit(EXIT_FAILURE);
      }
   }

   // Create the socket
   server_sock = socket(AF_UNIX, SOCK_STREAM, 0);
   if (server_sock < 0) {
      std::cerr << "Failed to create socket: " << strerror(errno) << std::endl;
      return;
   }

   // Setup socket address
   addr.sun_family = AF_UNIX;
   strcpy(addr.sun_path, socket_path);

   // Bind the socket
   if (bind(server_sock, (struct sockaddr*) &addr, sizeof(addr)) < 0) {
      std::cerr << "Failed to bind socket: " << strerror(errno) << std::endl;
      close(server_sock);
      return;
   }

   std::cout << "Socket bound to " << socket_path << ".\n";

   // Start listening for incoming connections
   if (listen(server_sock, 5) < 0) {
      std::cerr << "Failed to listen on socket: " << strerror(errno) << std::endl;
      close(server_sock);
      return;
   }

   // Set the server socket to non-blocking
   int flags = fcntl(server_sock, F_GETFL, 0);
   if (flags == -1) {
      std::cerr << "Failed to get socket flags: " << strerror(errno) << std::endl;
      close(server_sock);
      return;
   }
   if (fcntl(server_sock, F_SETFL, flags | O_NONBLOCK) == -1) {
      std::cerr << "Failed to set socket to non-blocking: " << strerror(errno) << std::endl;
      close(server_sock);
      return;
   }

   std::cout << "Listening for errors... (Press Ctrl+C to stop)" << std::endl;

   // Notify that the server is ready to accept connections
   {
      std::lock_guard<std::mutex> lk(cv_m);
      is_server_ready = true;
   }
   cv.notify_all(); // Notify waiting threads


   int count = 0;
   while (true) { // Keep listening indefinitely
      // Accept a connection from a client
      client_sock = accept(server_sock, nullptr, nullptr);
      if (client_sock < 0) {
         // Check if the error is due to no pending connections
         if (errno == EAGAIN || errno == EWOULDBLOCK) {
            // No connections available, continue to the next iteration
            continue;
         }
         std::cerr << "Failed to accept connection: " << strerror(errno) << std::endl;
         continue; // Skip to the next iteration if an error occurs
      }
      std::cout << "Connection accepted.\n";

      // Read error message from the client
      ssize_t bytes_read = read(client_sock, buffer, sizeof(buffer) - 1);
      if (bytes_read < 0) {
         std::cerr << "Failed to read error message: " << strerror(errno) << std::endl;
      } else if (bytes_read == 0) {
         std::cerr << "Client disconnected unexpectedly." << std::endl;
      } else {
         count++;
         std::cout << "Current Count " << count << "\n";
         buffer[bytes_read] = '\0'; // Null-terminate the string
         std::cerr << "Error from libfuse: " << buffer << "\n";

         // Extracting segment and page_number
         std::string error_message(buffer);
         size_t hyphen_pos = error_message.find('-');
         std::string before_hyphen = error_message.substr(0, hyphen_pos);
         std::string after_hyphen = error_message.substr(hyphen_pos + 1);
         if (before_hyphen == "/sp_segment.txt") {
            sp_pages.insert(std::stoi(after_hyphen));
         } else if (before_hyphen == "/fsi_segment.txt") {
            fsi_pages.insert(std::stoi(after_hyphen));
         } else {
            schema_pages.insert(std::stoi(after_hyphen));
         }
      }

      // Close the client socket after reading
      close(client_sock);
   }

   // Cleanup (unreachable in this infinite loop)
   close(server_sock);
}


int main() {
   // std::cout << "Setting memory limit..." << std::endl;
   //
   // struct rlimit limit;
   // limit.rlim_cur = 512 * 1024 * 1024; // Set current limit to 256 MB
   // limit.rlim_max = 512 * 1024 * 1024; // Set maximum limit to 256 MB
   //
   // if (setrlimit(RLIMIT_AS, &limit) == -1) {
   //    std::cerr << "Failed to set memory limit: " << strerror(errno) << std::endl;
   //    return 1; // Return a non-zero value to indicate failure
   // }
   //
   // std::cout << "Memory limit set successfully." << std::endl;
   //
   // struct rlimit new_limit;
   // if (getrlimit(RLIMIT_AS, &new_limit) == 0) {
   //    std::cout << "Current limit: " << new_limit.rlim_cur << " MB\n";
   // } else {
   //    std::cerr << "Failed to get memory limit: " << strerror(errno) << std::endl;
   // }


   // Create the errorListener thread before joining the other threads
   std::thread errorListener(listenForErrors);

   {
      std::unique_lock<std::mutex> lk(cv_m);
      cv.wait(lk, [] { return is_server_ready; });
   }


   using moderndbs::File;
   for (const auto* segment_file : std::vector<const char*>{"/tmp/transfuse_mnt/fsi_segment.txt", "/tmp/transfuse_mnt/sp_segment.txt", "/tmp/transfuse_mnt/schema_segment.txt"}) {
      auto file = File::open_file(segment_file, File::Mode::WRITE);
      file->resize(0);
   }

   auto db = moderndbs::Database();
   {
      moderndbs::FileMapper schema_file_mapper("/tmp/transfuse_mnt/schema_segment.txt", (sysconf(_SC_PAGESIZE)));
      moderndbs::SchemaSegment schema_segment(49, schema_file_mapper);
      schema_segment.set_schema(moderndbs::getTPCHOrderSchema());
      schema_segment.write();
   }
   db.load_schema(49);
   auto& table = db.get_schema().tables[0];

   /// INSERTIONS -> INSERT OrderRecords for 20 pages.
   for (uint64_t i = 0; i < 1600; ++i) {
      moderndbs::OrderRecord order = {i, i * 2, i * 100, i % 5, (i % 2 == 0 ? 'G' : 'H')};
      db.insert(table, order);
   }

   // Pre-allocate the tids vector to the correct size
   std::vector<moderndbs::TID> tids;
   uint32_t insertions_per_thread = 100;
   tids.reserve(4 * insertions_per_thread); // 4 threads

   std::vector<std::thread> threads;
   std::vector<std::vector<moderndbs::TID>> tids_per_thread(4);

   for (size_t thread = 0; thread < 10; ++thread) {
      threads.emplace_back([thread, &table, &db, &tids_per_thread, insertions_per_thread] {
         std::mt19937_64 engine{thread};
         // 5% of queries are scans.
         std::bernoulli_distribution scan_distr{0.05};
         // Number of pages accessed by a point query is geometrically distributed.
         std::geometric_distribution<size_t> num_pages_distr{0.5};
         // 60% of point queries are reads.
         std::bernoulli_distribution reads_distr{0.6};

         // Pages and Slots
         // Out of 20 accesses, 10 are from page 0, 4 from page 1, 2 from page 2, 1 from page 3, and 3 from page 4.
         std::uniform_int_distribution<uint16_t> page_distr{0, 19};
         std::uniform_int_distribution<uint16_t> slot_distr{0, 79};

         for (size_t j = 0; j < 400; ++j) {
            if (scan_distr(engine)) {
               /// READ two full segments
               auto start_page = page_distr(engine);
               auto end_page = start_page + 2;
               for (uint16_t pg = start_page; pg < end_page && pg < 20; ++pg) {
                  for (uint16_t sl = 0; sl < 79; ++sl) {
                     std::cout << " Page: " << pg << " SLOT: " << sl << " \n";
                     moderndbs::TID tid{pg, sl};
                     std::cout << " Reading Tuple with TID: " << tid.get_value() << " \n";
                     db.read_tuple(table, tid);
                  }
               }
            } else {
               if (reads_distr(engine)) {
                  auto page = page_distr(engine);
                  auto slot = slot_distr(engine);
                  std::cout << " Page: " << page << " SLOT: " << slot << " \n";
                  moderndbs::TID tid{page, slot};
                  std::cout << " Reading Tuple with TID: " << tid.get_value() << " \n";
                  db.read_tuple(table, tid);
               } else {
                  moderndbs::OrderRecord order = {j, j * 2, j * 100, j % 5, (j % 2 == 0 ? 'G' : 'H')};
                  db.insert(table, order);
               }
            }
         }

         // for (uint64_t i = 0; i < insertions_per_thread; ++i) {
         //    moderndbs::OrderRecord order = {i, i * 2, i * 100, i % 5, (i % 2 == 0 ? 'G' : 'H')};
         //    auto tid = db.insert(table, order);
         //    tids_per_thread[thread].push_back(tid);
         // }
      });
   }

   for (auto& t : threads)
      t.join();

   errorListener.join();
   std::cout << "Error listener is ready, now starting other threads...\n";


   // // for (int i = 0; i < 1000; ++i) {
   // //    uint64_t random_page = page(engine);
   // //    uint64_t random_slot = slot(engine);
   // //    auto tid = moderndbs::TID(random_page, random_slot);
   // //    // Alternatively, you could print them directly:
   // //    // std::cout << "RAND: " << random_value << " " << random_value2  << " \n";
   // //    // std::cout << "TID: " << tid.get_page_id(table.sp_segment) << " Slot: " <<tid.get_slot() << std::endl;
   // //    auto result = db.read_tuple(table, tid);
   // // }
}

// int main() {
//
//    std::thread errorListener(listenForErrors);
//
//    using moderndbs::File;
//    for (const auto* segment_file : std::vector<const char*>{"/tmp/transfuse_mnt/test.txt"}) {
//       auto file = File::open_file(segment_file, File::Mode::WRITE);
//       file->resize(0);
//    }
//
//    moderndbs::FileMapper schema_file_mapper("/tmp/transfuse_mnt/test.txt", (sysconf(_SC_PAGESIZE)));
//    auto p = schema_file_mapper.get_page(8, true);
//    schema_file_mapper.release_page(p);
//    should_stop_error_listener = true;
//    errorListener.join();
//    std::cout << "Error listener is ready, now starting other threads...\n";
//
// }
