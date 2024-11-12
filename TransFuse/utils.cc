//
// Created by george-elfayoumi on 11/8/24.
//
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <cerrno>
#include <chrono>
#include <iostream>
#include <sys/socket.h>
#include <sys/un.h>
#include <cstdint>
#include <iomanip>
#include <limits.h>


static void logMessage(const std::string &message) {
    // Get the current time
    auto now = std::chrono::system_clock::now();
    std::time_t currentTime = std::chrono::system_clock::to_time_t(now);
    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

    // Output the time and message
    std::cout << "["
              << std::put_time(std::localtime(&currentTime), "%Y-%m-%d %H:%M:%S")
              << "." << std::setfill('0') << std::setw(3) << milliseconds.count()  // Milliseconds
              << "] Message sent to server: " << message << std::endl;
}

static void notifyDatabase(const char *filePath, const uint64_t page_offset, const uint64_t writeSize) {
    // int sock;
    // struct sockaddr_un addr;
    // const char* msg = "Invalid write detected";
    //
    // sock = socket(AF_UNIX, SOCK_STREAM, 0);
    // addr.sun_family = AF_UNIX;
    // strcpy(addr.sun_path, "/tmp/db_socket");
    //
    // connect(sock, (struct sockaddr*)&addr, sizeof(addr));
    // send(sock, msg, strlen(msg), 0);
    //
    // close(sock);
    std::string message = std::string(filePath) + "-" + std::to_string(page_offset) + "-" + std::to_string(writeSize);
    int client_sock;
    struct sockaddr_un addr;

    // Create a socket
    client_sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_sock < 0) {
        std::cerr << "Failed to create socket: " << strerror(errno) << std::endl;
        return;
    }
    std::cout << "Client socket created.\n";

    // Setup socket address
    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, "/tmp/db_socket");

    // Connect to the server
    if (connect(client_sock, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        std::cerr << "Failed to connect to server: " << strerror(errno) << std::endl;
        close(client_sock);
        return;
    }
    std::cout << "Connected to server.\n";

    // Send error message
    ssize_t bytes_sent = send(client_sock, message.c_str(), strlen(message.c_str()), 0);
    if (bytes_sent < 0) {
        std::cerr << "Failed to send message: " << strerror(errno) << std::endl;
    } else {
        logMessage(message);
    }

    // Clean up
    close(client_sock);
}


static uint64_t get_latest_flushed_LSN(const char *fuse_root_dir) {
    const char *path = "/wal_segment.txt";
    char fpath[PATH_MAX];
    // Construct the full path by combining the FUSE root directory with the relative path
    snprintf(fpath, sizeof(fpath), "%s%s", fuse_root_dir, path);

    // Open the file for reading
    int fd = open(fpath, O_RDONLY);
    if (fd == -1) {
        perror("Error opening file");
    }

    // Buffer to hold the 8 bytes
    uint64_t latestFlushedLSN;
    static constexpr uint32_t headerSize = 2 * sizeof(size_t) + 1;
    static constexpr uint32_t lockDataSize = 2 * sizeof(int);
    size_t offset = headerSize + lockDataSize;

    // Read the first 8 bytes of the file
    ssize_t res = pread(fd, &latestFlushedLSN, sizeof(uint64_t), offset);
    if (res == -1) {
        perror("Error reading file");
        res = -errno;
    }


    // Print the LSN value
    std::cout << "Latest Flushed LSN: " << latestFlushedLSN << std::endl;

    // Close the file descriptor
    close(fd);

    return latestFlushedLSN;
}


static bool check_valid_wal(const char *buffer, const char *path, const char *root_path) {
    size_t lsn_ref = *reinterpret_cast<const size_t *>(buffer + 2 * sizeof(int) + sizeof(size_t));
    printf("LSN IN PAGE and path is %s : %lu\n", path, lsn_ref);
    uint64_t latest_flushed_lsn = get_latest_flushed_LSN(root_path);
    bool is_valid = strcmp(path, "/sp_segment.txt") != 0 || lsn_ref <= latest_flushed_lsn;
    return is_valid;
}


static bool check_valid_locks(const char *buffer) {
    int readers_count_ref = *reinterpret_cast<const int *>(buffer);
    int state = *reinterpret_cast<const int *>(buffer + sizeof(int));
    printf("First int: %d, Second int: %d\n", readers_count_ref, state);
    return state != 2;
}

static bool is_write_valid(const char *buffer, const char *path, const char *fuse_root_dir) {
    return check_valid_wal(buffer, path, fuse_root_dir) && check_valid_locks(buffer);
}
