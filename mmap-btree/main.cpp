// main.cpp

#include "transfuse/hex_dump.h"
#include "transfuse/file_mapper.h"
// #include "transfuse/btree.h"
#include <iostream>
#include <cstring>

#include "transfuse/btree.h"

using namespace transfuse;


int main() {

    // const std::string filename = "btree.dat";
    // constexpr size_t page_size = 4096;
    // FileMapper file_mapper(filename, page_size);
    // size_t s = file_mapper.calculate_file_size(0);
    // cout << s << endl;
    //
    // s = file_mapper.calculate_file_size(100);
    // cout << s << endl;
    //
    // s = file_mapper.calculate_file_size(1000);
    // cout << s << endl;
    //
    // s = file_mapper.calculate_file_size(35000);
    // cout << s << endl;
    //
    // s = file_mapper.calculate_file_size(66536);
    // cout << s << endl;
    //
    // s = file_mapper.calculate_file_size(1LL << 30);
    // cout << s << endl;


    // READ Data from DISK
    // char *d = file_mapper.get_page(0);
    //
    // auto* s = reinterpret_cast<transfuse::SerializedPage*>(d);
    //
    // cout << d[0] << endl;
    // WRITE SerializedPage to DISK
    // size_t total_size = sizeof(SerializedPage);
    // SerializedPage sp = {true, 42, 1024};
    //
    // // Convert to std::byte*
    // auto* bytePtr = reinterpret_cast<std::byte*>(&sp);

    // Accessing the data (for demonstration)
    // for (size_t i = 0; i < sizeof(SerializedPage); ++i) {
    //     std::cout << std::to_integer<int>(bytePtr[i]) << " ";
    // }
    // std::cout << std::endl;

    //file_mapper.write_to_file(bytePtr, total_size);
    //
    // delete sp;
    //char* example_data = "Hello!";
    //size_t data_size = std::strlen(example_data) + 1;
    //size_t total_size = sizeof(transfuse::SerializedPage) + data_size;
    // auto *sp = reinterpret_cast<transfuse::SerializedPage *>(new byte[total_size]);
    // std::memcpy(sp->data, example_data, data_size);
    // sp->exclusive = true;
    // sp->id = 1;
    // sp->size = total_size;
    // //transfuse::Page p(const_cast<char*>(example_data), total_size, 0);
    // std::string hx = transfuse::hex_dump_str(sp, total_size);
    // file_mapper.write_to_file(hx.c_str(), total_size);
    // delete[] reinterpret_cast<char *>(sp);
    //
    // auto *d = static_cast<char*>(file_mapper.get_page(0));
    // auto *sp2 = reinterpret_cast<transfuse::SerializedPage*>(d);
    //
    // cout << sp->id << endl;
    //     transfuse::BTree<uint64_t, uint64_t, std::less<>, 1024> btree(0, file_mapper);
    //     // Example usage
    //     btree.insert(10, 10);
    //     btree.lookup(10);
    //
    //     // if (node) {
    //     //     std::cout << "Found node with keys: ";
    //     //     for (int key : node->keys()) {
    //     //         std::cout << key << " ";
    //     //     }
    //     //     std::cout << std::endl;
    //     // } else {
    //     //     std::cout << "Key not found." << std::endl;
    //     // }
    // Page *page = file_mapper.get_page(0);
    // std::cout << "Page ID: " << page->get_id() << std::endl;
    // std::cout << "Page Count: " << page->get_count() << std::endl;
    // std::cout << "Page Exclusive: " << page->is_exclusive() << std::endl;
    // using BTree = BTree<uint64_t, uint64_t, std::less<uint64_t>, 1024>;
    //
    // auto *root_node = reinterpret_cast<BTree::Node*>(page->get_data());
    //
    // cout << root_node->is_leaf() << endl;
    // Page *p = btree.read_page(0, true);
    // p->set_count(22);
    // Set or modify the id and count
    //page.set_id(12);
    // page.set_count(1);
    //page.set_exclusive(false);
    // // Modify the data directly
    //std::strcpy(page->get_data(), "This data is directly in mmap AGAIN AND AGAIN!");

    // Print to confirm changes
    // std::cout << "Page ID: " << page->get_id() << std::endl;
    // std::cout << "Page Count: " << page->get_count() << std::endl;
    // std::cout << "Page Exclusive: " << page->is_exclusive() << std::endl;
    //std::cout << "Page Data: " << page->get_data() << std::endl;

    return 0;
}
