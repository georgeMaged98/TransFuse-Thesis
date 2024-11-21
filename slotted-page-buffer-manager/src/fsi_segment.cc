#include "moderndbs/segment.h"
#include <cstring>
#include <algorithm>

using FSISegment = moderndbs::FSISegment;
using namespace std;

FSISegment::FSISegment(uint16_t segment_id, BufferManager &buffer_manager, schema::Table &table)
        : Segment(segment_id, buffer_manager), table(table) {
   std::string filename = std::to_string(segment_id) + ".txt";
   // Check if the file exists
   std::ifstream infile(filename);
   if (!infile.good()) {
      std::cerr << "File not found. Creating file: " << filename << std::endl;
      std::ofstream outfile(filename); // Creates the file
      outfile.close();
   }
}

uint8_t FSISegment::encode_free_space(uint32_t free_space) {
    // free size / (page size / (2^bits) )
    // bits = 4 in this example (CHECK line 76 in segment.h)
    // pow(2, 4) = 16
    uint8_t encoded_free_space = free_space / (buffer_manager.get_page_size() / 16);

    //   uint8_t encoded_free_space = ceil(log2(free_space));
    return static_cast<uint8_t>(encoded_free_space);
}

uint32_t FSISegment::decode_free_space(uint8_t free_space) {
    // Linear scale
   const auto decoded_free_space = static_cast<uint32_t>(free_space * (buffer_manager.get_page_size() / 16));
    // Logarithmic scale
    //   uint32_t decoded_free_space = (uint32_t) pow(2, free_space - 1);
    return decoded_free_space;
}

//
// void FSISegment::update(uint64_t target_page, uint32_t free_space) {
//    /// Target Page in Slotted Pages whose free space would change
//     uint64_t target_page_id = target_page & 0xFFFFFFULL;
//     uint8_t encoded_space = encode_free_space(free_space);
//
//    // Calculate the target page and offset
//    const size_t entries_per_page = (buffer_manager.get_page_size() - sizeof(uint64_t)) * 2; // 2 pages per byte (4 bits per page)
//    uint64_t page_data_capacity = buffer_manager.get_page_size() - sizeof(uint64_t);
//    const size_t fsi_target_page_number = target_page_id / entries_per_page;
//    const size_t target_entry_index = target_page_id % entries_per_page;
//    const size_t byte_index = target_entry_index / 2;
//    const bool is_upper_nibble = (target_entry_index % 2 == 0);
//
//    // If the target_page does not exist, then it should be created and updated!!!
//    // auto &page = buffer_manager.fix_pagee(static_cast<uint64_t>(segment_id) << 48, true);
//    auto page0 = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, true);
//
//    // [0-8[   : Schema string length in #bytes
//    auto &fsi_bitmap_size = *reinterpret_cast<uint64_t *>(page0->get_data());
//
//    // Each element in the bitmap is 8 bytes -> save # bytes that were previously saved in the bitmap and we fill it again.
//     if (target_page_id / 2 >= fsi_bitmap.size()) { // Resize fsi_bitmap to fit more pages
//        fsi_bitmap_size = (target_page_id / 2) + 1;
//        // Load the current FSI bitmap from the page data
//        fsi_bitmap.resize(fsi_bitmap_size);
//     }
//
//    // Get max of page 0
//    size_t bytes_to_write = std::min(fsi_bitmap_size, page_data_capacity);
//    std::memcpy(fsi_bitmap.data(), page0->get_data() + sizeof(uint64_t), bytes_to_write);
//
//    if(fsi_target_page_number == 0) {
//       if (is_upper_nibble) {
//          fsi_bitmap[target_page_id / 2] &= 0x0F; // Clear the most significant 4 bits of the existing value
//          fsi_bitmap[target_page_id / 2] |= (encoded_space << 4); // Set the most significant 4 bits to encoded_space
//       } else {
//          fsi_bitmap[target_page_id / 2] &= 0xF0; // Clear the lower 4 bits
//          fsi_bitmap[target_page_id / 2] |= (encoded_space & 0x0F); // Set the lower 4 bits
//       }
//       std::memcpy(page0->get_data() + sizeof(uint64_t), fsi_bitmap.data(), bytes_to_write);
//       // Unfix first page with the updated fsi_bitmap_size
//       buffer_manager.unfix_page(page0,true);
//       return;
//    }
//    buffer_manager.unfix_page(page0,true);
//
//    // Write overflow bitmap data to additional pages
//     uint64_t remaining_bytes = fsi_bitmap_size - bytes_to_write;
//    uint64_t current_page_id = 1;
//
//    while (remaining_bytes > 0) {
//       const auto overflow_page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ current_page_id, true);
//
//       size_t bytes_for_page = std::min(remaining_bytes, page_data_capacity);
//       std::memcpy(fsi_bitmap.data() + bytes_to_write, overflow_page->get_data(), bytes_for_page);
//
//       if(current_page_id == fsi_target_page_number) {
//          if (is_upper_nibble) {
//             fsi_bitmap[target_page_id / 2] &= 0x0F; // Clear the most significant 4 bits of the existing value
//             fsi_bitmap[target_page_id / 2] |= (encoded_space << 4); // Set the most significant 4 bits to encoded_space
//          } else {
//             fsi_bitmap[target_page_id / 2] &= 0xF0; // Clear the lower 4 bits
//             fsi_bitmap[target_page_id / 2] |= (encoded_space & 0x0F); // Set the lower 4 bits
//          }
//          std::memcpy(overflow_page->get_data(), fsi_bitmap.data() + bytes_to_write, bytes_for_page);
//          buffer_manager.unfix_page(overflow_page, true);
//          return;
//       }
//
//       // Unfix page
//       buffer_manager.unfix_page(overflow_page, true);
//
//       bytes_to_write += bytes_for_page;
//       remaining_bytes -= bytes_for_page;
//       current_page_id++;
//    }
//
//    // auto target_page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ target_page_number, true);
//
//     // buffer_manager.unfix_page(page, true);
//
//
//
//    // // Each element in the bitmap is 8 bytes -> save # bytes that were previously saved in the bitmap and we fill it again.
//    // if (byte_index >= fsi_bitmap_size) { // Resize fsi_bitmap to fit more pages
//    //    fsi_bitmap_size = byte_index + 1;
//    // }
//    //
//    // // Load the current FSI bitmap from the page data
//    // fsi_bitmap.resize(fsi_bitmap_size);
//    // std::memcpy(fsi_bitmap.data(), page->get_data() + sizeof(uint64_t), fsi_bitmap_size);
//    //
//    // if (is_upper_nibble) {
//    //    fsi_bitmap[target_page_id / 2] &= 0x0F; // Clear the most significant 4 bits of the existing value
//    //    fsi_bitmap[target_page_id / 2] |= (encoded_space << 4); // Set the most significant 4 bits to encoded_space
//    // } else {
//    //    fsi_bitmap[target_page_id / 2] &= 0xF0; // Clear the lower 4 bits
//    //    fsi_bitmap[target_page_id / 2] |= (encoded_space & 0x0F); // Set the lower 4 bits
//    // }
//    //
//    // std::memcpy(page->get_data() + sizeof(uint64_t), fsi_bitmap.data(), fsi_bitmap_size);
//    // buffer_manager.unfix_page(page, true);
// }
//


// std::optional<uint64_t> FSISegment::find(uint32_t required_space) {
//    auto page = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, false);
//    auto *page_data = page->get_data();
//     // [0-8[   : Schema string length in #bytes
//     auto fsi_bitmap_size = *reinterpret_cast<uint64_t *>(page_data);
//     // Each element in the bitmap is 8 bytes, hence we save the number of bytes that were previously saved in the bitmap, and we fill the bitmap again.
//     auto remaining_bytes = fsi_bitmap_size;
//     fsi_bitmap.resize(fsi_bitmap_size);
//     std::memcpy(fsi_bitmap.data(), page_data + sizeof(uint64_t), remaining_bytes);
//
//     for (uint64_t i = 0; i < fsi_bitmap.size(); i++) {
//         uint8_t stored_high_value = (fsi_bitmap[i] >> 4) & 0x0F; // Extract high 4 bits
//         auto decoded_space_high = decode_free_space(
//                 stored_high_value); // Represents Fill Rate of the page in higher 4 bits
//         if (required_space <= decoded_space_high) {
//             buffer_manager.unfix_page(page, false);
//             return (i * 2);
//         }
//         uint8_t stored_low_value = fsi_bitmap[i] & 0x0F; // Extract low 4 bits
//         auto decoded_space_low = decode_free_space(
//                 stored_low_value); // Represents Fill Rate of the page in lower 4 bits
//         if (required_space <= decoded_space_low) {
//             buffer_manager.unfix_page(page, false);
//             return (i * 2) + 1;
//         }
//     }
//     // Unfix the page
//    buffer_manager.unfix_page(page, false);
//    return std::nullopt;
// }
//
// std::optional<uint64_t> FSISegment::find(uint32_t required_space) {
//     // Ensure the in-memory `fsi_bitmap` has been loaded and resized to the correct size
//     if (fsi_bitmap.empty()) {
//         auto first_page = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, false);
//         auto *first_page_data = first_page->get_data();
//         size_t fsi_bitmap_size = *reinterpret_cast<uint64_t *>(first_page_data);
//         fsi_bitmap.resize(fsi_bitmap_size); // Resize to match the actual bitmap size
//         buffer_manager.unfix_page(first_page, false);
//     }
//
//    if (last_page_with_space) {
//       uint64_t cached_page_id = *last_page_with_space;
//       uint8_t stored_value = (fsi_bitmap[cached_page_id / 2] >> ((cached_page_id % 2 == 0) ? 4 : 0)) & 0x0F;
//       uint32_t decoded_space = decode_free_space(stored_value);
//       if (required_space <= decoded_space) {
//          return cached_page_id;
//       }
//    }
//
//
//    uint64_t current_page_id = 0;
//    size_t cumulative_index = 0;
//
//     while (cumulative_index < fsi_bitmap.size() * 2) {
//         // Fix the current page in read mode
//         auto page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ current_page_id, false);
//         auto *page_data = page->get_data();
//
//        size_t page_data_capacity = buffer_manager.get_page_size() - sizeof(uint64_t); // Capacity per page
//        const size_t total_entries_per_page = page_data_capacity * 2; // 2 entries per byte (4 bits per page)
//
//         // Calculate the number of bytes to read from the current page
//         size_t bytes_to_read = std::min(page_data_capacity, fsi_bitmap.size() - cumulative_index / 2);
//          auto *bitmap_data = current_page_id == 0 ?
//             page_data + sizeof(uint64_t) // Skip the uint64_t header in the first page
//             : page_data;                   // Directly point to the data for subsequent pages
//
//         // Copy the relevant data directly into `fsi_bitmap`
//         std::memcpy(fsi_bitmap.data() + cumulative_index / 2, bitmap_data, bytes_to_read);
//
//         // Iterate over the loaded portion of the bitmap to find a match
//         for (size_t i = cumulative_index / 2; i < (cumulative_index / 2) + bytes_to_read; i++) {
//             // Check the high nibble (4 bits)
//             uint8_t stored_high_value = (fsi_bitmap[i] >> 4) & 0x0F;
//             auto decoded_space_high = decode_free_space(stored_high_value);
//             if (required_space <= decoded_space_high) {
//                 buffer_manager.unfix_page(page, false);
//                 return i * 2; // Return the index of the matching page
//             }
//
//             // Check the low nibble (4 bits)
//             uint8_t stored_low_value = fsi_bitmap[i] & 0x0F;
//             auto decoded_space_low = decode_free_space(stored_low_value);
//             if (required_space <= decoded_space_low) {
//                 buffer_manager.unfix_page(page, false);
//                 return (i * 2) + 1; // Return the index of the matching page
//             }
//         }
//
//         // Unfix the current page after processing
//         buffer_manager.unfix_page(page, false);
//
//         // Update cumulative index and move to the next page
//         cumulative_index += total_entries_per_page;
//         current_page_id++;
//          std::cout <<  "SIZE: " << fsi_bitmap.size()  << std::endl;
//         // Break if we've processed all the pages
//         if (cumulative_index >= fsi_bitmap.size() * 2) {
//             break;
//         }
//     }
//
//     // If no page meets the required space, return std::nullopt
//     return std::nullopt;
// }



std::optional<uint64_t> FSISegment::find(uint32_t required_space) {
    const uint64_t entries_per_page = (buffer_manager.get_page_size() - sizeof(uint64_t)) * 2; // 2 entries per byte

    // 1. Check the cached `last_page_with_space`
    if (last_page_with_space) {
        uint64_t cached_page_id = *last_page_with_space;
        uint64_t cached_fsi_page_id = cached_page_id / entries_per_page;
        uint64_t  target_entry_index = cached_page_id % entries_per_page;
        uint64_t  byte_index = target_entry_index / 2;
        bool is_upper_nibble = (target_entry_index % 2 == 0);

        // Fix the FSI page containing the cached entry
        auto cached_page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ cached_fsi_page_id, false);
        auto *cached_data = cached_page->get_data();
        size_t header_offset = (cached_fsi_page_id == 0) ? sizeof(uint64_t) : 0;
        uint8_t current_byte = reinterpret_cast<uint8_t *>(cached_data + header_offset)[byte_index];

        // Decode the nibble
        uint8_t stored_value = is_upper_nibble ? (current_byte >> 4) & 0x0F : current_byte & 0x0F;
        uint32_t decoded_space = decode_free_space(stored_value);

        buffer_manager.unfix_page(cached_page, false);

        if (required_space <= decoded_space) {
            return cached_page_id;
        }

        // If the cached page doesn't satisfy, invalidate the cache
        last_page_with_space.reset();
    }

    // 2. Fallback to scanning all pages
   uint64_t current_page_id = 0;
   uint64_t fsi_total_entries = 0;

    while (true) {
        // Fix the current FSI page in read mode
        auto page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ current_page_id, false);
        auto *page_data = page->get_data();

       if(current_page_id == 0) {
          fsi_total_entries = *reinterpret_cast<uint64_t *>(page_data);
       }
        // Determine the starting offset for FSI data
        size_t header_offset = (current_page_id == 0) ? sizeof(uint64_t) : 0;
        size_t entries_in_this_page = std::min(entries_per_page, fsi_total_entries - (current_page_id * entries_per_page));

        // Iterate over the entries in this page
        for (size_t i = 0; i < entries_in_this_page / 2; i++) {
            uint8_t current_byte = reinterpret_cast<uint8_t *>(page_data + header_offset)[i];

            // Check the high nibble (4 bits)
            uint8_t stored_high_value = (current_byte >> 4) & 0x0F;
            uint32_t decoded_space_high = decode_free_space(stored_high_value);
            if (required_space <= decoded_space_high) {
                buffer_manager.unfix_page(page, false);
                last_page_with_space = (current_page_id * entries_per_page) + (i * 2); // Update the cache
                return *last_page_with_space;
            }

            // Check the low nibble (4 bits)
            uint8_t stored_low_value = current_byte & 0x0F;
            uint32_t decoded_space_low = decode_free_space(stored_low_value);
            if (required_space <= decoded_space_low) {
                buffer_manager.unfix_page(page, false);
                last_page_with_space = (current_page_id * entries_per_page) + (i * 2) + 1; // Update the cache
                return *last_page_with_space;
            }
        }

        // Unfix the page after processing
        buffer_manager.unfix_page(page, false);

        // Move to the next page
        current_page_id++;

        // Break if we've processed all entries
        if ((current_page_id * entries_per_page) >= fsi_total_entries) {
            break;
        }
    }

    // If no suitable page is found, return std::nullopt
    return std::nullopt;
}


void FSISegment::update(uint64_t target_page, uint32_t free_space) {
    uint64_t target_page_id = target_page & 0xFFFFFFULL;
    uint8_t encoded_space = encode_free_space(free_space);

    // Calculate the target FSI page number and offset
    const size_t entries_per_page = (buffer_manager.get_page_size() - sizeof(uint64_t)) * 2; // 2 entries per byte (4 bits per page)
    const size_t fsi_target_page_number = target_page_id / entries_per_page;
    const size_t target_entry_index = target_page_id % entries_per_page;
    const size_t byte_index = target_entry_index / 2;
    const bool is_upper_nibble = (target_entry_index % 2 == 0);

   {
       auto first_page = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, true);
       auto *first_page_data = first_page->get_data();
       // [0-8[   : Schema string length in #bytes
       auto &fsi_bitmap_size = *reinterpret_cast<uint64_t *>(first_page_data);

       // Each element in the bitmap is 8 bytes -> save # bytes that were previously saved in the bitmap and we fill it again.
       if (target_page_id / 2 >= fsi_bitmap.size()) { // Resize fsi_bitmap to fit more pages
          fsi_bitmap_size = (target_page_id / 2) + 1;
          // Load the current FSI bitmap from the page data
          fsi_bitmap.resize(fsi_bitmap_size);
       }
       buffer_manager.unfix_page(first_page, true);
   }

    // Fix the target FSI page
    auto fsi_page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ fsi_target_page_number, true);
    auto *fsi_data = fsi_page->get_data();

    // Skip the header for the first page
    size_t header_offset = (fsi_target_page_number == 0) ? sizeof(uint64_t) : 0;
    uint8_t &byte_to_update = reinterpret_cast<uint8_t *>(fsi_data + header_offset)[byte_index];

    // Update the specific nibble (4 bits) in the byte
    if (is_upper_nibble) {
        byte_to_update &= 0x0F;                 // Clear the upper nibble
        byte_to_update |= (encoded_space << 4); // Set the upper nibble
    } else {
        byte_to_update &= 0xF0;                // Clear the lower nibble
        byte_to_update |= (encoded_space & 0x0F); // Set the lower nibble
    }

    // Update the last_page_with_space cache
    if (free_space > 0) {
        // If the page has free space, update the cache
        if (!last_page_with_space || target_page_id < *last_page_with_space) {
            last_page_with_space = target_page_id;
        }
    } else {
        // If the page is full and it was cached, invalidate the cache
        if (last_page_with_space && *last_page_with_space == target_page_id) {
            last_page_with_space.reset();
        }
    }

    // Write back the changes and unfix the page
    buffer_manager.unfix_page(fsi_page, true);
}
