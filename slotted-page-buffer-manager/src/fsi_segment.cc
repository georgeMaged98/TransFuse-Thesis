#include "moderndbs/segment.h"
#include <cstring>


using FSISegment = moderndbs::FSISegment;

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

void FSISegment::update(uint64_t target_page, uint32_t free_space) {
   /// Target Page in Slotted Pages whose free space would change
    uint64_t target_page_id = target_page & 0xFFFFFFULL;
    uint8_t encoded_space = encode_free_space(free_space);
   if(target_page_id == 8160) {
      std::cout << " CHECK !\n";
   }
   // Calculate the target page and offset
   const size_t entries_per_page = (buffer_manager.get_page_size() - sizeof(uint64_t)) * 2; // 2 pages per byte (4 bits per page)
   size_t page_data_capacity = buffer_manager.get_page_size() - sizeof(uint64_t);
   const size_t fsi_target_page_number = target_page_id / entries_per_page;
   const size_t target_entry_index = target_page_id % entries_per_page;
   const size_t byte_index = target_entry_index / 2;
   const bool is_upper_nibble = (target_entry_index % 2 == 0);

   // If the target_page does not exist, then it should be created and updated!!!
   // auto &page = buffer_manager.fix_pagee(static_cast<uint64_t>(segment_id) << 48, true);
   auto page0 = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, true);

   // [0-8[   : Schema string length in #bytes
   auto &fsi_bitmap_size = *reinterpret_cast<uint64_t *>(page0->get_data());

   // Each element in the bitmap is 8 bytes -> save # bytes that were previously saved in the bitmap and we fill it again.
    if (target_page_id / 2 >= fsi_bitmap.size()) { // Resize fsi_bitmap to fit more pages
       fsi_bitmap_size = (target_page_id / 2) + 1;
       // Load the current FSI bitmap from the page data
       fsi_bitmap.resize(fsi_bitmap_size);
    }

   // Get max of page 0
   size_t bytes_to_write = std::min(fsi_bitmap_size, page_data_capacity);
   std::memcpy(fsi_bitmap.data(), page0->get_data() + sizeof(uint64_t), bytes_to_write);

   if(fsi_target_page_number == 0) {
      if (is_upper_nibble) {
         fsi_bitmap[target_page_id / 2] &= 0x0F; // Clear the most significant 4 bits of the existing value
         fsi_bitmap[target_page_id / 2] |= (encoded_space << 4); // Set the most significant 4 bits to encoded_space
      } else {
         fsi_bitmap[target_page_id / 2] &= 0xF0; // Clear the lower 4 bits
         fsi_bitmap[target_page_id / 2] |= (encoded_space & 0x0F); // Set the lower 4 bits
      }
      std::memcpy(page0->get_data() + sizeof(uint64_t), fsi_bitmap.data(), bytes_to_write);
      // Unfix first page with the updated fsi_bitmap_size
      buffer_manager.unfix_page(page0,true);
      return;
   }
   buffer_manager.unfix_page(page0,true);

   // Write overflow bitmap data to additional pages
   size_t remaining_bytes = fsi_bitmap_size - bytes_to_write;
   uint64_t current_page_id = 1;

   while (remaining_bytes > 0) {
      const auto overflow_page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ current_page_id, true);

      size_t bytes_for_page = std::min(remaining_bytes, page_data_capacity);
      std::memcpy(fsi_bitmap.data() + bytes_to_write, overflow_page->get_data(), bytes_for_page);

      if(current_page_id == fsi_target_page_number) {
         if (is_upper_nibble) {
            fsi_bitmap[target_page_id / 2] &= 0x0F; // Clear the most significant 4 bits of the existing value
            fsi_bitmap[target_page_id / 2] |= (encoded_space << 4); // Set the most significant 4 bits to encoded_space
         } else {
            fsi_bitmap[target_page_id / 2] &= 0xF0; // Clear the lower 4 bits
            fsi_bitmap[target_page_id / 2] |= (encoded_space & 0x0F); // Set the lower 4 bits
         }
         std::memcpy(overflow_page->get_data(), fsi_bitmap.data() + bytes_to_write, bytes_for_page);
         buffer_manager.unfix_page(overflow_page, true);
         return;
      }

      // Unfix page
      buffer_manager.unfix_page(overflow_page, true);

      bytes_to_write += bytes_for_page;
      remaining_bytes -= bytes_for_page;
      current_page_id++;
   }

   // auto target_page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ target_page_number, true);

    // buffer_manager.unfix_page(page, true);



   // // Each element in the bitmap is 8 bytes -> save # bytes that were previously saved in the bitmap and we fill it again.
   // if (byte_index >= fsi_bitmap_size) { // Resize fsi_bitmap to fit more pages
   //    fsi_bitmap_size = byte_index + 1;
   // }
   //
   // // Load the current FSI bitmap from the page data
   // fsi_bitmap.resize(fsi_bitmap_size);
   // std::memcpy(fsi_bitmap.data(), page->get_data() + sizeof(uint64_t), fsi_bitmap_size);
   //
   // if (is_upper_nibble) {
   //    fsi_bitmap[target_page_id / 2] &= 0x0F; // Clear the most significant 4 bits of the existing value
   //    fsi_bitmap[target_page_id / 2] |= (encoded_space << 4); // Set the most significant 4 bits to encoded_space
   // } else {
   //    fsi_bitmap[target_page_id / 2] &= 0xF0; // Clear the lower 4 bits
   //    fsi_bitmap[target_page_id / 2] |= (encoded_space & 0x0F); // Set the lower 4 bits
   // }
   //
   // std::memcpy(page->get_data() + sizeof(uint64_t), fsi_bitmap.data(), fsi_bitmap_size);
   // buffer_manager.unfix_page(page, true);
}

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

std::optional<uint64_t> FSISegment::find(uint32_t required_space) {
    // Ensure the in-memory `fsi_bitmap` has been loaded and resized to the correct size
    if (fsi_bitmap.empty()) {
        auto first_page = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, false);
        auto *first_page_data = first_page->get_data();
        size_t fsi_bitmap_size = *reinterpret_cast<uint64_t *>(first_page_data);
        fsi_bitmap.resize(fsi_bitmap_size); // Resize to match the actual bitmap size
        buffer_manager.unfix_page(first_page, false);
    }


   uint64_t current_page_id = 0;
   size_t cumulative_index = 0;

    while (cumulative_index < fsi_bitmap.size() * 2) {
        // Fix the current page in read mode
        auto page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ current_page_id, false);
        auto *page_data = page->get_data();

       size_t page_data_capacity = buffer_manager.get_page_size() - sizeof(uint64_t); // Capacity per page
       const size_t total_entries_per_page = page_data_capacity * 2; // 2 entries per byte (4 bits per page)

        // Calculate the number of bytes to read from the current page
        size_t bytes_to_read = std::min(page_data_capacity, fsi_bitmap.size() - cumulative_index / 2);
         auto *bitmap_data = current_page_id == 0 ?
            page_data + sizeof(uint64_t) // Skip the uint64_t header in the first page
            : page_data;                   // Directly point to the data for subsequent pages

        // Copy the relevant data directly into `fsi_bitmap`
        std::memcpy(fsi_bitmap.data() + cumulative_index / 2, bitmap_data, bytes_to_read);

        // Iterate over the loaded portion of the bitmap to find a match
        for (size_t i = cumulative_index / 2; i < (cumulative_index / 2) + bytes_to_read; i++) {
            // Check the high nibble (4 bits)
            uint8_t stored_high_value = (fsi_bitmap[i] >> 4) & 0x0F;
            auto decoded_space_high = decode_free_space(stored_high_value);
            if (required_space <= decoded_space_high) {
                buffer_manager.unfix_page(page, false);
                return i * 2; // Return the index of the matching page
            }

            // Check the low nibble (4 bits)
            uint8_t stored_low_value = fsi_bitmap[i] & 0x0F;
            auto decoded_space_low = decode_free_space(stored_low_value);
            if (required_space <= decoded_space_low) {
                buffer_manager.unfix_page(page, false);
                return (i * 2) + 1; // Return the index of the matching page
            }
        }

        // Unfix the current page after processing
        buffer_manager.unfix_page(page, false);

        // Update cumulative index and move to the next page
        cumulative_index += total_entries_per_page;
        current_page_id++;
         std::cout <<  "SIZE: " << fsi_bitmap.size()  << std::endl;
        // Break if we've processed all the pages
        if (cumulative_index >= fsi_bitmap.size() * 2) {
            break;
        }
    }

    // If no page meets the required space, return std::nullopt
    return std::nullopt;
}
