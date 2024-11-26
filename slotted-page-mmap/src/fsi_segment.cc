#include "moderndbs/segment.h"
#include <cstring>

using FSISegment = moderndbs::FSISegment;

FSISegment::FSISegment(uint16_t segment_id, FileMapper &file_mapper, schema::Table &table)
    : Segment(segment_id, file_mapper), table(table) {
}

uint8_t FSISegment::encode_free_space(uint32_t free_space) {
    // free size / (page size / (2^bits) )
    // bits = 4 in this example (CHECK line 76 in segment.h)
    // pow(2, 4) = 16
    uint8_t encoded_free_space = free_space / ((file_mapper.get_data_size()) / 16);

    //   uint8_t encoded_free_space = ceil(log2(free_space));
    return static_cast<uint8_t>(encoded_free_space);
}

uint32_t FSISegment::decode_free_space(uint8_t free_space) {
    // Linear scale
    const auto decoded_free_space = static_cast<uint32_t>(free_space * ((file_mapper.get_data_size()) / 16));
    // Logarithmic scale
    //   uint32_t decoded_free_space = (uint32_t) pow(2, free_space - 1);
    return decoded_free_space;
}

void FSISegment::update(uint64_t target_page, uint32_t free_space) {
    uint64_t target_page_id = target_page & 0xFFFFFFULL;
    uint8_t encoded_space = encode_free_space(free_space);

    // Calculate the target page and offset
    const size_t entries_per_page = (file_mapper.get_data_size() - sizeof(uint64_t)) * 2;
    // 2 pages per byte (4 bits per page)
    const size_t fsi_target_page_number = target_page_id / entries_per_page;
    const size_t target_entry_index = target_page_id % entries_per_page;
    const size_t byte_index = target_entry_index / 2;
    const bool is_upper_nibble = (target_entry_index % 2 == 0);

    // Update size in first page
    {
        auto first_page = file_mapper.get_page(0, true);
        auto *first_page_data = first_page->get_data();

        // [0-8[   : FSI string length in #bytes
        auto &fsi_bitmap_size = *reinterpret_cast<uint64_t *>(first_page_data);
        if (target_page_id / 2 >= fsi_bitmap_size) {
            fsi_bitmap_size = (target_page_id / 2) + 1;
        }
        file_mapper.release_page(first_page);
    }

    auto fsi_page = file_mapper.get_page(fsi_target_page_number, true);
    auto *fsi_data = fsi_page->get_data();

    // Skip the header
    size_t header_offset = (fsi_target_page_number == 0) ? sizeof(uint64_t) : 0;
    uint8_t &byte_to_update = reinterpret_cast<uint8_t *>(fsi_data + header_offset)[byte_index];

    if (is_upper_nibble) {
        byte_to_update &= 0x0F; // Clear the most significant 4 bits of the existing value
        byte_to_update |= (encoded_space << 4); // Set the most significant 4 bits to encoded_space
    } else {
        byte_to_update &= 0xF0; // Clear the lower 4 bits
        byte_to_update |= (encoded_space & 0x0F); // Set the lower 4 bits
    }

    // Update the last_page_with_space cache
    if (free_space > 0) {

        // If the page has free space, update the cache
        if (!last_page_with_space || target_page_id < *last_page_with_space) {
            last_page_with_space = target_page_id;
        }
    }else {

        // If the page is full and it was cached, invalidate the cache
        if (last_page_with_space && *last_page_with_space == target_page_id) {
            last_page_with_space.reset();
        }
    }

    // Release the page to be written to file
    file_mapper.release_page(fsi_page);

    //
    //  // If the target_page does not exist, then it should be created and updated!!!
    //  // auto &page = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, true);
    //  auto page = file_mapper.get_page(fsi_target_page_number, true);
    //
    //  // [0-8[   : Schema string length in #bytes
    //  auto &fsi_bitmap_size = *reinterpret_cast<uint64_t *>(page->get_data());
    //
    //  // Each element in the bitmap is 8 bytes -> save # bytes that were previously saved in the bitmap and we fill it again.
    //  if (byte_index >= fsi_bitmap_size) { // Resize fsi_bitmap to fit more pages
    //      fsi_bitmap_size = byte_index + 1;
    //  }
    //
    //  // Load the current FSI bitmap from the page data
    //  fsi_bitmap.resize(fsi_bitmap_size);
    //  std::memcpy(fsi_bitmap.data(), page->get_data() + sizeof(uint64_t), fsi_bitmap_size);
    //
    //  if (is_upper_nibble) {
    //      fsi_bitmap[target_page_id / 2] &= 0x0F; // Clear the most significant 4 bits of the existing value
    //      fsi_bitmap[target_page_id / 2] |= (encoded_space << 4); // Set the most significant 4 bits to encoded_space
    //  } else {
    //      fsi_bitmap[target_page_id / 2] &= 0xF0; // Clear the lower 4 bits
    //      fsi_bitmap[target_page_id / 2] |= (encoded_space & 0x0F); // Set the lower 4 bits
    //  }
    //
    //  std::memcpy(page->get_data() + sizeof(uint64_t), fsi_bitmap.data(), fsi_bitmap_size);
    //  // buffer_manager.unfix_page(page, true);
    // file_mapper.release_page(page);
}

std::optional<uint64_t> FSISegment::find(uint32_t required_space) {
    const size_t entries_per_page = (file_mapper.get_data_size() - sizeof(uint64_t)) * 2;
    // 2 pages per byte (4 bits per page)

    // 1. Check the cached `last_page_with_space`
    if (last_page_with_space) {
        uint64_t cached_page_id = *last_page_with_space;
        uint64_t cached_fsi_page_id = cached_page_id / entries_per_page;
        uint64_t target_entry_index = cached_page_id % entries_per_page;
        uint64_t byte_index = target_entry_index / 2;
        bool is_upper_nibble = (target_entry_index % 2 == 0);

        // Fix the FSI page containing the cached entry
        auto cached_page = file_mapper.get_page(cached_fsi_page_id, false);
        auto *cached_data = cached_page->get_data();
        size_t header_offset = (cached_fsi_page_id == 0) ? sizeof(uint64_t) : 0;
        uint8_t current_byte = reinterpret_cast<uint8_t *>(cached_data + header_offset)[byte_index];

        // Decode the nibble
        uint8_t stored_value = is_upper_nibble ? (current_byte >> 4) & 0x0F : current_byte & 0x0F;
        uint32_t decoded_space = decode_free_space(stored_value);

        file_mapper.release_page(cached_page);

        if (required_space <= decoded_space) {
            return cached_page_id;
        }

        // If the cached page doesn't satisfy, invalidate the cache
        last_page_with_space.reset();
    }

    uint64_t current_page_id = 0;
    uint64_t fsi_total_entries = 0;

    while (true) {
        auto page = file_mapper.get_page(current_page_id, false);
        auto *page_data = page->get_data();

        if (current_page_id == 0) {
            // [0-8[   : FSI string length in #bytes
            fsi_total_entries = *reinterpret_cast<uint64_t *>(page_data);
        }

        size_t header_offset = (current_page_id == 0) ? sizeof(uint64_t) : 0;
        size_t entries_in_this_page = std::min(entries_per_page,
                                               fsi_total_entries - (current_page_id * entries_per_page));

        // Iterate over entries in this page
        for (int i = 0; i < entries_in_this_page / 2; ++i) {
            uint8_t current_byte = reinterpret_cast<uint8_t *>(page_data + header_offset)[i];

            // Check the high nibble (4 bits)
            uint8_t stored_high_value = (current_byte >> 4) & 0x0F; // Extract high 4 bits
            uint32_t decoded_space_high = decode_free_space(stored_high_value);
            if (required_space <= decoded_space_high) {
                file_mapper.release_page(page);
                last_page_with_space = (current_page_id * entries_per_page) + (i * 2);
                return *last_page_with_space;
            }

            // Check the low nibble (4 bits)
            uint8_t stored_low_value = current_byte & 0x0F; // Extract low 4 bits
            uint32_t decoded_space_low = decode_free_space(stored_low_value);
            if (required_space <= decoded_space_low) {
                file_mapper.release_page(page);
                last_page_with_space = (current_page_id * entries_per_page) + (i * 2) + 1; // Update the cached page
                return *last_page_with_space;
            }
        }

        file_mapper.release_page(page);

        current_page_id++;

        // If we processed all entried, break
        if ((current_page_id * entries_per_page) >= fsi_total_entries) {
            break;
        }
    }

    return std::nullopt;

    // auto page = file_mapper.get_page(0, false);
    //
    // const auto fsi_bitmap_size = *reinterpret_cast<uint64_t *>(page->get_data());
    // // Each element in the bitmap is 8 bytes, hence we save the number of bytes that were previously saved in the bitmap, and we fill the bitmap again.
    // const auto remaining_bytes = fsi_bitmap_size;
    // file_mapper.release_page(page);
    //
    // if (fsi_bitmap_size == 0) {
    //     return std::nullopt;
    // }
    //
    // const size_t total_pages = (fsi_bitmap_size + entries_per_page - 1) / entries_per_page;
    // size_t processed_bytes = 0;
    // const size_t bytes_per_page = 4 * 1024 - sizeof(uint64_t);
    //
    // for (size_t page_number = 0; page_number < total_pages; page_number++) {
    //     page = file_mapper.get_page(page_number, false);
    //
    //     // Determine how many bytes to read from the current page
    //     size_t remaining_bytes = fsi_bitmap_size - processed_bytes;
    //     size_t bytes_to_read = std::min(remaining_bytes, bytes_per_page);
    //     fsi_bitmap.resize(bytes_to_read);
    //     std::memcpy(fsi_bitmap.data(), page->get_data() + sizeof(uint64_t), bytes_to_read);
    //
    //     for (uint64_t i = 0; i < fsi_bitmap.size(); i++) {
    //         const uint8_t stored_high_value = (fsi_bitmap[i] >> 4) & 0x0F; // Extract high 4 bits
    //         if (const auto decoded_space_high = decode_free_space(stored_high_value); required_space <= decoded_space_high) {
    //             //buffer_manager.unfix_page(page, true);
    //             file_mapper.release_page(page);
    //             return  (page_number * entries_per_page) + (i * 2);
    //         }
    //         const uint8_t stored_low_value = fsi_bitmap[i] & 0x0F; // Extract low 4 bits
    //         if (const auto decoded_space_low = decode_free_space(stored_low_value); required_space <= decoded_space_low) {
    //             //buffer_manager.unfix_page(page, true);
    //             file_mapper.release_page(page);
    //             return  (page_number * entries_per_page) + (i * 2) + 1;
    //         }
    //     }
    //
    //     processed_bytes += bytes_to_read;
    //     // Unfix the page
    //     //buffer_manager.unfix_page(page, true);
    //     file_mapper.release_page(page);
    //
    // }
}
