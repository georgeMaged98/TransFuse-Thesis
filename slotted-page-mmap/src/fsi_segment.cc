#include "moderndbs/segment.h"
#include <cstring>

using FSISegment = moderndbs::FSISegment;

FSISegment::FSISegment(uint16_t segment_id, FileMapper &file_mapper, schema::Table &table)
        : Segment(segment_id, file_mapper), table(table) {
}

uint8_t FSISegment::encode_free_space(uint32_t free_space) {
    // TODO: add your implementation here
    // free size / (page size / (2^bits) )
    // bits = 4 in this example (CHECK line 76 in segment.h)
    // pow(2, 4) = 16
    uint8_t encoded_free_space = free_space / ((file_mapper.get_data_size()) / 16);

    //   uint8_t encoded_free_space = ceil(log2(free_space));
    return static_cast<uint8_t>(encoded_free_space);
}

uint32_t FSISegment::decode_free_space(uint8_t free_space) {
    // TODO: add your implementation here
    // Linear scale
   const auto decoded_free_space = static_cast<uint32_t>(free_space * ((file_mapper.get_data_size()) / 16));
    // Logarithmic scale
    //   uint32_t decoded_free_space = (uint32_t) pow(2, free_space - 1);
    return decoded_free_space;
}

void FSISegment::update(uint64_t target_page, uint32_t free_space) {
    /// TODO: add your implementation here
    uint64_t target_page_id = target_page & 0xFFFFFFULL;
    uint8_t encoded_space = encode_free_space(free_space);
    // If the target_page does not exist, then it should be created and updated!!!
    // auto &page = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, true);
    auto page = file_mapper.get_page(0, true);
    // [0-8[   : Schema string length in #bytes
    auto &fsi_bitmap_size = *reinterpret_cast<uint64_t *>(page->get_data());
    // Each element in the bitmap is 8 bytes -> save # bytes that were previously saved in the bitmap and we fill it again.
    if (target_page_id / 2 >= fsi_bitmap.size()) { // Resize fsi_bitmap to fit more pages
        fsi_bitmap_size = (target_page_id / 2) + 1;
        fsi_bitmap.resize(fsi_bitmap_size);
    }

    // Load the current FSI bitmap from the page data
    std::memcpy(fsi_bitmap.data(), page->get_data() + sizeof(uint64_t), fsi_bitmap_size);

    if (target_page_id % 2 == 0) {
        fsi_bitmap[target_page_id / 2] &= 0x0F; // Clear the most significant 4 bits of the existing value
        fsi_bitmap[target_page_id / 2] |= (encoded_space << 4); // Set the most significant 4 bits to encoded_space
    } else {
        fsi_bitmap[target_page_id / 2] &= 0xF0; // Clear the lower 4 bits
        fsi_bitmap[target_page_id / 2] |= (encoded_space & 0x0F); // Set the lower 4 bits
    }

    std::memcpy(page->get_data() + sizeof(uint64_t), fsi_bitmap.data(), fsi_bitmap_size);
    // buffer_manager.unfix_page(page, true);
   file_mapper.release_page(page);
}

std::optional<uint64_t> FSISegment::find(uint32_t required_space) {
    /// TODO: add your implementation here
    // auto &page = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, true);
    auto page = file_mapper.get_page(0, false);
    // [0-8[   : Schema string length in #bytes
    const auto fsi_bitmap_size = *reinterpret_cast<uint64_t *>(page->get_data());
    // Each element in the bitmap is 8 bytes, hence we save the number of bytes that were previously saved in the bitmap, and we fill the bitmap again.
    const auto remaining_bytes = fsi_bitmap_size;
    fsi_bitmap.resize(fsi_bitmap_size);
    std::memcpy(fsi_bitmap.data(), page->get_data() + sizeof(uint64_t), remaining_bytes);

    for (uint64_t i = 0; i < fsi_bitmap.size(); i++) {
       const uint8_t stored_high_value = (fsi_bitmap[i] >> 4) & 0x0F; // Extract high 4 bits
       if (const auto decoded_space_high = decode_free_space(stored_high_value); required_space <= decoded_space_high) {
           //buffer_manager.unfix_page(page, true);
          file_mapper.release_page(page);
           return (i * 2);
        }
        const uint8_t stored_low_value = fsi_bitmap[i] & 0x0F; // Extract low 4 bits
        if (const auto decoded_space_low = decode_free_space(stored_low_value); required_space <= decoded_space_low) {
            //buffer_manager.unfix_page(page, true);
           file_mapper.release_page(page);
            return (i * 2) + 1;
        }
    }
    // Unfix the page
   //buffer_manager.unfix_page(page, true);
   file_mapper.release_page(page);
   return std::nullopt;
}