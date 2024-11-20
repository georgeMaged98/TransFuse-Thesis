#include "moderndbs/segment.h"
#include <cstring>

using FSISegment = moderndbs::FSISegment;

FSISegment::FSISegment(uint16_t segment_id, BufferManager &buffer_manager, schema::Table &table)
        : Segment(segment_id, buffer_manager), table(table) {
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
    uint64_t target_page_id = target_page & 0xFFFFFFULL;
    uint8_t encoded_space = encode_free_space(free_space);
    // If the target_page does not exist, then it should be created and updated!!!
    auto page = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, true);
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
    buffer_manager.unfix_page(page, true);
}

std::optional<uint64_t> FSISegment::find(uint32_t required_space) {
   auto page = buffer_manager.fix_page(static_cast<uint64_t>(segment_id) << 48, false);

   auto *page_data = page->get_data();
    // [0-8[   : Schema string length in #bytes
    auto fsi_bitmap_size = *reinterpret_cast<uint64_t *>(page_data);
    // Each element in the bitmap is 8 bytes, hence we save the number of bytes that were previously saved in the bitmap, and we fill the bitmap again.
    auto remaining_bytes = fsi_bitmap_size;
    fsi_bitmap.resize(fsi_bitmap_size);
    std::memcpy(fsi_bitmap.data(), page_data + sizeof(uint64_t), remaining_bytes);

    for (uint64_t i = 0; i < fsi_bitmap.size(); i++) {
        uint8_t stored_high_value = (fsi_bitmap[i] >> 4) & 0x0F; // Extract high 4 bits
        auto decoded_space_high = decode_free_space(
                stored_high_value); // Represents Fill Rate of the page in higher 4 bits
        if (required_space <= decoded_space_high) {
            buffer_manager.unfix_page(page, false);
            return (i * 2);
        }
        uint8_t stored_low_value = fsi_bitmap[i] & 0x0F; // Extract low 4 bits
        auto decoded_space_low = decode_free_space(
                stored_low_value); // Represents Fill Rate of the page in lower 4 bits
        if (required_space <= decoded_space_low) {
            buffer_manager.unfix_page(page, false);
            return (i * 2) + 1;
        }
    }
    // Unfix the page
   buffer_manager.unfix_page(page, false);
   return std::nullopt;
}