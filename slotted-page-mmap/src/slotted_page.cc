#include "moderndbs/slotted_page.h"
#include <cstring>
#include <stdexcept>

using moderndbs::SlottedPage;

SlottedPage::Header::Header(uint32_t page_size)
        : slot_count(0),
          first_free_slot(0),
          data_start(page_size),
          free_space(page_size - sizeof(Header)) {}

SlottedPage::SlottedPage(uint32_t page_size)
        : header(page_size) {
    std::memset(get_data() + sizeof(SlottedPage), 0x00, page_size - sizeof(SlottedPage));
}

std::byte *SlottedPage::get_data() {
    return reinterpret_cast<std::byte *>(this);
}

const std::byte *SlottedPage::get_data() const {
    return reinterpret_cast<const std::byte *>(this);
}

//This non-const version allows modification of the slots in the page.
SlottedPage::Slot *SlottedPage::get_slots() {
    //   This expression calculates the starting address of the slot array within the page
    //   by first obtaining the address of the page's data (get_data()),
    //   then adding the size of the SlottedPage structure itself (sizeof(SlottedPage)),
    //   and finally casting the result to a pointer to Slot.
    return reinterpret_cast<Slot *>(get_data() + sizeof(SlottedPage));
}

//This const version returns a pointer to constant slots, meaning the caller cannot modify the slots through this pointer.
const SlottedPage::Slot *SlottedPage::get_slots() const {
    //   This expression calculates the starting address of the slot array within the page
    //   by first obtaining the address of the page's data (get_data()),
    //   then adding the size of the SlottedPage structure itself (sizeof(SlottedPage)),
    //   and finally casting the result to a pointer to Slot.
    return reinterpret_cast<const Slot *>(get_data() + sizeof(SlottedPage));
}

uint32_t SlottedPage::get_fragmented_free_space() {
    // TODO: add your implementation here
    // This is the effective free space that Professor Neumann talked about in the lecture
    // Full Equation: EffectiveFreeSpace = PageSize - HeaderSize - SlotCount * SlotSize - (PageSize - DataStart).
    // This translates to
    return header.data_start - sizeof(Header) - header.slot_count * sizeof(Slot);
}

uint16_t SlottedPage::allocate(uint32_t data_size, uint32_t page_size) {

    // TODO: add your implementation here
    if (get_free_space() < data_size) {
        throw std::logic_error{"not enough free space"};
    }

    if (get_fragmented_free_space() < data_size) {
        compactify(page_size);
    }

    // Update Header attributes
    auto slot_id = header.first_free_slot;

    // Update data_start -> It should be (old data start - data_size)
    header.data_start -= data_size;
    // Update free_space -> It should be (old free space - data_size) , WE MIGHT NEED (page_size).
    //WE subtract slot_size if we are adding a new slot. But if we are reusing an old slot, we don't subtract slot size again.
    header.free_space -= data_size;

    // Get Slot from slots array
    auto &slot = get_slots()[slot_id];

    // Set new Slot attributes
    slot.set_slot(header.data_start, data_size, false);

    // Update First Free Slot -> Iterate over slot array until the first NON-empty slot to consider erased entries
    // When an entry is erased, the first free slot becomes in the middle.
    // Hence, if we need to search for the following NON-empty slot or if the current slot_count was equal to first free slot,
    // then normally increase first free slot.
    if (slot_id == header.slot_count) {
        // Increase Slot Count
        header.slot_count++;
        header.first_free_slot++;
        header.free_space -= sizeof(Slot);
    } else {
        // Search for another place that can be first free slot. We are mainly search for an empty slot that was previously erased.
        for (uint16_t i = slot_id + 1; i < header.slot_count; i++) {
            auto &s = get_slots()[i];
            if (s.is_empty()) {
                header.first_free_slot = i;
                break;
            }
        }
        // If we couldn't find any empty slot that was previously erased. The first free slot becomes the header.slot_count.
        if (header.first_free_slot == slot_id) { // Could not find any erased slot
            header.first_free_slot = header.slot_count;
        }
    }

    // Return the new slot
    return slot_id;

}

void SlottedPage::relocate(uint16_t slot_id, uint32_t data_size, uint32_t page_size) {
    // TODO: add your implementation here
    auto &slot = get_slots()[slot_id];

    // In relocation, we have 3 possibilities.
    // Possibility 1: data_size is smaller than, the previous data_size. Relocate in place.
    if (data_size <= slot.get_size()) {
        // Update Header Values -> Add the difference between original size and new size to the free space.
        header.free_space += (slot.get_size() - data_size);
        slot.set_slot(slot.get_offset(), data_size, slot.is_redirect_target()); // Update Slot
        return;
    }

    uint32_t additional_size = data_size - slot.get_size();

    // This condition checks whether the record associated with the slot is located at the very start of the free data area within the slotted page.
    // As records are deleted and others are resized, the page can become increasingly fragmented.
    // If the current record is not at the start of the free space (header.data_start), it might be advantageous to move it to reduce fragmentation and gain more contiguous memory.
    if (header.data_start != slot.get_offset()) {
        // Possibility 2: data_size is bigger than previously allocated data_size, BUT the page still have enough free space.
        if (data_size <= get_free_space()) {
            // We need to see if we have enough free fragmented space in the page, we still can allocate it.
            if (get_fragmented_free_space() <
                data_size) { // Check if we can locate the new data on current page after compactification.
                compactify(page_size);
            }
            // Update Header values
            header.free_space -= additional_size;
            header.data_start -= data_size;

            // We relocate the record at offset (header.data_start), so we shift records to free the old place
            memcpy(get_data() + header.data_start, get_data() + slot.get_offset(), slot.get_size());
            // Update Slot
            slot.set_slot(header.data_start, data_size, slot.is_redirect_target());
            return;
        }
            // Possibility 3: data_size is bigger than previously allocated data_size AND page does not have enough free space. MAKE Slot a FORWARD Record that references the data in another page
            // using TID: (page_id, slot_id)
        else {
            if (get_fragmented_free_space() < slot.get_size()) {
                throw std::logic_error{"not enough free space to reallocate record"};
            }

            // Move record to front of data
            header.data_start -= slot.get_size();
            memcpy(get_data() + header.data_start, get_data() + slot.get_offset(), slot.get_size());
            slot.set_slot(header.data_start, slot.get_size(), slot.is_redirect_target());

            // When a record is being redirected to another page because it was resized, the original tuple id needs to be stored in the first 8 bytes of the data (not the slot), is this correct?
            // This means on the new page we need to allocate 8 more bytes than the original size of the data.
            // Also, when reading from and writing to a redirection target, read/write methods need to skip the first 8 bytes.
            if (get_fragmented_free_space() <= additional_size) {
                compactify(page_size);
            }

            header.free_space -= additional_size;
            header.data_start -= data_size;

            for (uint32_t chunk = 0; chunk < slot.get_size(); chunk += additional_size) {
                // Copy data forward in small chunks
                memcpy(get_data() + header.data_start + chunk, get_data() + slot.get_offset() + chunk,
                       std::min(additional_size, slot.get_size() - chunk));
            }

            // Update Slot
            slot.set_slot(header.data_start, data_size, slot.is_redirect_target());
        }
    }
}

void SlottedPage::erase(uint16_t slot_id) {
    // TODO: add your implementation here
    auto *slot = get_slots() + slot_id;
    // Differentiate between Erasing entry in the beginning --> NOT SURE ABOUT /end YET

    // When erasing an entry in the middle, we DON'T update (data_start) AND (slot_count).
    // Since this is not the last entry, we keep the slot as it is. And we only reclaim the data_size to free space.
    // We Don't reclaim the slot size.
    header.free_space += slot->get_size();

    // We update the data_start ONLY when offset = header.data_start
    if (header.data_start == slot->get_offset()) {
        header.data_start += slot->get_size();
    }

    // Update Slot; we no longer need slot details (size, offset)
    slot->clear();
    // The erased slot would be the first free slot if the slot_id is before first_free_slot
    if (slot_id < header.first_free_slot) {
        header.first_free_slot = slot_id;
    }

    // Erasing entry at index (header.slot_count - 1) NOT iN THE MIDDLE
    // If it's the last slot, we erase it and update the values in the header.
    if (slot_id == header.slot_count - 1) {
        for (int32_t i = slot_id; i >= 0; i--) {
            // Update Header
            slot = get_slots() + slot_id;
            if (slot->is_empty()) {
                header.slot_count--;
                header.first_free_slot = header.slot_count;
                header.free_space += sizeof(Slot);
            }
        }
    }
}

//void SlottedPage::compactify(uint32_t page_size) {
//   // TODO: add your implementation here
//
//   auto* slots = get_slots();
//
//   uint32_t cur_datat_start = page_size;
//
//   for (int i = 0; i < header.slot_count; ++i) {
//      Slot& slot = slots[i];
//      if (slot.is_empty() && slot.is_redirect()) {
//         continue;
//      }
//      cur_datat_start -= slot.get_size();
//
//      if (cur_datat_start != slot.get_offset()) {
//         // TODO: COPY DATA
//
//         // Update the slot with the new offset
//         slot.set_slot(cur_datat_start, slot.get_size(), slot.is_redirect_target());
//      }
//   }
//   header.data_start = cur_datat_start;
//   header.free_space = header.data_start - sizeof(Header) - sizeof(Slot) * header.slot_count;
//}

void SlottedPage::compactify(uint32_t page_size) {
    uint32_t data_start = page_size;

    while (true) {
        uint32_t offset = 0;
        uint16_t slot_id = 0xFF;

        for (uint16_t i = 0; i < header.slot_count; i++) {
            const auto &slot = get_slots()[i];

            if (slot.is_empty() || slot.is_redirect()) {
                continue;
            }

            if (slot.get_offset() > offset && slot.get_offset() < data_start) {
                offset = slot.get_offset();
                slot_id = i;
            }
        }

        if (slot_id == 0xFF) {
            break;
        }

        auto &slot = get_slots()[slot_id];

        uint32_t free_space_back = data_start - (slot.get_offset() + slot.get_size());

        data_start -= slot.get_size();

        if (free_space_back == 0) {
            continue;
        }

        int32_t chunk_offset = slot.get_size() - free_space_back;
        for (; chunk_offset >= 0; chunk_offset -= free_space_back) {
            memcpy(get_data() + data_start + chunk_offset, get_data() + slot.get_offset() + chunk_offset,
                   free_space_back);
        }
        if (chunk_offset < 0) {
            memcpy(get_data() + data_start, get_data() + slot.get_offset(), free_space_back + chunk_offset);
        }

        slot.set_slot(data_start, slot.get_size(), slot.is_redirect_target());
    }

    header.data_start = data_start;
}

