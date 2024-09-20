#include "moderndbs/segment.h"
#include "moderndbs/slotted_page.h"

#include <cstring>

using moderndbs::Segment;
using moderndbs::SPSegment;
using moderndbs::TID;

SPSegment::SPSegment(uint16_t segment_id, BufferManager &buffer_manager, SchemaSegment &schema, FSISegment &fsi,
                     schema::Table &table)
        : Segment(segment_id, buffer_manager), schema(schema), fsi(fsi), table(table) {
    // TODO: add your implementation here
}


TID SPSegment::allocate(uint32_t size) {
    // TODO: add your implementation here
    // Allocate a new record.
    // The allocate method should use the free-space inventory to find a suitable page quickly.
    std::optional<uint64_t> page_with_free_space = fsi.find(size);
    if (page_with_free_space.has_value()) {
       uint64_t page_id = page_with_free_space.value();
       auto& page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ page_id, true);
       auto* page_data = page.get_data();
       // If the page is already there, it's sufficient to use reinterpret_cast<SlottedPage*>
       auto* slotted_page = reinterpret_cast<SlottedPage*>(page_data);
       auto slot_id = slotted_page->allocate(size, buffer_manager.get_page_size());
       // Unfix the page
       buffer_manager.unfix_page(page, true);
       // Update fsi
       fsi.update(page_id, slotted_page->get_free_space());

       TID tid = TID(page_id, slot_id);
       return tid;
    }

    // There is no available page that has enough space to hold data of "size" -> Create new slotted page
    // Update Number of Slotted Pages
    auto& first_page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48), true);
    auto& number_of_slots = *reinterpret_cast<uint64_t*>(first_page.get_data());
    auto new_page_id = table.allocated_pages++; // Adjust number of allocated pages in Table
    number_of_slots = table.allocated_pages;
    buffer_manager.unfix_page(first_page, true);

    auto& page = buffer_manager.fix_page((static_cast<uint64_t>(segment_id) << 48) ^ new_page_id, true);
    auto* slotted_page = new (page.get_data()) SlottedPage(buffer_manager.get_page_size());

    uint16_t slot_id = slotted_page->allocate(size, buffer_manager.get_page_size());
    // Unfix the page
    buffer_manager.unfix_page(page, true);
    // update free_space_inventory
    fsi.update(new_page_id, slotted_page->header.free_space);

    // Returns a TID that stores the page as well as the slot of the allocated record.
    TID tid = TID(new_page_id, slot_id);
    return tid;
}

std::optional<uint32_t> SPSegment::read(TID tid, std::byte *record, uint32_t capacity) const {
    // TODO: add your implementation here
    // The tid.get_page_id() does the same functionality of the XOR that was used in the schema_segment and fsi_segment.
    // Hence, we just pass the result page_id to buffer_manager
    auto page_id = tid.get_page_id(segment_id);
    // First, I need to find the slot that was previously allocated in allocate method. TID is used for that.
    auto &page = buffer_manager.fix_page(page_id, false);
    // If the page is already there, it's sufficient to use reinterpret_cast<SlottedPage*>
    auto *slotted_page = reinterpret_cast<SlottedPage *>(page.get_data());
    auto &slot = slotted_page->get_slots()[tid.get_slot()];

    if (slot.is_empty()) {
        return std::nullopt;
    }
    if (slot.is_redirect()) {
       TID redirect_tid = slot.as_redirect_tid();
       buffer_manager.unfix_page(page, false);
       return read(redirect_tid, record, capacity);
    }
   // Not a redirect slot.
    uint32_t read_bytes = std::min(slot.get_size(), capacity);
    memcpy(record, slotted_page->get_data() + slot.get_offset(), read_bytes);
    buffer_manager.unfix_page(page, false);
    return read_bytes;
}

uint32_t SPSegment::write(TID tid, std::byte *record, uint32_t record_size, bool is_update) {
    // TODO: add your implementation here
    // The tid.get_page_id() does the same functionality of the XOR that was used in the schema_segment and fsi_segment.
    // Hence, we just pass the result page_id to buffer_manager
    auto page_id = tid.get_page_id(segment_id);
    // First, I need to find the slot that was previously allocated in allocate method. TID is used for that.
    auto &page = buffer_manager.fix_page(page_id, true);
    // If the page is already there, it's sufficient to use reinterpret_cast<SlottedPage*>
    auto &slotted_page = *reinterpret_cast<SlottedPage *>(page.get_data());
    auto &slot = slotted_page.get_slots()[tid.get_slot()];
    if (is_update && slot.is_empty()) {
        throw std::logic_error("TID Not Found!");
    }

    if (slot.is_redirect()) {
        TID redirect_tid = slot.as_redirect_tid();
        buffer_manager.unfix_page(page, true);
        return write(redirect_tid, record, record_size);
    } else {
        uint32_t written_bytes = std::min(slot.get_size(), record_size);
        auto offset = slot.get_offset(); // The place at which we write the data.
        memcpy(slotted_page.get_data() + offset, record, written_bytes);
        buffer_manager.unfix_page(page, true);
        return written_bytes;
    }
}

void SPSegment::resize(TID tid, uint32_t new_length) {
    // TODO: add your implementation here
    // The tid.get_page_id() does the same functionality of the XOR that was used in the schema_segment and fsi_segment.
    // Hence, we just pass the result page_id to buffer_manager.
    auto page_id = tid.get_page_id(segment_id);
    auto slot_id = tid.get_slot();
    // First, I need to find the slot that was previously allocated in allocate method. TID is used for that.
    auto &page = buffer_manager.fix_page(page_id, true);
    // If the page is already there, it's sufficient to use reinterpret_cast<SlottedPage*>
    auto &slotted_page = *reinterpret_cast<SlottedPage *>(page.get_data());
    auto &slot = slotted_page.get_slots()[slot_id];
    if (slot.is_redirect()) {
        //            TID old_redirect_tid = slot.as_redirect_tid();
        //            TID new_redirect_tid = resize(old_redirect_tid, new_length);
        //
        //            slot.set_redirect_tid(new_redirect_tid);
        //            buffer_manager.unfix_page(page, old_redirect_tid.get_value() == new_redirect_tid.get_value());
    } else {
        if (new_length < slot.get_size()) {
            slotted_page.relocate(tid.get_slot(), new_length, buffer_manager.get_page_size());
            buffer_manager.unfix_page(page, true);
        }

        if (slotted_page.get_free_space() < new_length) {
            TID new_tid = allocate(new_length);
            auto &new_page = buffer_manager.fix_page(new_tid.get_page_id(segment_id), true);
            auto &new_slotted_page = *reinterpret_cast<SlottedPage *>(new_page.get_data());
            auto &new_slot = new_slotted_page.get_slots()[new_tid.get_slot()];
            new_slot.mark_as_redirect_target();

            // Copy data
            memcpy(new_slotted_page.get_data() + new_slot.get_offset(), slotted_page.get_data() + slot.get_offset(),
                   slot.get_size());

            if (slot.is_redirect_target()) {
                slotted_page.erase(tid.get_slot());
                buffer_manager.unfix_page(page, true);
                buffer_manager.unfix_page(new_page, true);
            } else {
                slot.set_redirect_tid(new_tid);
                buffer_manager.unfix_page(page, true);
                buffer_manager.unfix_page(new_page, true);
            }
        } else {
            slotted_page.relocate(tid.get_slot(), new_length, buffer_manager.get_page_size());
            buffer_manager.unfix_page(page, true);
        }
    }
}

bool SPSegment::erase(TID tid) {
    // TODO: add your implementation here
    // tid.get_page_id() does the same functionality of the XOR that was used in the schema_segment and fsi_segment.
    // Hence, we just pass the result page_id to buffer_manager
    // First, I need to find the slot that was previously allocated in allocate method. TID is used for that.
    auto &page = buffer_manager.fix_page(tid.get_page_id(segment_id), true);
    // If the page is already there, it's sufficient to use reinterpret_cast<SlottedPage*>
    auto &slotted_page = *reinterpret_cast<SlottedPage *>(page.get_data());
    auto &slot = slotted_page.get_slots()[tid.get_slot()];

    if (slot.is_empty()) {
        return false;
    }
    if (slot.is_redirect()) {
        return erase(slot.as_redirect_tid());
    }
    slotted_page.erase(tid.get_slot());
    buffer_manager.unfix_page(page, true);
    fsi.update(tid.get_page_id(segment_id), slotted_page.get_free_space());
    return true;
}