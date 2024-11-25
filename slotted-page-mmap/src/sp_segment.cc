#include "moderndbs/segment.h"
#include "moderndbs/slotted_page.h"

#include <cstring>

using moderndbs::Segment;
using moderndbs::SPSegment;
using moderndbs::TID;

SPSegment::SPSegment(uint16_t segment_id, FileMapper& file_mapper, SchemaSegment& schema, FSISegment& fsi,
                     schema::Table& table)
   : Segment(segment_id, file_mapper), schema(schema), fsi(fsi), table(table) {
   // TODO: add your implementation here
}

TID SPSegment::allocate(uint32_t size) {
   // Allocate a new record.
   // The allocate method should use the free-space inventory to find a suitable page quickly.
   fsi.fsi_mutex.lock();
   std::optional<uint64_t> page_with_free_space = fsi.find(size);
   if (page_with_free_space.has_value()) {
      uint64_t page_id = page_with_free_space.value();
      auto page = file_mapper.get_page(page_id, true);
      auto* page_data = page->get_data();

      // If the page is already there, it's sufficient to use reinterpret_cast<SlottedPage*>
      auto* slotted_page = reinterpret_cast<SlottedPage*>(page_data);
      auto slot_id = slotted_page->allocate(size, file_mapper.get_data_size());

      // Unfix the page
      file_mapper.release_page(page);

      // Update fsi
      fsi.update(page_id, slotted_page->get_free_space());

      auto tid = TID(page_id, slot_id);
      fsi.fsi_mutex.unlock();
      return tid;
   }

   //  Create new slotted page -> Update Number of Slotted Pages in Table
   auto new_page_id = table.allocated_pages++;
   auto page = file_mapper.get_page(new_page_id, true);
   auto* slotted_page = new (page->get_data()) SlottedPage(file_mapper.get_data_size());
   const uint16_t slot_id = slotted_page->allocate(size, file_mapper.get_data_size());

   const auto tid = TID(new_page_id, slot_id);

   // Unfix the page
   file_mapper.release_page(page);

   // update free_space_inventory
   fsi.update(new_page_id, slotted_page->header.free_space);
   fsi.fsi_mutex.unlock();

   // Returns a TID that stores the page as well as the slot of the allocated record.
   return tid;
}

std::optional<uint32_t> SPSegment::read(const TID tid, std::byte* record, const uint32_t capacity) const {
   // The tid.get_page_id() does the same functionality of the XOR that was used in the schema_segment and fsi_segment.
   // Hence, we just pass the result page_id to file_mapper
   auto page_id = tid.get_page_id(segment_id);

   // First, I need to find the slot that was previously allocated in allocate method. TID is used for that.
   auto page = file_mapper.get_page(page_id, false);

   // If the page is already there, it's sufficient to use reinterpret_cast<SlottedPage*>
   auto* slotted_page = reinterpret_cast<SlottedPage*>(page->get_data());
   auto& slot = slotted_page->get_slots()[tid.get_slot()];

   if (slot.is_empty()) {
      file_mapper.release_page(page);
      return std::nullopt;
   }

   if (slot.is_redirect()) {
      TID redirect_tid = slot.as_redirect_tid();
      file_mapper.release_page(page);
      return read(redirect_tid, record, capacity);
   }

   uint32_t read_bytes = std::min(slot.get_size(), capacity);
   memcpy(record, slotted_page->get_data() + slot.get_offset(), read_bytes);
   file_mapper.release_page(page);
   return read_bytes;
}

uint32_t SPSegment::write(TID tid, std::byte* record, uint32_t record_size, uint64_t lsn, bool is_update) {
   // The tid.get_page_id() does the same functionality of the XOR that was used in the schema_segment and fsi_segment.
   // Hence, we just pass the result page_id to file_mapper
   const auto page_id = tid.get_page_id(segment_id);

   // First, I need to find the slot that was previously allocated in allocate method. TID is used for that.
   auto page = file_mapper.get_page(page_id, true);

   // If the page is already there, it's sufficient to use reinterpret_cast<SlottedPage*>
   auto& slotted_page = *reinterpret_cast<SlottedPage*>(page->get_data());
   auto& slot = slotted_page.get_slots()[tid.get_slot()];
   if (is_update && slot.is_empty()) {
      file_mapper.release_page(page);
      throw std::logic_error("TID Not Found!");
   }

   if (slot.is_redirect()) {
      TID redirect_tid = slot.as_redirect_tid();
      file_mapper.release_page(page);
      return write(redirect_tid, record, record_size, lsn);
   }

   uint32_t written_bytes = std::min(slot.get_size(), record_size);
   auto offset = slot.get_offset(); // The place at which we write the data.
   memcpy(slotted_page.get_data() + offset, record, written_bytes);

   /// UPDATE LSN on page
   page->set_lsn(lsn);

   file_mapper.release_page(page);
   return written_bytes;
}

void SPSegment::resize(TID tid, uint32_t new_length) {
   // TODO: add your implementation here
   // The tid.get_page_id() does the same functionality of the XOR that was used in the schema_segment and fsi_segment.
   // Hence, we just pass the result page_id to file_mapper.
   auto page_id = tid.get_page_id(segment_id);
   auto slot_id = tid.get_slot();
   // First, I need to find the slot that was previously allocated in allocate method. TID is used for that.
   auto page = file_mapper.get_page(page_id, true);
   // If the page is already there, it's sufficient to use reinterpret_cast<SlottedPage*>
   auto& slotted_page = *reinterpret_cast<SlottedPage*>(page->get_data());
   auto& slot = slotted_page.get_slots()[slot_id];
   if (slot.is_redirect()) {
      //            TID old_redirect_tid = slot.as_redirect_tid();
      //            TID new_redirect_tid = resize(old_redirect_tid, new_length);
      //
      //            slot.set_redirect_tid(new_redirect_tid);
      //            file_mapper.unfix_page(page, old_redirect_tid.get_value() == new_redirect_tid.get_value());
   } else {
      if (new_length < slot.get_size()) {
         slotted_page.relocate(tid.get_slot(), new_length, file_mapper.get_page_size());
         // file_mapper.unfix_page(page, true);
      }

      if (slotted_page.get_free_space() < new_length) {
         TID new_tid = allocate(new_length);
         auto new_page = file_mapper.get_page(new_tid.get_page_id(segment_id), true);
         auto& new_slotted_page = *reinterpret_cast<SlottedPage*>(new_page->get_data());
         auto& new_slot = new_slotted_page.get_slots()[new_tid.get_slot()];
         new_slot.mark_as_redirect_target();

         // Copy data
         memcpy(new_slotted_page.get_data() + new_slot.get_offset(), slotted_page.get_data() + slot.get_offset(),
                slot.get_size());

         if (slot.is_redirect_target()) {
            slotted_page.erase(tid.get_slot());
            // file_mapper.unfix_page(page, true);
            // file_mapper.unfix_page(new_page, true);
         } else {
            slot.set_redirect_tid(new_tid);
            // file_mapper.unfix_page(page, true);
            // file_mapper.unfix_page(new_page, true);
         }
      } else {
         slotted_page.relocate(tid.get_slot(), new_length, file_mapper.get_page_size());
         // file_mapper.unfix_page(page, true);
      }
   }
}

bool SPSegment::erase(TID tid, uint64_t lsn) {
   fsi.fsi_mutex.lock();
   // tid.get_page_id() does the same functionality of the XOR that was used in the schema_segment and fsi_segment.
   // Hence, we just pass the result page_id to file_mapper
   // First, I need to find the slot that was previously allocated in allocate method. TID is used for that.
   auto page = file_mapper.get_page(tid.get_page_id(segment_id), true);

   // If the page is already there, it's sufficient to use reinterpret_cast<SlottedPage*>
   auto& slotted_page = *reinterpret_cast<SlottedPage*>(page->get_data());
   const auto& slot = slotted_page.get_slots()[tid.get_slot()];

   if (slot.is_empty()) {
      file_mapper.release_page(page);
      return false;
   }

   if (slot.is_redirect()) {
      file_mapper.release_page(page);
      return erase(slot.as_redirect_tid(), lsn);
   }
   slotted_page.erase(tid.get_slot());

   /// UPDATE LSN on page
   page->set_lsn(lsn);

   file_mapper.release_page(page);
   fsi.update(tid.get_page_id(segment_id), slotted_page.get_free_space());
   fsi.fsi_mutex.unlock();
   return true;
}