#ifndef INCLUDE_TRANSFUSE_SEGMENT_H_
#define INCLUDE_TRANSFUSE_SEGMENT_H_

#include <cstdint>
#include <transfuse/file_mapper.h>
// #include <vector>
// #include <cstring>

namespace transfuse {

    class Segment {
    public:
        /// Constructor.
        /// @param[in] segment_id       Id of the segment.
        /// @param[in] file_mapper   The file mapper that should be used by the segment.
        Segment(const uint16_t segment_id, FileMapper &file_mapper)
            : segment_id(segment_id), file_mapper(file_mapper) {
        }

        /// The segment id
        uint16_t segment_id;

    protected:
        /// The file mapper
        FileMapper &file_mapper;
    };
} // namespace transfuse

#endif // INCLUDE_TRANSFUSE_SEGMENT_H_
