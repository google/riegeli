#include "riegeli/iouring/fd_io_uring_options.h"

namespace riegeli {

uint32_t FdIoUringOptions::RoundUpToNextPowerTwo(uint32_t size) {
    if(size == 0) {
        return size;
    }

    --size;
    size |= size >> 1;
    size |= size >> 2;
    size |= size >> 4;
    size |= size >> 8;
    size |= size >> 16;
    if(size + 1 > 4096) {
        return 4096;
    }
    return size + 1;
}

} // namespace riegeli
