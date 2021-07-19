#include "riegeli/iouring/fd_io_uring.h"

namespace riegeli {

namespace ioUring {

bool IsIoUringAvailable() {
    struct io_uring test_ring;
    bool available = false;
    if(io_uring_queue_init(4, &test_ring, 0) == 0) {
        available = true;
        io_uring_queue_exit(&test_ring);
    }
    return available;
}

}
} // namespace riegeli