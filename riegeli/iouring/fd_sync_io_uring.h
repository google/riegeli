#ifndef RIEGELI_IOURING_FD_SYNC_IO_URING_H_
#define RIEGELI_IOURING_FD_SYNC_IO_URING_H_

#include "riegeli/iouring/fd_io_uring.h"

namespace riegeli {
// Perform Io_Uring synchronously.
class FdSyncIoUring : public FdIoUring {
    public:
        // Constructor and destructor for this class.
        FdSyncIoUring() = delete;
        FdSyncIoUring(const FdSyncIoUring&) = delete;
        FdSyncIoUring& operator=(const FdSyncIoUring&) = delete;
        
        explicit FdSyncIoUring(FdIoUringOptions options, int fd = -1);

        ~FdSyncIoUring() override {}

        // Override the file operation interface for Io_Uring.
        ssize_t pread(int fd, void *buf, size_t count, off_t offset) override;

        ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) override;

        int fsync(int fd) override;

        // Get the mode of Io_Uring.
        IoUringMode Mode() override {
            return IoUringMode::SYNCIOURING;
        }

    private:
        // Get sqe.
        struct io_uring_sqe* GetSqe();

        // Submit sqe to kernel.
        ssize_t SubmitAndGetResult();
};

}  // namespace riegeli

#endif  // RIEGELI_IOURING_FD_SYNC_IO_URING_H_S