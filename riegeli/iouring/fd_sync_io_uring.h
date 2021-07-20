#ifndef RIEGELI_IOURING_FD_SYNC_IO_URING_H_
#define RIEGELI_IOURING_FD_SYNC_IO_URING_H_

#include "riegeli/iouring/fd_io_uring_options.h"
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

        ~FdSyncIoUring() override;

        // Override the file operation interface for Io_Uring.
        ssize_t pread(int fd, void *buf, size_t count, off_t offset) override;

        ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) override;

        ssize_t preadv(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) override;

        ssize_t pwritev(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) override;

        int fsync(int fd) override;

        // Interface for register and unregister fd.
        void RegisterFd(int fd) override;

        void UnRegisterFd() override;

        IoUringMode Mode() override {
            return IoUringMode::SYNCIOURING;
        }

        // Get Io_Uring settings. 
        bool fd_register() override {
            return fd_register_;
        }

        uint32_t size() override {
            return size_;
        }

        int fd() override {
            return fd_;
        }

    private:
        // Initilize Io_Uring.
        bool InitIoUring();

        // Update registered fd.
        void UpdateFd();

        // Get sqe.
        struct io_uring_sqe* GetSqe();

        // Submit sqe to kernel.
        ssize_t SubmitAndGetResult();

    private:
        // Io_Uring entrance and set up params.
        struct io_uring_params params_;
        struct io_uring ring_;

        // Io_Uring settings.
        bool fd_register_ = false;
        uint32_t size_ = 0;
        int fd_ = -1;
};

}  // namespace riegeli

#endif  // RIEGELI_IOURING_FD_SYNC_IO_URING_H_S