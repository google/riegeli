#ifndef RIEGELI_IOURING_FD_ASYNC_IO_URING_H_
#define RIEGELI_IOURING_FD_ASYNC_IO_URING_H_

#include "riegeli/iouring/fd_io_uring_options.h"
#include "riegeli/iouring/fd_io_uring.h"

namespace riegeli {

// Perform Io_Uring synchronously.
class FdAsyncIoUring : public FdIoUring {
    public:
        // Constructor and destructor for this class.
        FdAsyncIoUring() = delete;
        FdAsyncIoUring(const FdAsyncIoUring&) = delete;
        FdAsyncIoUring& operator=(const FdAsyncIoUring&) = delete;
        
        explicit FdAsyncIoUring(FdIoUringOptions options, int fd = -1);

        ~FdAsyncIoUring() override;

        // Override the file operation interface for Io_Uring.
        ssize_t pread(int fd, void *buf, size_t count, off_t offset) override;

        ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) override;

        ssize_t preadv(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) override;

        ssize_t pwritev(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) override;

        int fsync(int fd) override;

        void RegisterFd(int fd) override;

        void UnRegisterFd() override;

        std::string Mode() override;

        // Get Io_Uring settings. 
        bool poll_io() {
            return poll_io_;
        }

        bool fd_register() {
            return fd_register_;
        }

        uint32_t size() {
            return size_;
        }

        int fd() {
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

        struct io_uring_params params_;
        struct io_uring ring_;

        bool poll_io_;
        bool fd_register_ = false;
        uint32_t size_;
        int fd_ = -1;
};

}  // namespace riegeli

#endif  // RIEGELI_IOURING_FD_ASYNC_IO_URING_H_