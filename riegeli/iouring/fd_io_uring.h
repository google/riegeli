#ifndef RIEGELI_IOURING_FD_IO_URING_H_
#define RIEGELI_IOURING_FD_IO_URING_H_

#include <stddef.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <utility>

namespace riegeli {

// The base interface class for sync or async Io_Uring.
class FdIoUring {
public:
    virtual ~FdIoUring() {}

    // The interface for file operation for Io_Uring.
    virtual ssize_t pread(int fd, void *buf, size_t count, off_t offset) = 0;

    virtual ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) = 0;

    virtual ssize_t preadv(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) = 0;

    virtual ssize_t pwritev(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) = 0;

    virtual int fsync(int fd) = 0;

    // Pre-register or unregister file descriptor for Io_Uring.
    virtual void RegisterFd(int fd) = 0;

    virtual void UnRegisterFd() = 0;
};



}  // namespace riegeli

#endif  // RIEGELI_IOURING_FD_IO_URING_H_