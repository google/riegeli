#ifndef RIEGELI_IOURING_FD_IO_URING_H_
#define RIEGELI_IOURING_FD_IO_URING_H_

#include <stddef.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <string>
#include <utility>

#include "liburing.h"
#include "syscall.h"

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"

#include "riegeli/base/base.h"
#include "riegeli/base/object.h"
#include "riegeli/iouring/fd_io_uring_options.h"

namespace riegeli {

namespace ioUring {

bool IsIoUringAvailable();

}

// The base class for sync or async Io_Uring.
class FdIoUring : public Object {
public:
    enum class IoUringMode {
        ASYNCIOURING,
        SYNCIOURING,
    };

public:
    FdIoUring() = delete;
    FdIoUring(const FdIoUring&) = delete;
    FdIoUring& operator=(const FdIoUring&) = delete;

protected:
    FdIoUring(FdIoUringOptions options, int fd);
    
public:
    virtual ~FdIoUring();

    // The interface for file operation for Io_Uring.
    virtual ssize_t pread(int fd, void *buf, size_t count, off_t offset) = 0;

    virtual ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) = 0;

    virtual int fsync(int fd) = 0;

    // Get the mode of Io_Uring.
    virtual IoUringMode Mode() = 0;

    // Pre-register or unregister file descriptor for Io_Uring.
    void RegisterFd(int fd);

    void UnRegisterFd();

    // Get Io_Uring settings. 
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
    // Init.
    bool Init(bool fd_register, int fd);

    // Update registered fd.
    void UpdateFd();

protected:
    // Fail Operation
    ABSL_ATTRIBUTE_COLD bool FailOperation(const int error_number, absl::string_view operation);

protected:
    // Io_Uring entrance and set up params.
    struct io_uring_params params_;
    struct io_uring ring_;

    // Io_Uring settings.
    bool fd_register_ = false;
    uint32_t size_ = 0;
    int fd_ = -1;
};

}  // namespace riegeli

#endif  // RIEGELI_IOURING_FD_IO_URING_H_