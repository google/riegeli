#include "riegeli/iouring/fd_async_io_uring.h"

#include "riegeli/base/base.h"

#include <cstring>
#include <stddef.h>

namespace riegeli {

FdAsyncIoUring::FdAsyncIoUring(FdIoUringOptions options, int fd) 
: poll_io_(options.poll_io()), size_(options.size()) {
    
    memset(&ring_, 0, sizeof(ring_));
    memset(&params_, 0, sizeof(params_));

    poll_io_ = options.poll_io();
    if(poll_io_) {
        params_.flags |= IORING_SETUP_IOPOLL;
    }

    bool flag = false;
    RIEGELI_ASSERT((flag = InitIoUring())) << "Failed initilization of Io_Uring. (FdAsyncIoUring)";

    if(fd_register_) {
        RegisterFd(fd);
    }
}

FdAsyncIoUring::~FdAsyncIoUring() {
    io_uring_queue_exit(&ring_);
}

bool FdAsyncIoUring::InitIoUring() {

    if(io_uring_queue_init_params(size_, &ring_, &params_) != 0) {
        return false;
    }
    
    return true;
}

void FdAsyncIoUring::RegisterFd(int fd) {
    fd_ = fd;
    
    if(fd_register_ == false) {
        RIEGELI_ASSERT_EQ(io_uring_register_files(&ring_, &fd_, 1), 1) << "Failed fd register.";
        fd_register_ = true;
    } else {
        UpdateFd();
    }
}

void FdAsyncIoUring::UnRegisterFd() {
    fd_ = -1;
    RIEGELI_ASSERT_EQ(io_uring_unregister_files(&ring_), 0) << "Failed fd unregister.";
    fd_register_ = false;
}

void FdAsyncIoUring::UpdateFd() {
    RIEGELI_ASSERT_EQ(io_uring_register_files_update(&ring_, 0, &fd_, 1), 1) << "Failed fd update.";
}

int FdAsyncIoUring::mode() {
    return 1;
}

ssize_t FdAsyncIoUring::pread(int fd, void *buf, size_t count, off_t offset) {
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        io_uring_prep_read(sqe, 0, buf, count, offset);
    } else {
        io_uring_prep_read(sqe, fd, buf, count, offset);
    }
    ssize_t res = SubmitAndGetResult();
    return res;
}

ssize_t FdAsyncIoUring::pwrite(int fd, const void *buf, size_t count, off_t offset) {
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        io_uring_prep_write(sqe, 0, buf, count, offset);
    } else {
        io_uring_prep_write(sqe, fd, buf, count, offset);
    }
    ssize_t res = SubmitAndGetResult();
    return res;
}

ssize_t FdSyncIoUring::preadv(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) {
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        io_uring_prep_readv(sqe, 0, iov, iovcnt, offset);
    } else {
        io_uring_prep_readv(sqe, fd, iov, iovcnt, offset);
    }
    ssize_t res = SubmitAndGetResult();
    return res;
}

ssize_t FdSyncIoUring::pwritev(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) {
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        io_uring_prep_writev(sqe, 0, iov, iovcnt, offset);
    } else {
        io_uring_prep_writev(sqe, fd, iov, iovcnt, offset);
    }
    ssize_t res = SubmitAndGetResult();
    return res;
}

int FdSyncIoUring::fsync(int fd) {
    struct io_uring_sqe *sqe  = GetSqe();
    io_uring_prep_fsync(sqe, fd, 0);
    if(fd_register_) {
        io_uring_prep_fsync(sqe, 0, 0);
    } else {
        io_uring_prep_fsync(sqe, fd, 0);
    }
    ssize_t res = SubmitAndGetResult();
    return res;
}

inline struct io_uring_sqe* FdSyncIoUring::GetSqe() {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    RIEGELI_ASSERT(!!sqe) << "Failed get a sqe.";
    return sqe;
}

inline ssize_t FdSyncIoUring::SubmitAndGetResult() {
    RIEGELI_ASSERT_GT(io_uring_submit(&ring_), 0) << "Failed to submit the sqe.";
    struct io_uring_cqe* cqe = NULL;
    io_uring_wait_cqe(&ring_, &cqe);
    return cqe -> res;
}

} // namespace riegeli
