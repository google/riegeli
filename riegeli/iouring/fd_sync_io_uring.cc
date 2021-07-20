#include "riegeli/iouring/fd_sync_io_uring.h"

#include "riegeli/base/base.h"

#include <cstring>
#include <stddef.h>

namespace riegeli {

FdSyncIoUring::FdSyncIoUring(FdIoUringOptions options, int fd) 
: size_(options.size()) {
    
    memset(&ring_, 0, sizeof(ring_));
    memset(&params_, 0, sizeof(params_));

    RIEGELI_ASSERT(InitIoUring()) << "Failed initilization of Io_Uring. (FdSyncIoUring)";

    if(options.fd_register()) {
        RegisterFd(fd);
    }
}

FdSyncIoUring::~FdSyncIoUring() {
    io_uring_queue_exit(&ring_);
}

bool FdSyncIoUring::InitIoUring() {

    if(io_uring_queue_init_params(size_, &ring_, &params_) != 0) {
        return false;
    }
    
    return true;
}

void FdSyncIoUring::RegisterFd(int fd) {
    fd_ = fd;
    
    if(fd_register_ == false) {
        RIEGELI_ASSERT_EQ(io_uring_register_files(&ring_, &fd_, 1), 0) << "Failed fd register.";
        fd_register_ = true;
    } else {
        UpdateFd();
    }
}

void FdSyncIoUring::UnRegisterFd() {
    fd_ = -1;
    RIEGELI_ASSERT_EQ(io_uring_unregister_files(&ring_), 0) << "Failed fd unregister.";
    fd_register_ = false;
}

void FdSyncIoUring::UpdateFd() {
    RIEGELI_ASSERT_EQ(io_uring_register_files_update(&ring_, 0, &fd_, 1), 1) << "Failed fd update.";
}

ssize_t FdSyncIoUring::pread(int fd, void *buf, size_t count, off_t offset) {
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        io_uring_prep_read(sqe, 0, buf, count, offset);
        sqe -> flags |= IOSQE_FIXED_FILE;
    } else {
        io_uring_prep_read(sqe, fd, buf, count, offset);
    }
    ssize_t res = SubmitAndGetResult();
    return res;
}

ssize_t FdSyncIoUring::pwrite(int fd, const void *buf, size_t count, off_t offset) {
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        io_uring_prep_write(sqe, 0, buf, count, offset);
        sqe -> flags |= IOSQE_FIXED_FILE;
    } else {
        io_uring_prep_write(sqe, fd, buf, count, offset);
    }
    ssize_t res = SubmitAndGetResult();
    return res;
}

ssize_t FdSyncIoUring::preadv(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) {
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        io_uring_prep_readv(sqe, 0, iov, iovcnt, offset);
        sqe -> flags |= IOSQE_FIXED_FILE;
    } else {
        io_uring_prep_readv(sqe, fd, iov, iovcnt, offset);
    }
    ssize_t res = SubmitAndGetResult();
    return res;
}

ssize_t FdSyncIoUring::pwritev(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) {
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        io_uring_prep_writev(sqe, 0, iov, iovcnt, offset);
        sqe -> flags |= IOSQE_FIXED_FILE;
    } else {
        io_uring_prep_writev(sqe, fd, iov, iovcnt, offset);
    }
    ssize_t res = SubmitAndGetResult();
    return res;
}

int FdSyncIoUring::fsync(int fd) {
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        io_uring_prep_fsync(sqe, 0, 0);
        sqe -> flags |= IOSQE_FIXED_FILE;
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
    RIEGELI_ASSERT_EQ(io_uring_wait_cqe(&ring_, &cqe), 0) << "Failed to get a cqe";
    ssize_t res = cqe -> res;
    io_uring_cqe_seen(&ring_, cqe);
    return res;
}

} // namespace riegeli
