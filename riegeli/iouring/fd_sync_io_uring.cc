#include "riegeli/iouring/fd_sync_io_uring.h"

namespace riegeli {

FdSyncIoUring::FdSyncIoUring(FdIoUringOptions options, int fd) 
: FdIoUring(options, fd) {}

ssize_t FdSyncIoUring::pread(int fd, void *buf, size_t count, off_t offset) {
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        io_uring_prep_read(sqe, 0, buf, count, offset);
        sqe -> flags |= IOSQE_FIXED_FILE;
    } else {
        io_uring_prep_read(sqe, fd, buf, count, offset);
    }
    const ssize_t res = SubmitAndGetResult();
    if(ABSL_PREDICT_FALSE(res < 0)) {
        if(res != -EINTR && res != -EAGAIN) {
            FailOperation(-res, "pread()");
        }
    }
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
    const ssize_t res = SubmitAndGetResult();
    if(ABSL_PREDICT_FALSE(res < 0)) {
        if(res != -EINTR && res != -EAGAIN) {
            FailOperation(-res, "pwrite()");
        }
    }
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
    const ssize_t res = SubmitAndGetResult();
    if(ABSL_PREDICT_FALSE(res < 0)) {
        FailOperation(-res, "fsync()");
    }
    return res;
}

inline struct io_uring_sqe* FdSyncIoUring::GetSqe() {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    RIEGELI_ASSERT(!!sqe) << "Failed get a sqe.";
    return sqe;
}

inline ssize_t FdSyncIoUring::SubmitAndGetResult() {
    const int submit_res = io_uring_submit(&ring_);
    RIEGELI_ASSERT_GT(submit_res, 0) << "Failed to submit the sqe.";
    struct io_uring_cqe* cqe = NULL;
    const int wait_res = io_uring_wait_cqe(&ring_, &cqe);
    RIEGELI_ASSERT_EQ(wait_res, 0) << "Failed to get a cqe";
    ssize_t res = cqe -> res;
    io_uring_cqe_seen(&ring_, cqe);
    return res;
}

} // namespace riegeli
