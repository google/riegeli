#include "riegeli/iouring/fd_async_io_uring.h"

#include "absl/functional/bind_front.h"

namespace riegeli {

FdAsyncIoUring::FdAsyncIoUring(FdIoUringOptions options, int fd) 
:FdIoUring(options, fd), exit_(false), process_num_(0) {
    reap_thread_ = std::thread([this]() { this->Reap(); });
}

FdAsyncIoUring::~FdAsyncIoUring() {
    exit_.store(true);

    if(reap_thread_.joinable()) {
        reap_thread_.join();
    }
}

ssize_t FdAsyncIoUring::pread(int fd, void *buf, size_t count, off_t offset) {
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        return preadInternel(0, buf, count, offset);
    }
    
    return preadInternel(fd, buf, count, offset);
}

ssize_t FdAsyncIoUring::pwrite(int fd, const void *buf, size_t count, off_t offset) {
    void* src = operator new(count);

    if(ABSL_PREDICT_FALSE(!src)) {
        FailOperation(12, "FdAsyncIoUring pwrite() Create Space");
        return -12;
    }

    std::memcpy(src, buf, count);

    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        return pwriteInternel(0, src, count, offset);
    }
    
    return pwriteInternel(fd, src, count, offset);
}

int FdAsyncIoUring::fsync(int fd) {
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        return fsyncInternel(0);
    }
    
    return fsyncInternel(fd);
}

ssize_t FdAsyncIoUring::preadInternel(int fd, void *buf, size_t count, off_t offset) {
    std::lock_guard<std::mutex> lock_g(sq_mutex_);
    struct io_uring_sqe *sqe  = GetSqe();
    io_uring_prep_read(sqe, fd, buf, count, offset);
    if(fd_register_) {
        sqe -> flags |= IOSQE_FIXED_FILE;
    }
    FdAsyncIoUringOp::CallBackFunc cb = absl::bind_front(&FdAsyncIoUring::preadCallBack, this);
    FdAsyncIoUringOp* op = new FdAsyncIoUringOp(sqe, cb);
    io_uring_sqe_set_data(sqe, op);
    SubmitSqe();
    return count;
}

ssize_t FdAsyncIoUring::pwriteInternel(int fd, const void* buf, size_t count, off_t offset) {
    std::lock_guard<std::mutex> lock_g(sq_mutex_);
    struct io_uring_sqe *sqe = GetSqe();
    io_uring_prep_write(sqe, fd, buf, count, offset);
    if(fd_register_) {
        sqe -> flags |= IOSQE_FIXED_FILE;
    }
    FdAsyncIoUringOp::CallBackFunc cb = absl::bind_front(&FdAsyncIoUring::pwriteCallBack, this);
    FdAsyncIoUringOp* op = new FdAsyncIoUringOp(sqe, cb);
    io_uring_sqe_set_data(sqe, op);
    SubmitSqe();
    return count;
}

int FdAsyncIoUring::fsyncInternel(int fd) {
    std::lock_guard<std::mutex> lock_g(sq_mutex_);
    struct io_uring_sqe *sqe  = GetSqe();
    io_uring_prep_fsync(sqe, fd, 0);
    if(fd_register_) {
        sqe -> flags |= IOSQE_FIXED_FILE;
    }
    FdAsyncIoUringOp::CallBackFunc cb = absl::bind_front(&FdAsyncIoUring::fsyncCallBack, this);
    FdAsyncIoUringOp* op = new FdAsyncIoUringOp(sqe, cb);
    io_uring_sqe_set_data(sqe, op);
    SubmitSqe();
    return 0;
}

void FdAsyncIoUring::preadCallBack(FdAsyncIoUringOp *op, const ssize_t res) {
    int fd = op -> GetSqe().fd;
    void* buf = reinterpret_cast<void*>(op -> GetSqe().addr);
    size_t offset = op -> GetSqe().off;
    size_t count = op -> GetSqe().len;
    delete op;

    if(ABSL_PREDICT_FALSE(res < 0)) {
        if(res == -EINTR || res == -EAGAIN) {
            preadInternel(fd, buf, count, offset); 
        } else {
            FailOperation(-res, "pread()"); 
        }
    }
    RIEGELI_ASSERT_GT(res, 0) << "pread() return 0.";
}

void FdAsyncIoUring::pwriteCallBack(FdAsyncIoUringOp *op, const ssize_t res) {
    int fd = op -> GetSqe().fd;
    void* buf = reinterpret_cast<void*>(op -> GetSqe().addr);
    size_t offset = op -> GetSqe().off;
    size_t count = op -> GetSqe().len;
    delete op;

    if(ABSL_PREDICT_FALSE(res < 0)) {
        if(res == -EINTR || res == -EAGAIN) {
            pwriteInternel(fd, buf, count, offset); 
        } else {
            operator delete(buf, count);
            FailOperation(-res, "pwrite()"); 
        }
        return;
    }
    RIEGELI_ASSERT_GT(res, 0) << "pwrite() return 0.";
    RIEGELI_ASSERT_LE((size_t) res, count) << "pwrite() wrote more than requested.";

    char* newBuf = static_cast<char*>(buf);
    operator delete(buf, res);
    if(ABSL_PREDICT_FALSE(count - res > 0)) {
        newBuf += res;
        offset += res;
        count -= res;
        pwriteInternel(fd, newBuf, count, offset);
    }
}

void FdAsyncIoUring::fsyncCallBack(FdAsyncIoUringOp *op, const ssize_t res) {
    delete op;

    if(ABSL_PREDICT_FALSE(res < 0)) {
        FailOperation(-res, "fsync()"); 
    }
}

inline struct io_uring_sqe* FdAsyncIoUring::GetSqe() {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    RIEGELI_ASSERT(!!sqe) << "Failed get a sqe.";
    return sqe;
}

inline void FdAsyncIoUring::SubmitSqe() {
    const int submit_res = io_uring_submit(&ring_);
    RIEGELI_ASSERT_GT(submit_res, 0) << "Failed to submit the sqe.";
    process_num_ += submit_res;
}

void FdAsyncIoUring::Reap() {
    while(!exit_.load() || process_num_ != 0) {
        if(process_num_ != 0) {
            struct io_uring_cqe* cqe = NULL;
            if(io_uring_wait_cqe(&ring_, &cqe) == 0) {
                --process_num_;
                const ssize_t res = cqe -> res;
                FdAsyncIoUringOp *op = static_cast<FdAsyncIoUringOp*>(io_uring_cqe_get_data(cqe));
                FdAsyncIoUringOp::CallBackFunc cb = op -> GetCallBackFunc();
                if(cb != NULL) {
                    cb(op, res);
                }
                io_uring_cqe_seen(&ring_, cqe);
            }
        }
    }
}

} // namespace riegeli
