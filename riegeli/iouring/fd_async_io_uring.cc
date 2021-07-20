#include "riegeli/iouring/fd_async_io_uring.h"

#include "riegeli/base/base.h"

#include <cstring>
#include <stddef.h>

namespace riegeli {

FdAsyncIoUring::FdAsyncIoUring(FdIoUringOptions options, int fd) 
:size_(options.size()), exit_(false), process_num_(0) {
    
    memset(&ring_, 0, sizeof(ring_));
    memset(&params_, 0, sizeof(params_));

    RIEGELI_ASSERT(InitIoUring()) << "Failed initilization of Io_Uring. (FdAsyncIoUring)";

    if(options.fd_register()) {
        RegisterFd(fd);
    }

    reap_thread_ = std::thread([this]() { this->Reap(); });
}

FdAsyncIoUring::~FdAsyncIoUring() {
    exit_.store(true);

    if(reap_thread_.joinable()) {
        reap_thread_.join();
    }

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
        RIEGELI_ASSERT_EQ(io_uring_register_files(&ring_, &fd_, 1), 0) << "Failed fd register.";
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

ssize_t FdAsyncIoUring::pread(int fd, void *buf, size_t count, off_t offset) {
    std::lock_guard<std::mutex> l(sq_mutex_);
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        io_uring_prep_read(sqe, 0, buf, count, offset);
        sqe -> flags |= IOSQE_FIXED_FILE;
    } else {
        io_uring_prep_read(sqe, fd, buf, count, offset);
    }
    FdAsyncIoUringOp::CallBackFunc cb = std::bind(&FdAsyncIoUring::fsyncCallBack, this, std::placeholders::_1, std::placeholders::_2);
    FdAsyncIoUringOp* op = new FdAsyncIoUringOp(sqe, cb);
    io_uring_sqe_set_data(sqe, op);
    SubmitSqe();
    return count;
}

ssize_t FdAsyncIoUring::pwrite(int fd, const void *buf, size_t count, off_t offset) {
    void* src = operator new(count);
    RIEGELI_ASSERT(!!src) << "Failed to create new space for data. (pwrite)";

    std::memcpy(src, buf, count);

    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        return pwriteInternel(0, src, count, offset);
    }
    
    return pwriteInternel(fd, src, count, offset);
}

ssize_t FdAsyncIoUring::preadv(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) {
    std::lock_guard<std::mutex> l(sq_mutex_);
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        io_uring_prep_readv(sqe, 0, iov, iovcnt, offset);
        sqe -> flags |= IOSQE_FIXED_FILE;
    } else {
        io_uring_prep_readv(sqe, fd, iov, iovcnt, offset);
    }
    FdAsyncIoUringOp::CallBackFunc cb = std::bind(&FdAsyncIoUring::fsyncCallBack, this, std::placeholders::_1, std::placeholders::_2);
    FdAsyncIoUringOp* op = new FdAsyncIoUringOp(sqe, cb);
    io_uring_sqe_set_data(sqe, op);
    SubmitSqe();
    return 0;
}

ssize_t FdAsyncIoUring::pwritev(int fd, const struct ::iovec *iov, int iovcnt, off_t offset) {
    std::lock_guard<std::mutex> l(sq_mutex_);
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        io_uring_prep_writev(sqe, 0, iov, iovcnt, offset);
        sqe -> flags |= IOSQE_FIXED_FILE;
    } else {
        io_uring_prep_writev(sqe, fd, iov, iovcnt, offset);
    }
    FdAsyncIoUringOp::CallBackFunc cb = std::bind(&FdAsyncIoUring::fsyncCallBack, this, std::placeholders::_1, std::placeholders::_2);
    FdAsyncIoUringOp* op = new FdAsyncIoUringOp(sqe, cb);
    io_uring_sqe_set_data(sqe, op);
    SubmitSqe();
    return 0;
}

int FdAsyncIoUring::fsync(int fd) {
    std::lock_guard<std::mutex> l(sq_mutex_);
    struct io_uring_sqe *sqe  = GetSqe();
    if(fd_register_) {
        RIEGELI_ASSERT_EQ(fd_, fd) << "The fd is not epual to the registered fd.";
        io_uring_prep_fsync(sqe, 0, 0);
        sqe -> flags |= IOSQE_FIXED_FILE;
    } else {
        io_uring_prep_fsync(sqe, fd, 0);
    }
    FdAsyncIoUringOp::CallBackFunc cb = std::bind(&FdAsyncIoUring::fsyncCallBack, this, std::placeholders::_1, std::placeholders::_2);
    FdAsyncIoUringOp* op = new FdAsyncIoUringOp(sqe, cb);
    io_uring_sqe_set_data(sqe, op);
    SubmitSqe();
    return 0;
}

ssize_t FdAsyncIoUring::pwriteInternel(int fd, const void* buf, size_t count, off_t offset) {
    std::lock_guard<std::mutex> l(sq_mutex_);
    struct io_uring_sqe *sqe = GetSqe();
    io_uring_prep_write(sqe, fd, buf, count, offset);
    if(fd_register_) {
        sqe -> flags |= IOSQE_FIXED_FILE;
    }
    FdAsyncIoUringOp::CallBackFunc cb = std::bind(&FdAsyncIoUring::pwriteCallBack, this, std::placeholders::_1, std::placeholders::_2);
    FdAsyncIoUringOp* op = new FdAsyncIoUringOp(sqe, cb);
    io_uring_sqe_set_data(sqe, op);
    SubmitSqe();
    return count;
}

void FdAsyncIoUring::pwriteCallBack(FdAsyncIoUringOp *op, ssize_t res) {
    int fd = op -> GetSqe().fd;
    void* buf = reinterpret_cast<void*>(op -> GetSqe().addr);
    size_t offset = op -> GetSqe().off;
    size_t count = op -> GetSqe().len;
    delete op;

    RIEGELI_ASSERT_GE(res, 0) << "pwrite() errno.";
    RIEGELI_ASSERT_GT(res, 0) << "pwrite() return 0.";
    RIEGELI_ASSERT_LE((size_t) res, count) << "pwrite() wrote more than requested.";

    char* newBuf = static_cast<char*>(buf);
    operator delete(buf, res);
    if(count - res > 0) {
        newBuf += res;
        offset += res;
        count -= res;
        pwriteInternel(fd, newBuf, count, offset);
    }
    
}

void FdAsyncIoUring::fsyncCallBack(FdAsyncIoUringOp *op, ssize_t res) {
    delete op;
    RIEGELI_ASSERT_EQ(res, 0) << "fsync() errno.";
}

void FdAsyncIoUring::preadCallBack(FdAsyncIoUringOp *op, ssize_t res) {
    delete op;
    RIEGELI_ASSERT_GE(res, 0) << "pread() errno.";
    RIEGELI_ASSERT_GT(res, 0) << "pread() return 0.";
}

void FdAsyncIoUring::pwritevCallBack(FdAsyncIoUringOp *op, ssize_t res) {
    delete op;
    RIEGELI_ASSERT_GE(res, 0) << "pwritev() errno.";
    RIEGELI_ASSERT_GT(res, 0) << "pwritev() return 0.";
}

void FdAsyncIoUring::preadvCallBack(FdAsyncIoUringOp *op, ssize_t res) {
    delete op;
    RIEGELI_ASSERT_GE(res, 0) << "preadv() errno.";
    RIEGELI_ASSERT_GT(res, 0) << "preadv() return 0.";
}

inline struct io_uring_sqe* FdAsyncIoUring::GetSqe() {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    RIEGELI_ASSERT(!!sqe) << "Failed get a sqe.";
    return sqe;
}

inline void FdAsyncIoUring::SubmitSqe() {
    RIEGELI_ASSERT_GT(io_uring_submit(&ring_), 0) << "Failed to submit the sqe.";
    ++process_num_;
}

void FdAsyncIoUring::Reap() {
    while(!exit_.load() || process_num_ != 0) {
        if(process_num_ != 0) {
            struct io_uring_cqe* cqe = NULL;
            RIEGELI_ASSERT_EQ(io_uring_wait_cqe(&ring_, &cqe), 0) << "Failed to get a cqe";
            --process_num_;
            ssize_t res = cqe -> res;
            FdAsyncIoUringOp *op = static_cast<FdAsyncIoUringOp*>(io_uring_cqe_get_data(cqe));
            FdAsyncIoUringOp::CallBackFunc cb = op -> GetCallBackFunc();
            if(cb != NULL) {
                cb(op, res);
            }
            io_uring_cqe_seen(&ring_, cqe);
        }
    }
}

} // namespace riegeli
