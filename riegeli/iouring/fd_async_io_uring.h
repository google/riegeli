#ifndef RIEGELI_IOURING_FD_ASYNC_IO_URING_H_
#define RIEGELI_IOURING_FD_ASYNC_IO_URING_H_

#include <atomic>
#include <thread>
#include <mutex>
#include <functional>

#include "riegeli/iouring/fd_io_uring.h"

namespace riegeli {

// Class maintains data and call back function for Io_Uring operation.
class FdAsyncIoUringOp {
    public:
        // Call back function.
        using CallBackFunc = std::function<void(FdAsyncIoUringOp*, const ssize_t)>;

        // Consturctor and destructor for FdAsyncIoUringOp.
        explicit FdAsyncIoUringOp() : cb_(NULL) {}

        explicit FdAsyncIoUringOp(struct io_uring_sqe *sqe) : sqe_(*sqe), cb_(NULL) {}
        
        explicit FdAsyncIoUringOp(struct io_uring_sqe *sqe, CallBackFunc cb) : sqe_(*sqe), cb_(cb) {}
        
        FdAsyncIoUringOp(const FdAsyncIoUringOp&) = delete;
        FdAsyncIoUringOp& operator=(const FdAsyncIoUringOp&) = delete;

        void SetCallBackFunc(CallBackFunc cb) {
            cb_ = cb;
        }

        CallBackFunc GetCallBackFunc() const {
            return cb_;
        }

        void SetSqe(const struct io_uring_sqe* sqe) {
            sqe_ = *sqe;
        }

        const struct io_uring_sqe& GetSqe() const {
            return sqe_;
        }

    private:
        struct io_uring_sqe sqe_;
        CallBackFunc cb_;
};

// Perform Io_Uring asynchronously.
class FdAsyncIoUring : public FdIoUring {
    public:
        // Constructor and destructor for this class.
        FdAsyncIoUring() = delete;
        FdAsyncIoUring(const FdAsyncIoUring&) = delete;
        FdAsyncIoUring& operator=(const FdAsyncIoUring&) = delete;
        
        explicit FdAsyncIoUring(FdIoUringOptions options, int fd = -1);

        ~FdAsyncIoUring() override;

        // Override the file operation interface for Io_Uring. ToDo: complete pread / preadv / pwritev function.
        ssize_t pread(int fd, void *buf, size_t count, off_t offset) override;
        
        ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) override;
        
        int fsync(int fd) override;

        // Get the mode of Io_Uring.
        IoUringMode Mode() override {
            return IoUringMode::ASYNCIOURING;
        }

    private:
        // Reap handler function.
        void Reap();

        // Get sqe.
        struct io_uring_sqe* GetSqe();

        // Submit sqe to kernel.
        void SubmitSqe();

        // Internel part and call back function of file operation.
        ssize_t pwriteInternel(int fd, const void *buf, size_t count, off_t offset);
        ssize_t preadInternel(int fd, void *buf, size_t count, off_t offset);
        int fsyncInternel(int fd);

        void preadCallBack(FdAsyncIoUringOp *op, const ssize_t res);
        void pwriteCallBack(FdAsyncIoUringOp *op, const ssize_t res);
        void fsyncCallBack(FdAsyncIoUringOp *op, const ssize_t res);

    private:
        // Joinable thread flag.
        std::atomic_bool exit_;

        // Amount of processing number.
        std::atomic_int process_num_;

        // Reaping thread.
        std::thread reap_thread_;

        // Submission queue mutex.
        std::mutex sq_mutex_;
};

}  // namespace riegeli

#endif  // RIEGELI_IOURING_FD_ASYNC_IO_URING_H_