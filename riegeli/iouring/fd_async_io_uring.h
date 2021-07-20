#ifndef RIEGELI_IOURING_FD_ASYNC_IO_URING_H_
#define RIEGELI_IOURING_FD_ASYNC_IO_URING_H_

#include <atomic>
#include <thread>
#include <mutex>
#include <functional>

#include "riegeli/iouring/fd_io_uring_options.h"
#include "riegeli/iouring/fd_io_uring.h"

namespace riegeli {

// Class maintains data and call back function for Io_Uring operation.
class FdAsyncIoUringOp {
    public:
        // Call back function.
        using CallBackFunc = std::function<void(FdAsyncIoUringOp*, ssize_t)>;

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
            return IoUringMode::ASYNCIOURING;
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

        // Reap handler function.
        void Reap();

        // Get sqe.
        struct io_uring_sqe* GetSqe();

        // Submit sqe to kernel.
        void SubmitSqe();

        // Internel part and call back function of file operation.
        ssize_t pwriteInternel(int fd, const void *buf, size_t count, off_t offset);
        void pwriteCallBack(FdAsyncIoUringOp *op, ssize_t res);

        void fsyncCallBack(FdAsyncIoUringOp *op, ssize_t res);
        void preadCallBack(FdAsyncIoUringOp *op, ssize_t res);
        void preadvCallBack(FdAsyncIoUringOp *op, ssize_t res);
        void pwritevCallBack(FdAsyncIoUringOp *op, ssize_t res);

    private:
        // Io_Uring entrance and set up params.
        struct io_uring_params params_;
        struct io_uring ring_;

        // Io_Uring settings.
        bool fd_register_ = false;
        uint32_t size_ = 0;
        int fd_ = -1;

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