#include "riegeli/iouring/fd_io_uring.h"

#include "riegeli/base/errno_mapping.h"

#include "absl/base/optimization.h"
#include "absl/strings/str_cat.h"

namespace riegeli {

namespace ioUring {

bool IsIoUringAvailable() {
    struct io_uring test_ring;
    bool available = false;
    if(io_uring_queue_init(4, &test_ring, 0) == 0) {
        available = true;
        io_uring_queue_exit(&test_ring);
    }
    return available;
}

}

FdIoUring::FdIoUring(FdIoUringOptions options, int fd) : Object(kInitiallyOpen), size_(options.size()) {
    Init(options.fd_register(), fd);
}

bool FdIoUring::Init(bool fd_register, int fd) {
    memset(&ring_, 0, sizeof(ring_));
    memset(&params_, 0, sizeof(params_));

    const int init_res = io_uring_queue_init_params(size_, &ring_, &params_);
    if(ABSL_PREDICT_FALSE(init_res < 0)) {
        return FailOperation(-init_res, "Init Io_Uring");
    }

    if(fd_register) {
        RegisterFd(fd);
    }

    return true;
}

FdIoUring::~FdIoUring() {
    io_uring_queue_exit(&ring_);
}

void FdIoUring::RegisterFd(int fd) {
    fd_ = fd;
    
    if(fd_register_ == false) {
        const int register_res = io_uring_register_files(&ring_, &fd_, 1);
        RIEGELI_ASSERT_EQ(register_res, 0) << "Failed fd register.";
        fd_register_ = true;
    } else {
        UpdateFd();
    }
}

void FdIoUring::UnRegisterFd() {
    fd_ = -1;
    const int unregister_res = io_uring_unregister_files(&ring_);
    RIEGELI_ASSERT_EQ(unregister_res, 0) << "Failed fd unregister.";
    fd_register_ = false;
}

void FdIoUring::UpdateFd() {
    const int update_res = io_uring_register_files_update(&ring_, 0, &fd_, 1);
    RIEGELI_ASSERT_EQ(update_res, 1) << "Failed fd update.";
}

bool FdIoUring::FailOperation(const int error_number, absl::string_view operation) {
  RIEGELI_ASSERT_NE(error_number, 0)
      << "Failed precondition of FdWriterBase::FailOperation(): "
         "zero errno";
  return Fail(
      ErrnoToCanonicalStatus(error_number, absl::StrCat(operation, " failed")));
}

} // namespace riegeli