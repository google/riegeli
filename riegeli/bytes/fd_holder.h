#ifndef RIEGELI_BYTES_FD_HOLDER_H_
#define RIEGELI_BYTES_FD_HOLDER_H_

#include <unistd.h>
#include <cerrno>

#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"

namespace riegeli {
namespace internal {

// Manages ownership of a file descriptor.
class FdHolder {
 public:
  // Creates a FdHolder which does not own a fd.
  FdHolder() noexcept {}

  // Creates a FdHolder which owns fd if fd >= 0.
  explicit FdHolder(int fd) noexcept : fd_(fd) {}

  FdHolder(FdHolder&& src) noexcept;
  FdHolder& operator=(FdHolder&& src) noexcept;

  ~FdHolder();

  // Return value:
  //  * 0     - success
  //  * errno - failure (fd is closed anyway)
  int Close();

  static absl::string_view CloseFunctionName();

 private:
  int fd_ = -1;
};

inline FdHolder::FdHolder(FdHolder&& src) noexcept
    : fd_(riegeli::exchange(src.fd_, -1)) {}

inline FdHolder& FdHolder::operator=(FdHolder&& src) noexcept {
  // Exchange src.fd_ early to support self-assignment.
  const int fd = riegeli::exchange(src.fd_, -1);
  Close();
  fd_ = fd;
  return *this;
}

inline FdHolder::~FdHolder() { Close(); }

inline int FdHolder::Close() {
  int error_code = 0;
  if (fd_ >= 0) {
    // http://austingroupbugs.net/view.php?id=529 explains this mess.
#ifdef POSIX_CLOSE_RESTART
    // Avoid EINTR by using posix_close(_, 0) if available.
    if (RIEGELI_UNLIKELY(posix_close(fd_, 0) < 0)) {
      error_code = errno;
      if (error_code == EINPROGRESS) error_code = 0;
    }
#else
    if (RIEGELI_UNLIKELY(close(fd_) < 0)) {
      error_code = errno;
      // After EINTR it is unspecified whether fd has been closed or not.
      // Assume that it is closed, which is the case e.g. on Linux.
      if (error_code == EINPROGRESS || error_code == EINTR) error_code = 0;
    }
#endif
    fd_ = -1;
  }
  return error_code;
}

inline absl::string_view FdHolder::CloseFunctionName() {
#ifdef POSIX_CLOSE_RESTART
  return "posix_close()";
#else
  return "close()";
#endif
}

}  // namespace internal
}  // namespace riegeli

#endif  // RIEGELI_BYTES_FD_HOLDER_H_
