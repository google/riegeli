#ifndef RIEGELI_IOURING_FD_IO_URING_OPTIONS_H_
#define RIEGELI_IOURING_FD_IO_URING_OPTIONS_H_

#include <stddef.h>
#include <sys/types.h>
#include <stdint.h>

#include <utility>

namespace riegeli {

// The base interface class for sync or async Io_Uring.
class FdIoUringOptions {
    public:
        FdIoUringOptions() noexcept {};

        // Tunes the Io_Uring mode (sync or async).
        //
        // If "true", we will return the function immediately. 
        // A reap thread will process the result of operations later.
        // 
        // If "false", we will wait for the result of operations.
        //
        // Default: "true"
        FdIoUringOptions& set_async(bool async) & {
            async_ = async;
            return *this;
        }

        FdIoUringOptions&& set_async(bool async) && {
            return std::move(set_async(async));
        }

        bool async() const {
            return async_;
        }
        
        // Tunes the size of Io_Uring instance. 
        //
        // The size must be a power of 2.
        //
        // Default: 8192.
        FdIoUringOptions& set_size(uint32_t size) & {
            size = RoundUpToNextPowerTwo(size);
            size_ = size;
            return *this;
        }

        FdIoUringOptions&& set_size(uint32_t size) && {
            return std::move(set_size(size));
        }

        uint32_t size() const {
            return size_;
        }

        // If "true", the Io_Uring instance will pre-register a file-set.
        //
        // If "false", the Io_Uring instance will not pre-register.
        //  
        // This can save overhead in kernel when you know the file in advance.
        // The kernel will not retrieve a reference of the file in this case.
        //
        // Default: "false".
        FdIoUringOptions& set_fd_register(bool fd_register) & {
            fd_register_ = fd_register;
            return *this;
        }

        FdIoUringOptions&& set_fd_register(bool fd_register) && {
            return std::move(set_fd_register(fd_register));
        }

        bool fd_register() const {
            return fd_register_;
        }

    private:
        // Tunes the value of size.
        // Get the next power of two.
        uint32_t RoundUpToNextPowerTwo(uint32_t size);

        bool async_ = true;
        uint32_t size_ = 512;
        bool fd_register_ = false;
};
    
}  // namespace riegeli

#endif  // RIEGELI_IOURING_FD_IO_URING_OPTIONS_H_