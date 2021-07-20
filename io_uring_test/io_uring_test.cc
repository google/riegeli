#include "riegeli/iouring/fd_async_io_uring.h"
#include "riegeli/iouring/fd_sync_io_uring.h"

#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "gtest/gtest.h"

namespace iouringtest { 
using IoUringPtr = std::unique_ptr<riegeli::FdIoUring>;

TEST(IoUringTest, CreateAsynIoUring) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(true);
    IoUring = std::make_unique<riegeli::FdAsyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> Mode(), riegeli::FdIoUring::IoUringMode::ASYNCIOURING);
}

TEST(IoUringTest, CreateSynIoUring) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(false);
    IoUring = std::make_unique<riegeli::FdSyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> Mode(), riegeli::FdIoUring::IoUringMode::SYNCIOURING);
}

TEST(IoUringTest, SyncUnRegisterFd) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(false);
    options.set_fd_register(false);
    IoUring = std::make_unique<riegeli::FdSyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> fd_register(), false);
    EXPECT_EQ(IoUring -> fd(), -1);
}

TEST(IoUringTest, AsyncUnRegisterFd) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(true);
    options.set_fd_register(false);
    IoUring = std::make_unique<riegeli::FdAsyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> fd_register(), false);
    EXPECT_EQ(IoUring -> fd(), -1);
}

TEST(IoUringTest, SyncRegisterAndUnregisterFd) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(false);
    options.set_fd_register(true);
    IoUring = std::make_unique<riegeli::FdSyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> fd_register(), true);
    EXPECT_EQ(IoUring -> fd(), 0);

    IoUring -> UnRegisterFd();
    EXPECT_EQ(IoUring -> fd_register(), false);
    EXPECT_EQ(IoUring -> fd(), -1);

}

TEST(IoUringTest, AsyncUnRegisterAndUnregisterFd) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(true);
    options.set_fd_register(true);
    IoUring = std::make_unique<riegeli::FdAsyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> fd_register(), true);
    EXPECT_EQ(IoUring -> fd(), 0);

    IoUring -> UnRegisterFd();
    EXPECT_EQ(IoUring -> fd_register(), false);
    EXPECT_EQ(IoUring -> fd(), -1);
}

TEST(IoUringTest, SyncRegisterAndUpdateFd) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(false);
    options.set_fd_register(true);
    IoUring = std::make_unique<riegeli::FdSyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> fd_register(), true);
    EXPECT_EQ(IoUring -> fd(), 0);

    std::string path = std::string(getenv("TEST_TMPDIR")) + "/io_uring_test_file";
    int fd  = open(path, O_WRONLY | O_CREAT | O_TRUNC);
    IoUring -> RegisterFd(fd);
    EXPECT_EQ(IoUring -> fd_register(), true);
    EXPECT_EQ(IoUring -> fd(), fd);

}

TEST(IoUringTest, AsyncUnRegisterAndUpdateFd) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(true);
    options.set_fd_register(true);
    IoUring = std::make_unique<riegeli::FdAsyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> fd_register(), true);
    EXPECT_EQ(IoUring -> fd(), 0);

    std::string path = std::string(getenv("TEST_TMPDIR")) + "/io_uring_test_file";
    int fd  = open(path, O_WRONLY | O_CREAT | O_TRUNC);
    EXPECT_EQ(IoUring -> fd_register(), true);
    EXPECT_EQ(IoUring -> fd(), fd);
}

TEST(IoUringTest, SyncDefaultSize) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(false);
    IoUring = std::make_unique<riegeli::FdSyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> size(), 8192);
}

TEST(IoUringTest, AsyncDefaultSize) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(true);
    IoUring = std::make_unique<riegeli::FdAsyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> size(), 8192);
}

TEST(IoUringTest, SyncSize) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(false);
    options.set_size(10);
    IoUring = std::make_unique<riegeli::FdSyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> size(), 16);
}

TEST(IoUringTest, AsyncSize) {
    IoUringPtr IoUring;
    riegeli::FdIoUringOptions options;
    options.set_async(true);
    options.set_size(10);
    IoUring = std::make_unique<riegeli::FdAsyncIoUring>(options, 0);

    EXPECT_EQ(IoUring -> size(), 16);
}

}