cc_test(
    name = "io_uring_write_test",
    srcs = ["io_uring_write_test.cc"],
    deps = [
        "//riegeli/bytes:fd_io_uring_writer",
        "//riegeli/records:record_writer",
        "//riegeli/bytes:fd_reader",
        "//riegeli/records:record_reader",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "io_uring_test",
    srcs = ["io_uring_test.cc"],
    deps = [
        "//riegeli/iouring:fd_async_io_uring",
        "//riegeli/iouring:fd_sync_io_uring",
        "@com_google_googletest//:gtest_main",
    ],
)