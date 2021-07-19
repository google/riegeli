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

cc_binary(
    name = "io_test",
    srcs = ["io_test.cc"],
    deps = [
        "//riegeli/bytes:fd_io_uring_writer",
        "//riegeli/records:record_writer",
        "//riegeli/bytes:fd_reader",
        "//riegeli/records:record_reader",
    ],
)