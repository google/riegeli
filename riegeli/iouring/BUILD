package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "fd_sync_io_uring",
    srcs = [
        "fd_io_uring_options.h",
        "fd_io_uring.h",
        "fd_sync_io_uring.cc",
    ],
    hdrs = ["fd_sync_io_uring.h"],
    deps = [
        "//riegeli/base",
        "@liburing",
    ],
)