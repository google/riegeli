package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "fd_io_uring_options",
    srcs = ["fd_io_uring_options.cc"],
    hdrs = ["fd_io_uring_options.h"],
)

cc_library(
    name = "fd_io_uring",
    srcs = ["fd_io_uring.cc"],
    hdrs = ["fd_io_uring.h"],
    deps = [
        "@liburing",
    ]
)

cc_library(
    name = "fd_sync_io_uring",
    srcs = [
        "fd_sync_io_uring.cc",
    ],
    hdrs = ["fd_sync_io_uring.h"],
    deps = [
        "//riegeli/base",
        ":fd_io_uring_options",
        ":fd_io_uring",
    ],
)

cc_library(
    name = "fd_async_io_uring",
    srcs = [
        "fd_async_io_uring.cc",
    ],
    hdrs = ["fd_async_io_uring.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/iouring:fd_io_uring_options",
        "//riegeli/iouring:fd_io_uring",
    ],
)