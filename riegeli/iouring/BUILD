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
        "//riegeli/base",
        "//riegeli/base:status",
        ":fd_io_uring_options",
        "@liburing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ]
)

cc_library(
    name = "fd_sync_io_uring",
    srcs = [
        "fd_sync_io_uring.cc",
    ],
    hdrs = ["fd_sync_io_uring.h"],
    deps = [
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
        ":fd_io_uring",
        "@com_google_absl//absl/functional:bind_front",
    ],
)