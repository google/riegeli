package(default_visibility = ["//python/riegeli:__subpackages__"])

licenses(["notice"])

cc_library(
    name = "python_writer",
    srcs = ["python_writer.cc"],
    hdrs = ["python_writer.h"],
    deps = [
        "//python/riegeli/base:utils",
        "//riegeli/base",
        "//riegeli/base:status",
        "//riegeli/bytes:buffered_writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/utility",
        "@local_config_python//:python_headers",
    ],
)

cc_library(
    name = "python_reader",
    srcs = ["python_reader.cc"],
    hdrs = ["python_reader.h"],
    deps = [
        "//python/riegeli/base:utils",
        "//riegeli/base",
        "//riegeli/base:status",
        "//riegeli/bytes:buffered_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/utility",
        "@local_config_python//:python_headers",
    ],
)
