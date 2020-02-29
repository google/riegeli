package(default_visibility = ["//python/riegeli:__subpackages__"])

licenses(["notice"])

cc_library(
    name = "utils",
    srcs = ["utils.cc"],
    hdrs = ["utils.h"],
    data = [":riegeli_error"],  # Python module imported from C++.
    deps = [
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:compare",
        "@com_google_absl//absl/types:optional",
        "@local_config_python//:python_headers",
    ],
)

py_library(
    name = "riegeli_error",
    srcs = ["riegeli_error.py"],
)
