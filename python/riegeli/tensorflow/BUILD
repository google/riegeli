load("@rules_cc//cc:defs.bzl", "cc_binary")
load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

py_library(
    name = "riegeli_dataset_ops",
    srcs = ["ops/riegeli_dataset_ops.py"],
    data = [":ops/_riegeli_dataset_ops.so"],
)

cc_binary(
    name = "ops/_riegeli_dataset_ops.so",
    srcs = [
        "//riegeli/tensorflow:kernels/riegeli_dataset_ops.cc",
        "//riegeli/tensorflow:ops/riegeli_dataset_ops.cc",
    ],
    # tensorflow/core/lib/core/refcount.h needs NDEBUG consistency between
    # translation units.
    copts = ["-DNDEBUG"],
    linkshared = True,
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/records:record_position",
        "//riegeli/records:record_reader",
        "//riegeli/records:skipped_region",
        "//riegeli/tensorflow/io:file_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@local_config_tf//:libtensorflow_framework",
        "@local_config_tf//:tf_header_lib",
    ],
)

py_test(
    name = "riegeli_dataset_test",
    srcs = ["kernel_tests/riegeli_dataset_test.py"],
    deps = [
        ":riegeli_dataset_ops",
        "//python/riegeli",
    ],
)
