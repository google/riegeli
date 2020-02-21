load("@local_config_python//:build_defs.bzl", "python_version")

package(default_visibility = ["//visibility:public"])

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
        "//riegeli/base",
        "//riegeli/records:record_position",
        "//riegeli/records:record_reader",
        "//riegeli/records:skipped_region",
        "//riegeli/tensorflow/io:file_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@local_config_tf//:libtensorflow_framework",
        "@local_config_tf//:tf_header_lib",
    ],
)

py_test(
    name = "riegeli_dataset_test",
    srcs = ["kernel_tests/riegeli_dataset_test.py"],
    python_version = python_version,
    deps = [
        ":riegeli_dataset_ops",
        "//python/riegeli",
    ],
)
