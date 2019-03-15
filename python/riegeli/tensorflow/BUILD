load("@org_tensorflow//tensorflow:tensorflow.bzl", "tf_custom_op_library")
load("@org_tensorflow//tensorflow:tensorflow.bzl", "tf_gen_op_wrapper_py")
load("@org_tensorflow//tensorflow:tensorflow.bzl", "tf_custom_op_py_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

tf_custom_op_py_library(
    name = "riegeli_dataset_ops",
    srcs = ["ops/riegeli_dataset_ops.py"],
    dso = [":ops/_riegeli_dataset_ops.so"],
    kernels = [
        "//riegeli/tensorflow:riegeli_dataset_kernel",
        "//riegeli/tensorflow:riegeli_dataset_ops_op_lib",
    ],
    deps = [
        ":gen_riegeli_dataset_ops",
        "@org_tensorflow//tensorflow:tensorflow_py",
        "@org_tensorflow//tensorflow/python:platform",
    ],
)

tf_gen_op_wrapper_py(
    name = "gen_riegeli_dataset_ops",
    out = "ops/gen_riegeli_dataset_ops.py",
    deps = ["//riegeli/tensorflow:riegeli_dataset_ops_op_lib"],
)

tf_custom_op_library(
    name = "ops/_riegeli_dataset_ops.so",
    srcs = [
        "//riegeli/tensorflow:kernels/riegeli_dataset_ops.cc",
        "//riegeli/tensorflow:ops/riegeli_dataset_ops.cc",
    ],
    # Merge @com_google_protobuf and @protobuf_archive.
    linkopts = ["-Wl,--allow-multiple-definition"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:status",
        "//riegeli/records:record_position",
        "//riegeli/records:record_reader",
        "//riegeli/tensorflow/io:file_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@org_tensorflow//tensorflow/core:lib",
        "@org_tensorflow//tensorflow/core:protos_all_cc",
    ],
)

py_test(
    name = "riegeli_dataset_test",
    srcs = ["kernel_tests/riegeli_dataset_test.py"],
    deps = [
        ":riegeli_dataset_ops",
        "//python/riegeli",
        "@org_tensorflow//tensorflow:tensorflow_py",
    ],
)
