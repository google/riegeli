load("@org_tensorflow//tensorflow:tensorflow.bzl", "tf_gen_op_libs")
load("@org_tensorflow//tensorflow:tensorflow.bzl", "tf_kernel_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

tf_gen_op_libs(
    op_lib_names = ["riegeli_dataset_ops"],
)

tf_kernel_library(
    name = "riegeli_dataset_kernel",
    srcs = ["kernels/riegeli_dataset_ops.cc"],
    deps = [
        "//riegeli/base",
        "//riegeli/records:record_position",
        "//riegeli/records:record_reader",
        "//riegeli/tensorflow/io:file_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@org_tensorflow//tensorflow/core:core_cpu",
        "@org_tensorflow//tensorflow/core:framework",
        "@org_tensorflow//tensorflow/core:lib",
        "@org_tensorflow//tensorflow/core:protos_all_cc",
    ],
)

exports_files([
    "kernels/riegeli_dataset_ops.cc",
    "ops/riegeli_dataset_ops.cc",
])
