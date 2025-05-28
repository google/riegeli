load("@com_google_protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")

package(
    default_visibility = ["//riegeli:__subpackages__"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_binary(
    name = "describe_riegeli_file",
    srcs = ["describe_riegeli_file.cc"],
    deps = [
        ":riegeli_summary_cc_proto",
        "//riegeli/base:any",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:initializer",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:chain_backward_writer",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:fd_reader",
        "//riegeli/bytes:limiting_reader",
        "//riegeli/bytes:null_backward_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:std_io",
        "//riegeli/bytes:writer",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:constants",
        "//riegeli/chunk_encoding:decompressor",
        "//riegeli/chunk_encoding:field_projection",
        "//riegeli/chunk_encoding:transpose_decoder",
        "//riegeli/lines:line_writing",
        "//riegeli/lines:text_writer",
        "//riegeli/messages:parse_message",
        "//riegeli/messages:text_print_message",
        "//riegeli/records:chunk_reader",
        "//riegeli/records:records_metadata_cc_proto",
        "//riegeli/records:skipped_region",
        "//riegeli/varint:varint_reading",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/flags:usage",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

proto_library(
    name = "riegeli_summary_proto",
    srcs = ["riegeli_summary.proto"],
    deps = ["//riegeli/records:records_metadata_proto"],
)

cc_proto_library(
    name = "riegeli_summary_cc_proto",
    deps = [":riegeli_summary_proto"],
)

cc_binary(
    name = "records_benchmark",
    srcs = ["records_benchmark.cc"],
    deps = [
        ":tfrecord_recognizer",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:initializer",
        "//riegeli/base:options_parser",
        "//riegeli/bytes:fd_reader",
        "//riegeli/bytes:fd_writer",
        "//riegeli/bytes:std_io",
        "//riegeli/bytes:writer",
        "//riegeli/lines:line_writing",
        "//riegeli/lines:newline",
        "//riegeli/lines:text_writer",
        "//riegeli/records:chunk_reader",
        "//riegeli/records:record_reader",
        "//riegeli/records:record_writer",
        "//riegeli/text:ascii_align",
        "//riegeli/varint:varint_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/flags:usage",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@local_config_tf//:libtensorflow_framework",
        "@local_config_tf//:tf_header_lib",
    ],
)

cc_library(
    name = "tfrecord_recognizer",
    srcs = ["tfrecord_recognizer.cc"],
    hdrs = ["tfrecord_recognizer.h"],
    deps = [
        "//riegeli/base:any",
        "//riegeli/base:assert",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/bytes:reader",
        "//riegeli/endian:endian_reading",
        "//riegeli/zlib:zlib_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@local_config_tf//:tf_header_lib",
    ],
)
