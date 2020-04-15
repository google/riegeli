load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "record_writer",
    srcs = [
        "chunk_writer_dependency.h",
        "record_writer.cc",
    ],
    hdrs = ["record_writer.h"],
    deps = [
        ":chunk_writer",
        ":record_position",
        ":records_metadata_cc_proto",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:options_parser",
        "//riegeli/base:parallelism",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:writer",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:chunk_encoder",
        "//riegeli/chunk_encoding:compressor_options",
        "//riegeli/chunk_encoding:constants",
        "//riegeli/chunk_encoding:deferred_encoder",
        "//riegeli/chunk_encoding:simple_encoder",
        "//riegeli/chunk_encoding:transpose_encoder",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:variant",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "record_reader",
    srcs = [
        "chunk_reader_dependency.h",
        "record_reader.cc",
    ],
    hdrs = ["record_reader.h"],
    deps = [
        ":block",
        ":chunk_reader",
        ":record_position",
        ":records_metadata_cc_proto",
        ":skipped_region",
        "//riegeli/base",
        "//riegeli/base:binary_search",
        "//riegeli/base:chain",
        "//riegeli/bytes:chain_backward_writer",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:message_parse",
        "//riegeli/bytes:reader",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:chunk_decoder",
        "//riegeli/chunk_encoding:constants",
        "//riegeli/chunk_encoding:field_projection",
        "//riegeli/chunk_encoding:transpose_decoder",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:compare",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:cc_wkt_protos",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "record_position",
    srcs = ["record_position.cc"],
    hdrs = ["record_position.h"],
    deps = [
        ":block",
        "//riegeli/base",
        "//riegeli/bytes:endian_reading",
        "//riegeli/bytes:endian_writing",
        "//riegeli/chunk_encoding:chunk",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:variant",
    ],
)

cc_library(
    name = "skipped_region",
    srcs = ["skipped_region.cc"],
    hdrs = ["skipped_region.h"],
    deps = [
        "//riegeli/base",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "chunk_writer",
    srcs = ["chunk_writer.cc"],
    hdrs = ["chunk_writer.h"],
    deps = [
        ":block",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:string_reader",
        "//riegeli/bytes:writer",
        "//riegeli/bytes:writer_utils",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:constants",
        "//riegeli/chunk_encoding:hash",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "chunk_reader",
    srcs = ["chunk_reader.cc"],
    hdrs = ["chunk_reader.h"],
    deps = [
        ":block",
        ":skipped_region",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:reader",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:constants",
        "//riegeli/chunk_encoding:hash",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "block",
    hdrs = ["block.h"],
    visibility = ["//visibility:private"],
    deps = [
        "//riegeli/base",
        "//riegeli/bytes:endian_reading",
        "//riegeli/bytes:endian_writing",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:hash",
        "@com_google_absl//absl/strings",
    ],
)

proto_library(
    name = "records_metadata_proto",
    srcs = ["records_metadata.proto"],
    deps = ["@com_google_protobuf//:descriptor_proto"],
)

cc_proto_library(
    name = "records_metadata_cc_proto",
    deps = [":records_metadata_proto"],
)
