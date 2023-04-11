load("@rules_proto//proto:defs.bzl", "proto_library")

package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "record_reader",
    srcs = ["record_reader.cc"],
    hdrs = ["record_reader.h"],
    deps = [
        ":chunk_reader",
        ":record_position",
        ":records_metadata_cc_proto",
        ":skipped_region",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:binary_search",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:object",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:chain_backward_writer",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:reader",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:chunk_decoder",
        "//riegeli/chunk_encoding:constants",
        "//riegeli/chunk_encoding:field_projection",
        "//riegeli/chunk_encoding:transpose_decoder",
        "//riegeli/messages:message_parse",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:compare",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "record_writer",
    srcs = ["record_writer.cc"],
    hdrs = ["record_writer.h"],
    deps = [
        ":chunk_writer",
        ":record_position",
        ":records_metadata_cc_proto",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:object",
        "//riegeli/base:options_parser",
        "//riegeli/base:parallelism",
        "//riegeli/base:stable_dependency",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:writer",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:chunk_encoder",
        "//riegeli/chunk_encoding:compressor_options",
        "//riegeli/chunk_encoding:constants",
        "//riegeli/chunk_encoding:deferred_encoder",
        "//riegeli/chunk_encoding:simple_encoder",
        "//riegeli/chunk_encoding:transpose_encoder",
        "//riegeli/messages:message_serialize",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:variant",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "record_position",
    srcs = ["record_position.cc"],
    hdrs = ["record_position.h"],
    deps = [
        ":block",
        "//riegeli/base:assert",
        "//riegeli/base:intrusive_ref_count",
        "//riegeli/base:types",
        "//riegeli/bytes:string_reader",
        "//riegeli/bytes:string_writer",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/endian:endian_reading",
        "//riegeli/ordered_varint:ordered_varint_reading",
        "//riegeli/ordered_varint:ordered_varint_writing",
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
        "//riegeli/base:assert",
        "//riegeli/base:types",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "chunk_reader",
    srcs = ["chunk_reader.cc"],
    hdrs = ["chunk_reader.h"],
    deps = [
        ":block",
        ":skipped_region",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:object",
        "//riegeli/base:types",
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
    name = "chunk_writer",
    srcs = ["chunk_writer.cc"],
    hdrs = ["chunk_writer.h"],
    deps = [
        ":block",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:object",
        "//riegeli/base:types",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:string_reader",
        "//riegeli/bytes:writer",
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
    visibility = ["//riegeli:__subpackages__"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:constexpr",
        "//riegeli/base:types",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:hash",
        "//riegeli/endian:endian_reading",
        "//riegeli/endian:endian_writing",
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
