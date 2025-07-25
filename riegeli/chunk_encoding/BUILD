load("@rules_cc//cc:defs.bzl", "cc_library")

package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "chunk_encoder",
    srcs = ["chunk_encoder.cc"],
    hdrs = ["chunk_encoder.h"],
    deps = [
        ":constants",
        "//riegeli/base:bytes_ref",
        "//riegeli/base:chain",
        "//riegeli/base:external_ref",
        "//riegeli/base:object",
        "//riegeli/bytes:writer",
        "//riegeli/messages:serialize_message",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "chunk_decoder",
    srcs = ["chunk_decoder.cc"],
    hdrs = ["chunk_decoder.h"],
    deps = [
        ":chunk",
        ":constants",
        ":field_projection",
        ":simple_decoder",
        ":transpose_decoder",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:reset",
        "//riegeli/bytes:array_backward_writer",
        "//riegeli/bytes:chain_backward_writer",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:reader",
        "//riegeli/messages:parse_message",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:span",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "constants",
    hdrs = ["constants.h"],
)

cc_library(
    name = "chunk",
    srcs = ["chunk.cc"],
    hdrs = ["chunk.h"],
    deps = [
        ":constants",
        ":hash",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "//riegeli/endian:endian_reading",
        "//riegeli/endian:endian_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "hash",
    srcs = ["hash.cc"],
    hdrs = ["hash.h"],
    deps = [
        "//riegeli/base:chain",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/strings",
        "@highwayhash//:hh_types",
        "@highwayhash//:highwayhash_dynamic",
        "@highwayhash//:instruction_sets",
    ],
)

cc_library(
    name = "compressor_options",
    srcs = ["compressor_options.cc"],
    hdrs = ["compressor_options.h"],
    deps = [
        ":constants",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:options_parser",
        "//riegeli/brotli:brotli_writer",
        "//riegeli/snappy:snappy_writer",
        "//riegeli/zstd:zstd_writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "compressor",
    srcs = ["compressor.cc"],
    hdrs = ["compressor.h"],
    deps = [
        ":brotli_encoder_selection",
        ":compressor_options",
        ":constants",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:types",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:writer",
        "//riegeli/snappy:snappy_writer",
        "//riegeli/varint:varint_writing",
        "//riegeli/zstd:zstd_writer",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "decompressor",
    srcs = ["decompressor.cc"],
    hdrs = ["decompressor.h"],
    deps = [
        ":constants",
        "//riegeli/base:any",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/brotli:brotli_reader",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:reader",
        "//riegeli/snappy:snappy_reader",
        "//riegeli/varint:varint_reading",
        "//riegeli/zstd:zstd_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "brotli_encoder_selection",
    srcs = ["brotli_encoder_selection.cc"],
    hdrs = ["brotli_encoder_selection.h"],
    deps = [
        ":compressor_options",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:initializer",
        "//riegeli/base:recycling_pool",
        "//riegeli/brotli:brotli_writer",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:null_writer",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "simple_encoder",
    srcs = ["simple_encoder.cc"],
    hdrs = ["simple_encoder.h"],
    deps = [
        ":chunk_encoder",
        ":compressor",
        ":compressor_options",
        ":constants",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:bytes_ref",
        "//riegeli/base:chain",
        "//riegeli/base:external_ref",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:types",
        "//riegeli/bytes:writer",
        "//riegeli/messages:serialize_message",
        "//riegeli/varint:varint_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "simple_decoder",
    srcs = ["simple_decoder.cc"],
    hdrs = ["simple_decoder.h"],
    deps = [
        ":constants",
        ":decompressor",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/bytes:limiting_reader",
        "//riegeli/bytes:reader",
        "//riegeli/varint:varint_reading",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "transpose_encoder",
    srcs = ["transpose_encoder.cc"],
    hdrs = ["transpose_encoder.h"],
    deps = [
        ":chunk_encoder",
        ":compressor",
        ":compressor_options",
        ":constants",
        ":transpose_internal",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffering",
        "//riegeli/base:bytes_ref",
        "//riegeli/base:chain",
        "//riegeli/base:compare",
        "//riegeli/base:external_ref",
        "//riegeli/base:initializer",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:types",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:chain_backward_writer",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:cord_reader",
        "//riegeli/bytes:limiting_reader",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:string_reader",
        "//riegeli/bytes:writer",
        "//riegeli/messages:message_wire_format",
        "//riegeli/varint:varint_reading",
        "//riegeli/varint:varint_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "transpose_decoder",
    srcs = ["transpose_decoder.cc"],
    hdrs = ["transpose_decoder.h"],
    deps = [
        ":constants",
        ":decompressor",
        ":field_projection",
        ":transpose_internal",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:global",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:limiting_backward_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:string_reader",
        "//riegeli/messages:message_wire_format",
        "//riegeli/varint:varint_reading",
        "//riegeli/varint:varint_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "transpose_internal",
    hdrs = ["transpose_internal.h"],
    visibility = ["//visibility:private"],
    deps = [
        "//riegeli/base:assert",
        "//riegeli/messages:message_wire_format",
        "//riegeli/varint:varint_writing",
    ],
)

cc_library(
    name = "field_projection",
    hdrs = ["field_projection.h"],
    deps = [
        "//riegeli/base:assert",
        "//riegeli/base:initializer",
        "@com_google_absl//absl/container:inlined_vector",
    ],
)

cc_library(
    name = "deferred_encoder",
    srcs = ["deferred_encoder.cc"],
    hdrs = ["deferred_encoder.h"],
    deps = [
        ":chunk_encoder",
        ":constants",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:bytes_ref",
        "//riegeli/base:chain",
        "//riegeli/base:external_ref",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:writer",
        "//riegeli/messages:serialize_message",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_protobuf//:protobuf_lite",
    ],
)
