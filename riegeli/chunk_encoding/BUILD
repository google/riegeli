package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

cc_library(
    name = "chunk_encoder",
    srcs = ["chunk_encoder.cc"],
    hdrs = ["chunk_encoder.h"],
    deps = [
        ":constants",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:message_serialize",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@protobuf_archive//:protobuf_lite",
    ],
)

cc_library(
    name = "chunk_decoder",
    srcs = ["chunk_decoder.cc"],
    hdrs = ["chunk_decoder.h"],
    deps = [
        ":chunk",
        ":constants",
        ":field_filter",
        ":simple_decoder",
        ":transpose_decoder",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:chain_backward_writer",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:limiting_reader",
        "//riegeli/bytes:message_parse",
        "//riegeli/bytes:reader_utils",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@protobuf_archive//:protobuf_lite",
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
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:endian",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
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
        "@com_google_highwayhash//:hh_types",
        "@com_google_highwayhash//:highwayhash_dynamic",
        "@com_google_highwayhash//:instruction_sets",
    ],
)

cc_library(
    name = "compressor_options",
    srcs = ["compressor_options.cc"],
    hdrs = ["compressor_options.h"],
    deps = [
        ":constants",
        "//riegeli/base",
        "//riegeli/base:options_parser",
        "//riegeli/bytes:brotli_writer",
        "//riegeli/bytes:zstd_writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "compressor",
    srcs = ["compressor.cc"],
    hdrs = ["compressor.h"],
    deps = [
        ":compressor_options",
        ":constants",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:brotli_writer",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:writer",
        "//riegeli/bytes:writer_utils",
        "//riegeli/bytes:zstd_writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/types:variant",
    ],
)

cc_library(
    name = "decompressor",
    srcs = ["decompressor.cc"],
    hdrs = ["decompressor.h"],
    deps = [
        ":constants",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:brotli_reader",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:reader_utils",
        "//riegeli/bytes:zstd_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
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
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:message_serialize",
        "//riegeli/bytes:writer",
        "//riegeli/bytes:writer_utils",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@protobuf_archive//:protobuf_lite",
    ],
)

cc_library(
    name = "simple_decoder",
    srcs = ["simple_decoder.cc"],
    hdrs = ["simple_decoder.h"],
    deps = [
        ":constants",
        ":decompressor",
        "//riegeli/base",
        "//riegeli/bytes:limiting_reader",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:reader_utils",
        "@com_google_absl//absl/base:core_headers",
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
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:backward_writer_utils",
        "//riegeli/bytes:chain_backward_writer",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:limiting_reader",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:reader_utils",
        "//riegeli/bytes:string_reader",
        "//riegeli/bytes:writer",
        "//riegeli/bytes:writer_utils",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "transpose_decoder",
    srcs = ["transpose_decoder.cc"],
    hdrs = ["transpose_decoder.h"],
    deps = [
        ":constants",
        ":decompressor",
        ":field_filter",
        ":transpose_internal",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:backward_writer_utils",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:limiting_backward_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:reader_utils",
        "//riegeli/bytes:writer_utils",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "transpose_internal",
    hdrs = ["transpose_internal.h"],
    visibility = [
        "//visibility:private",
    ],
    deps = [
        "//riegeli/base",
        "//riegeli/bytes:writer_utils",
    ],
)

cc_library(
    name = "field_filter",
    hdrs = ["field_filter.h"],
    deps = [
        "//riegeli/base",
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
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:message_serialize",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@protobuf_archive//:protobuf_lite",
    ],
)
