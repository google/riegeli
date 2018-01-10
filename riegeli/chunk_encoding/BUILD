package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

cc_library(
    name = "chunk_encoder",
    srcs = ["chunk_encoder.cc"],
    hdrs = ["chunk_encoder.h"],
    deps = [
        ":chunk",
        ":internal_types",
        ":transpose_encoder",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:brotli_writer",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:message_serialize",
        "//riegeli/bytes:writer",
        "//riegeli/bytes:writer_utils",
        "//riegeli/bytes:zstd_writer",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "chunk_decoder",
    srcs = ["chunk_decoder.cc"],
    hdrs = ["chunk_decoder.h"],
    deps = [
        ":chunk",
        ":field_filter",
        ":internal_types",
        ":transpose_decoder",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:brotli_reader",
        "//riegeli/bytes:chain_backward_writer",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:limiting_reader",
        "//riegeli/bytes:message_parse",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:reader_utils",
        "//riegeli/bytes:zstd_reader",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "chunk",
    srcs = ["chunk.cc"],
    hdrs = ["chunk.h"],
    deps = [
        ":hash",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:endian",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
    ],
)

cc_library(
    name = "internal_types",
    hdrs = ["internal_types.h"],
    visibility = [
        "//riegeli/records:__pkg__",
    ],
)

cc_library(
    name = "hash",
    srcs = ["hash.cc"],
    hdrs = ["hash.h"],
    visibility = ["//riegeli/records:__pkg__"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_highwayhash//:hh_types",
        "@com_google_highwayhash//:highwayhash_dynamic",
        "@com_google_highwayhash//:instruction_sets",
    ],
)

cc_library(
    name = "transpose_encoder",
    srcs = [
        "canonical_varint.cc",
        "canonical_varint.h",
        "transpose_encoder.cc",
    ],
    hdrs = ["transpose_encoder.h"],
    visibility = [
        "//visibility:private",
    ],
    deps = [
        ":internal_types",
        ":transpose_internal",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:backward_writer_utils",
        "//riegeli/bytes:brotli_writer",
        "//riegeli/bytes:chain_backward_writer",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:chain_writer",
        "//riegeli/bytes:limiting_reader",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:reader_utils",
        "//riegeli/bytes:string_reader",
        "//riegeli/bytes:writer",
        "//riegeli/bytes:writer_utils",
        "//riegeli/bytes:zstd_writer",
    ],
)

cc_library(
    name = "transpose_decoder",
    srcs = ["transpose_decoder.cc"],
    hdrs = ["transpose_decoder.h"],
    visibility = [
        "//visibility:private",
    ],
    deps = [
        ":field_filter",
        ":internal_types",
        ":transpose_internal",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:backward_writer_utils",
        "//riegeli/bytes:brotli_reader",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:reader_utils",
        "//riegeli/bytes:writer_utils",
        "//riegeli/bytes:zstd_reader",
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
)
