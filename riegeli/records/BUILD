package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # Apache 2.0

cc_library(
    name = "record_writer",
    srcs = ["record_writer.cc"],
    hdrs = ["record_writer.h"],
    deps = [
        ":chunk_writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:options_parser",
        "//riegeli/base:parallelism",
        "//riegeli/bytes:brotli_writer",
        "//riegeli/bytes:writer",
        "//riegeli/bytes:zstd_writer",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:chunk_encoder",
        "//riegeli/chunk_encoding:deferred_encoder",
        "//riegeli/chunk_encoding:simple_encoder",
        "//riegeli/chunk_encoding:transpose_encoder",
        "//riegeli/chunk_encoding:types",
        "@protobuf_archive//:protobuf_lite",
    ],
)

cc_library(
    name = "record_reader",
    srcs = ["record_reader.cc"],
    hdrs = ["record_reader.h"],
    deps = [
        ":chunk_reader",
        ":record_position",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:reader",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:chunk_decoder",
        "//riegeli/chunk_encoding:field_filter",
        "@protobuf_archive//:protobuf_lite",
    ],
)

cc_library(
    name = "record_position",
    srcs = ["record_position.cc"],
    hdrs = ["record_position.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:endian",
    ],
)

cc_library(
    name = "chunk_writer",
    srcs = ["chunk_writer.cc"],
    hdrs = ["chunk_writer.h"],
    deps = [
        ":block",
        "//riegeli/base",
        "//riegeli/bytes:chain_reader",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:string_reader",
        "//riegeli/bytes:writer",
        "//riegeli/bytes:writer_utils",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:hash",
    ],
)

cc_library(
    name = "chunk_reader",
    srcs = ["chunk_reader.cc"],
    hdrs = ["chunk_reader.h"],
    deps = [
        ":block",
        "//riegeli/base",
        "//riegeli/bytes:reader",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:hash",
    ],
)

cc_library(
    name = "block",
    hdrs = ["block.h"],
    visibility = ["//visibility:private"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:endian",
        "//riegeli/chunk_encoding:chunk",
        "//riegeli/chunk_encoding:hash",
    ],
)
