package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "writer",
    srcs = ["writer.cc"],
    hdrs = ["writer.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "backward_writer",
    srcs = ["backward_writer.cc"],
    hdrs = ["backward_writer.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "reader",
    srcs = ["reader.cc"],
    hdrs = ["reader.h"],
    deps = [
        ":backward_writer",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "writer_utils",
    srcs = ["writer_utils.cc"],
    hdrs = [
        "varint.h",
        "writer_utils.h",
    ],
    deps = [
        ":writer",
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "backward_writer_utils",
    hdrs = [
        "backward_writer_utils.h",
        "varint.h",
    ],
    deps = [
        ":backward_writer",
        ":writer_utils",
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "reader_utils",
    hdrs = [
        "reader_utils.h",
        "varint.h",
    ],
    deps = [
        ":reader",
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "pushable_writer",
    srcs = ["pushable_writer.cc"],
    hdrs = ["pushable_writer.h"],
    deps = [
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "pullable_reader",
    srcs = ["pullable_reader.cc"],
    hdrs = ["pullable_reader.h"],
    deps = [
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "buffered_writer",
    srcs = ["buffered_writer.cc"],
    hdrs = ["buffered_writer.h"],
    deps = [
        ":writer",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "buffered_reader",
    srcs = ["buffered_reader.cc"],
    hdrs = ["buffered_reader.h"],
    deps = [
        ":backward_writer",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "limiting_writer",
    srcs = ["limiting_writer.cc"],
    hdrs = ["limiting_writer.h"],
    deps = [
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "limiting_backward_writer",
    srcs = ["limiting_backward_writer.cc"],
    hdrs = ["limiting_backward_writer.h"],
    deps = [
        ":backward_writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "limiting_reader",
    srcs = ["limiting_reader.cc"],
    hdrs = ["limiting_reader.h"],
    deps = [
        ":backward_writer",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "null_writer",
    srcs = ["null_writer.cc"],
    hdrs = ["null_writer.h"],
    deps = [
        ":writer",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "null_backward_writer",
    srcs = ["null_backward_writer.cc"],
    hdrs = ["null_backward_writer.h"],
    deps = [
        ":backward_writer",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "tee_writer",
    srcs = ["tee_writer.cc"],
    hdrs = ["tee_writer.h"],
    deps = [
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "tee_reader",
    srcs = ["tee_reader.cc"],
    hdrs = ["tee_reader.h"],
    deps = [
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "array_writer",
    srcs = [
        "array_writer.cc",
        "span_dependency.h",
    ],
    hdrs = ["array_writer.h"],
    deps = [
        ":writer",
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "array_backward_writer",
    srcs = [
        "array_backward_writer.cc",
        "span_dependency.h",
    ],
    hdrs = ["array_backward_writer.h"],
    deps = [
        ":backward_writer",
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "string_writer",
    srcs = ["string_writer.cc"],
    hdrs = ["string_writer.h"],
    deps = [
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "string_reader",
    srcs = [
        "string_reader.cc",
        "string_view_dependency.h",
    ],
    hdrs = ["string_reader.h"],
    deps = [
        ":reader",
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "chain_writer",
    srcs = ["chain_writer.cc"],
    hdrs = ["chain_writer.h"],
    deps = [
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "chain_backward_writer",
    srcs = ["chain_backward_writer.cc"],
    hdrs = ["chain_backward_writer.h"],
    deps = [
        ":backward_writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "chain_reader",
    srcs = ["chain_reader.cc"],
    hdrs = ["chain_reader.h"],
    deps = [
        ":backward_writer",
        ":pullable_reader",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "fd_writer",
    srcs = [
        "fd_dependency.h",
        "fd_writer.cc",
    ],
    hdrs = ["fd_writer.h"],
    deps = [
        ":buffered_writer",
        "//riegeli/base",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "fd_reader",
    srcs = [
        "fd_dependency.h",
        "fd_reader.cc",
    ],
    hdrs = ["fd_reader.h"],
    deps = [
        ":buffered_reader",
        ":chain_reader",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:memory_estimator",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "brotli_writer",
    srcs = ["brotli_writer.cc"],
    hdrs = ["brotli_writer.h"],
    deps = [
        ":buffered_writer",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@org_brotli//:brotlienc",
    ],
)

cc_library(
    name = "brotli_reader",
    srcs = ["brotli_reader.cc"],
    hdrs = ["brotli_reader.h"],
    deps = [
        ":pullable_reader",
        ":reader",
        "//riegeli/base",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@org_brotli//:brotlidec",
    ],
)

cc_library(
    name = "zstd_writer",
    srcs = ["zstd_writer.cc"],
    hdrs = ["zstd_writer.h"],
    deps = [
        ":buffered_writer",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@net_zstd//:zstdlib",
    ],
)

cc_library(
    name = "zstd_reader",
    srcs = ["zstd_reader.cc"],
    hdrs = ["zstd_reader.h"],
    deps = [
        ":buffered_reader",
        ":reader",
        "//riegeli/base",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@net_zstd//:zstdlib",
    ],
)

cc_library(
    name = "zlib_writer",
    srcs = ["zlib_writer.cc"],
    hdrs = ["zlib_writer.h"],
    deps = [
        ":buffered_writer",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@zlib",
    ],
)

cc_library(
    name = "zlib_reader",
    srcs = ["zlib_reader.cc"],
    hdrs = ["zlib_reader.h"],
    deps = [
        ":buffered_reader",
        ":reader",
        "//riegeli/base",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@zlib",
    ],
)

cc_library(
    name = "snappy_writer",
    srcs = ["snappy_writer.cc"],
    hdrs = ["snappy_writer.h"],
    deps = [
        ":chain_reader",
        ":snappy_streams",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@snappy",
    ],
)

cc_library(
    name = "snappy_reader",
    srcs = ["snappy_reader.cc"],
    hdrs = ["snappy_reader.h"],
    deps = [
        ":chain_reader",
        ":chain_writer",
        ":reader",
        ":snappy_streams",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@snappy",
    ],
)

cc_library(
    name = "snappy_streams",
    srcs = ["snappy_streams.cc"],
    hdrs = ["snappy_streams.h"],
    visibility = ["//visibility:private"],
    deps = [
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@snappy",
    ],
)

cc_library(
    name = "framed_snappy_writer",
    srcs = ["framed_snappy_writer.cc"],
    hdrs = ["framed_snappy_writer.h"],
    deps = [
        ":pushable_writer",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:endian",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@crc32c",
        "@snappy",
    ],
)

cc_library(
    name = "framed_snappy_reader",
    srcs = ["framed_snappy_reader.cc"],
    hdrs = ["framed_snappy_reader.h"],
    deps = [
        ":pullable_reader",
        ":reader",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:endian",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@crc32c",
        "@snappy",
    ],
)

cc_library(
    name = "message_serialize",
    srcs = ["message_serialize.cc"],
    hdrs = ["message_serialize.h"],
    deps = [
        ":chain_writer",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_protobuf//:protobuf_lite",
    ],
)

cc_library(
    name = "message_parse",
    srcs = ["message_parse.cc"],
    hdrs = ["message_parse.h"],
    deps = [
        ":chain_reader",
        ":reader",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_protobuf//:protobuf_lite",
    ],
)
