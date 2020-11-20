package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "framed_snappy_writer",
    srcs = ["framed_snappy_writer.cc"],
    hdrs = ["framed_snappy_writer.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:status",
        "//riegeli/bytes:pushable_writer",
        "//riegeli/bytes:writer",
        "//riegeli/endian:endian_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@crc32c",
        "@snappy",
    ],
)

cc_library(
    name = "framed_snappy_reader",
    srcs = ["framed_snappy_reader.cc"],
    hdrs = ["framed_snappy_reader.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:status",
        "//riegeli/bytes:pullable_reader",
        "//riegeli/bytes:reader",
        "//riegeli/endian:endian_reading",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@crc32c",
        "@snappy",
    ],
)
