load("@rules_cc//cc:defs.bzl", "cc_library")

package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "framed_snappy_reader",
    srcs = ["framed_snappy_reader.cc"],
    hdrs = ["framed_snappy_reader.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffer",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:moving_dependency",
        "//riegeli/base:object",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:pullable_reader",
        "//riegeli/bytes:reader",
        "//riegeli/endian:endian_reading",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/crc:crc32c",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@snappy",
    ],
)

cc_library(
    name = "framed_snappy_writer",
    srcs = ["framed_snappy_writer.cc"],
    hdrs = ["framed_snappy_writer.h"],
    deps = [
        ":framed_snappy_reader",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffer",
        "//riegeli/base:buffering",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:pushable_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "//riegeli/digests:crc32c_digester",
        "//riegeli/digests:digesting_writer",
        "//riegeli/endian:endian_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@snappy",
    ],
)
