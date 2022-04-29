package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "hadoop_snappy_reader",
    srcs = ["hadoop_snappy_reader.cc"],
    hdrs = ["hadoop_snappy_reader.h"],
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
        "@snappy",
    ],
)

cc_library(
    name = "hadoop_snappy_writer",
    srcs = ["hadoop_snappy_writer.cc"],
    hdrs = ["hadoop_snappy_writer.h"],
    deps = [
        ":hadoop_snappy_reader",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:status",
        "//riegeli/bytes:pushable_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "//riegeli/endian:endian_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@snappy",
    ],
)
