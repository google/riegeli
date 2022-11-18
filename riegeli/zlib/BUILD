package(
    default_applicable_licenses = ["//riegeli:license"],
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "zlib_reader",
    srcs = [
        "zlib_dictionary.h",
        "zlib_reader.cc",
    ],
    hdrs = ["zlib_reader.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_reader",
        "//riegeli/bytes:reader",
        "//riegeli/endian:endian_reading",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@zlib",
    ],
)

cc_library(
    name = "zlib_writer",
    srcs = [
        "zlib_dictionary.h",
        "zlib_writer.cc",
    ],
    hdrs = ["zlib_writer.h"],
    deps = [
        ":zlib_reader",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@zlib",
    ],
)
