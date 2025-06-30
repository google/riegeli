package(
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
        ":zlib_error",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:bytes_ref",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:shared_ptr",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_reader",
        "//riegeli/bytes:reader",
        "//riegeli/endian:endian_reading",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
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
        ":zlib_error",
        ":zlib_reader",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:bytes_ref",
        "//riegeli/base:compare",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:shared_ptr",
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

cc_library(
    name = "zlib_error",
    srcs = ["zlib_error.cc"],
    hdrs = ["zlib_error.h"],
    visibility = ["//visibility:private"],
    deps = [
        "//riegeli/base:assert",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@zlib",
    ],
)
