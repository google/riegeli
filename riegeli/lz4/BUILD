package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "lz4_reader",
    srcs = ["lz4_reader.cc"],
    hdrs = ["lz4_reader.h"],
    # zstd_reader.cc has #define before #include to influence what the included
    # files provide.
    features = ["-use_header_modules"],
    deps = [
        ":lz4_dictionary",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_reader",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@lz4//:lz4_frame",
    ],
)

cc_library(
    name = "lz4_writer",
    srcs = ["lz4_writer.cc"],
    hdrs = ["lz4_writer.h"],
    # lz4_writer.cc has #define before #include to influence what the included
    # files provide.
    features = ["-use_header_modules"],
    deps = [
        ":lz4_dictionary",
        ":lz4_reader",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
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
        "@lz4",
        "@lz4//:lz4_frame",
    ],
)

cc_library(
    name = "lz4_dictionary",
    srcs = ["lz4_dictionary.cc"],
    hdrs = ["lz4_dictionary.h"],
    # lz4_dictionary.cc has #define before #include to influence what the
    # included files provide.
    features = ["-use_header_modules"],
    visibility = ["//visibility:private"],
    deps = [
        "//riegeli/base:bytes_ref",
        "//riegeli/base:initializer",
        "//riegeli/base:shared_ptr",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@lz4//:lz4_frame",
    ],
)
