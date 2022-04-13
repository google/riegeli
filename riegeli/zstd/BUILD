package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "zstd_writer",
    srcs = ["zstd_writer.cc"],
    hdrs = ["zstd_writer.h"],
    # zstd_writer.cc has #define before #include to influence what the included
    # files provide.
    features = ["-use_header_modules"],
    deps = [
        ":zstd_dictionary",
        ":zstd_reader",
        "//riegeli/base",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:status",
        "//riegeli/bytes:buffered_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@net_zstd//:zstdlib",
    ],
)

cc_library(
    name = "zstd_reader",
    srcs = ["zstd_reader.cc"],
    hdrs = ["zstd_reader.h"],
    # zstd_reader.cc has #define before #include to influence what the included
    # files provide.
    features = ["-use_header_modules"],
    deps = [
        ":zstd_dictionary",
        "//riegeli/base",
        "//riegeli/base:recycling_pool",
        "//riegeli/base:status",
        "//riegeli/bytes:buffered_reader",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@net_zstd//:zstdlib",
    ],
)

cc_library(
    name = "zstd_dictionary",
    srcs = ["zstd_dictionary.cc"],
    hdrs = ["zstd_dictionary.h"],
    # zstd_dictionary.cc has #define before #include to influence what the included
    # files provide.
    features = ["-use_header_modules"],
    visibility = ["//visibility:private"],
    deps = [
        "//riegeli/base:intrusive_ref_count",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/strings",
        "@net_zstd//:zstdlib",
    ],
)
