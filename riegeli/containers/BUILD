load("@rules_cc//cc:defs.bzl", "cc_library")

package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "linear_sorted_string_set",
    srcs = ["linear_sorted_string_set.cc"],
    hdrs = ["linear_sorted_string_set.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:compact_string",
        "//riegeli/base:compare",
        "//riegeli/base:debug",
        "//riegeli/base:dependency",
        "//riegeli/base:iterable",
        "//riegeli/base:stream_utils",
        "//riegeli/bytes:compact_string_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "//riegeli/endian:endian_reading",
        "//riegeli/varint:varint_reading",
        "//riegeli/varint:varint_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/numeric:bits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "chunked_sorted_string_set",
    srcs = ["chunked_sorted_string_set.cc"],
    hdrs = ["chunked_sorted_string_set.h"],
    deps = [
        ":linear_sorted_string_set",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:binary_search",
        "//riegeli/base:compact_string",
        "//riegeli/base:compare",
        "//riegeli/base:debug",
        "//riegeli/base:dependency",
        "//riegeli/base:iterable",
        "//riegeli/base:memory_estimator",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "//riegeli/varint:varint_reading",
        "//riegeli/varint:varint_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
)
