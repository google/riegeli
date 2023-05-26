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
        "//riegeli/base:memory_estimator",
        "//riegeli/bytes:compact_string_writer",
        "//riegeli/varint:varint_reading",
        "//riegeli/varint:varint_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "chunked_sorted_string_set",
    srcs = ["chunked_sorted_string_set.cc"],
    hdrs = ["chunked_sorted_string_set.h"],
    deps = [
        ":linear_sorted_string_set",
        "//riegeli/base:assert",
        "//riegeli/base:binary_search",
        "//riegeli/base:memory_estimator",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:compare",
        "@com_google_absl//absl/types:optional",
    ],
)
