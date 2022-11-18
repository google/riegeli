package(
    default_applicable_licenses = ["//riegeli:license"],
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "ordered_varint_reading",
    srcs = [
        "ordered_varint_internal.h",
        "ordered_varint_reading.cc",
    ],
    hdrs = ["ordered_varint_reading.h"],
    deps = [
        "//riegeli/base:assert",
        "//riegeli/base:constexpr",
        "//riegeli/bytes:reader",
        "//riegeli/endian:endian_reading",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "ordered_varint_writing",
    srcs = [
        "ordered_varint_internal.h",
        "ordered_varint_writing.cc",
    ],
    hdrs = ["ordered_varint_writing.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:constexpr",
        "//riegeli/bytes:writer",
        "//riegeli/endian:endian_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/numeric:bits",
    ],
)
