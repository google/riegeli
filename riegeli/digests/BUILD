package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "digesting_reader",
    srcs = [
        "digesting_internal.h",
        "digesting_reader.cc",
    ],
    hdrs = ["digesting_reader.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "digesting_writer",
    srcs = [
        "digesting_internal.h",
        "digesting_writer.cc",
    ],
    hdrs = ["digesting_writer.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "crc32c_digester",
    hdrs = ["crc32c_digester.h"],
    deps = [
        "@com_google_absl//absl/strings",
        "@crc32c",
    ],
)
