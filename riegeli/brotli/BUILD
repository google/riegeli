package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "brotli_writer",
    srcs = ["brotli_writer.cc"],
    hdrs = ["brotli_writer.h"],
    deps = [
        ":brotli_allocator",
        ":brotli_dictionary",
        "//riegeli/base",
        "//riegeli/base:status",
        "//riegeli/bytes:buffered_writer",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@org_brotli//:brotlienc",
    ],
)

cc_library(
    name = "brotli_reader",
    srcs = ["brotli_reader.cc"],
    hdrs = ["brotli_reader.h"],
    deps = [
        ":brotli_allocator",
        ":brotli_dictionary",
        "//riegeli/base",
        "//riegeli/base:status",
        "//riegeli/bytes:pullable_reader",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@org_brotli//:brotlicommon",
        "@org_brotli//:brotlidec",
    ],
)

cc_library(
    name = "brotli_dictionary",
    srcs = ["brotli_dictionary.cc"],
    hdrs = ["brotli_dictionary.h"],
    visibility = ["//visibility:private"],
    deps = [
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@org_brotli//:brotlicommon",
        "@org_brotli//:brotlienc",
    ],
)

cc_library(
    name = "brotli_allocator",
    srcs = ["brotli_allocator.cc"],
    hdrs = ["brotli_allocator.h"],
    visibility = ["//visibility:private"],
    deps = ["@org_brotli//:brotlicommon"],
)
