package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "status",
    srcs = [
        "canonical_errors.cc",
        "errno_mapping.cc",
        "status.cc",
    ],
    hdrs = [
        "canonical_errors.h",
        "errno_mapping.h",
        "status.h",
    ],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/utility",
    ],
)

cc_library(
    name = "base",
    srcs = [
        "base.cc",
        "object.cc",
    ],
    hdrs = [
        "base.h",
        "dependency.h",
        "memory.h",
        "object.h",
        "port.h",
        "stable_dependency.h",
    ],
    deps = [
        ":status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/utility",
    ],
)

cc_library(
    name = "memory_estimator",
    srcs = ["memory_estimator.cc"],
    hdrs = ["memory_estimator.h"],
    deps = [
        ":base",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/utility",
    ],
)

cc_library(
    name = "chain",
    srcs = ["chain.cc"],
    hdrs = ["chain.h"],
    deps = [
        ":base",
        ":memory_estimator",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@com_google_absl//absl/utility",
    ],
)

cc_library(
    name = "buffer",
    hdrs = ["buffer.h"],
    deps = [
        ":base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/utility",
    ],
)

cc_library(
    name = "recycling_pool",
    hdrs = ["recycling_pool.h"],
    deps = [
        ":base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "endian",
    hdrs = ["endian.h"],
)

cc_library(
    name = "parallelism",
    srcs = ["parallelism.cc"],
    hdrs = ["parallelism.h"],
    visibility = ["//riegeli:__subpackages__"],
    deps = [
        ":base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "options_parser",
    srcs = ["options_parser.cc"],
    hdrs = ["options_parser.h"],
    deps = [
        ":base",
        ":status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)
