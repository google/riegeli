load("@rules_cc//cc:defs.bzl", "cc_library")

package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "interned_object",
    srcs = [
        "interned_common_internal.h",
        "interned_internal.h",
        "interned_object_internal.h",
    ],
    hdrs = ["interned_object.h"],
    deps = [
        "//riegeli/base:assert",
        "//riegeli/base:compare",
        "//riegeli/base:external_data",
        "//riegeli/base:global",
        "//riegeli/base:initializer",
        "//riegeli/base:shared_ptr",
        "//riegeli/base:type_traits",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:hash_container_defaults",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/numeric:bits",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)
