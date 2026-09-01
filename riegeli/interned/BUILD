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
        "//riegeli/base:cord_utils",
        "//riegeli/base:external_data",
        "//riegeli/base:global",
        "//riegeli/base:initializer",
        "//riegeli/base:null_safe_memcpy",
        "//riegeli/base:shared_ptr",
        "//riegeli/base:type_traits",
        "@com_google_absl//absl/base:config",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:hash_container_defaults",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/numeric:bits",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "interned_string",
    srcs = [
        "interned_common_internal.h",
        "interned_internal.h",
        "interned_string_internal.h",
    ],
    hdrs = ["interned_string.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:compare",
        "//riegeli/base:cord_utils",
        "//riegeli/base:external_data",
        "//riegeli/base:global",
        "//riegeli/base:initializer",
        "//riegeli/base:iterable",
        "//riegeli/base:new_aligned",
        "//riegeli/base:null_safe_memcpy",
        "//riegeli/base:shared_ptr",
        "//riegeli/base:type_traits",
        "@com_google_absl//absl/base:config",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:hash_container_defaults",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/numeric:bits",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)
