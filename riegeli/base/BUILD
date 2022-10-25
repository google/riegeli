package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "type_traits",
    hdrs = ["type_traits.h"],
)

cc_library(
    name = "constexpr",
    srcs = ["port.h"],
    hdrs = ["constexpr.h"],
    deps = [":type_traits"],
)

cc_library(
    name = "no_destructor",
    hdrs = ["no_destructor.h"],
)

cc_library(
    name = "assert",
    srcs = [
        "assert.cc",
        "port.h",
    ],
    hdrs = ["assert.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "types",
    hdrs = ["types.h"],
)

cc_library(
    name = "arithmetic",
    hdrs = ["arithmetic.h"],
    deps = [
        ":assert",
        ":type_traits",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/numeric:bits",
    ],
)

cc_library(
    name = "buffering",
    hdrs = ["buffering.h"],
    deps = [
        ":arithmetic",
        ":constexpr",
        ":types",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "estimated_allocated_size",
    hdrs = ["estimated_allocated_size.h"],
    deps = [":arithmetic"],
)

cc_library(
    name = "new_aligned",
    hdrs = ["new_aligned.h"],
    deps = [
        ":arithmetic",
        ":assert",
        ":estimated_allocated_size",
        "@com_google_absl//absl/numeric:bits",
    ],
)

cc_library(
    name = "string_utils",
    srcs = ["string_utils.cc"],
    hdrs = ["string_utils.h"],
    deps = [":arithmetic"],
)

cc_library(
    name = "cord_utils",
    srcs = ["cord_utils.cc"],
    hdrs = ["cord_utils.h"],
    deps = [
        ":string_utils",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "zeros",
    srcs = ["zeros.cc"],
    hdrs = ["zeros.h"],
    deps = [
        ":cord_utils",
        ":no_destructor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "type_id",
    hdrs = ["type_id.h"],
)

cc_library(
    name = "reset",
    hdrs = ["reset.h"],
    deps = [
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "dependency",
    hdrs = ["dependency.h"],
    deps = [
        ":reset",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "stable_dependency",
    hdrs = ["stable_dependency.h"],
    deps = [
        ":assert",
        ":dependency",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "any_dependency",
    hdrs = ["any_dependency.h"],
    deps = [
        ":arithmetic",
        ":assert",
        ":dependency",
        ":type_id",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/utility",
    ],
)

cc_library(
    name = "status",
    srcs = [
        "errno_mapping.cc",
        "status.cc",
    ],
    hdrs = [
        "errno_mapping.h",
        "status.h",
    ],
    # errno_mapping.cc has #define before #include to influence what the
    # included files provide.
    features = ["-use_header_modules"],
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "object",
    srcs = ["object.cc"],
    hdrs = ["object.h"],
    deps = [
        ":assert",
        ":constexpr",
        ":type_id",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
    ],
)

# TODO: Remove this library once users are migrated to more specific
# libraries.
cc_library(
    name = "base",
    hdrs = [
        "any_dependency.h",
        "base.h",
        "dependency.h",
        "memory.h",
        "object.h",
    ],
    deps = [
        ":any_dependency",
        ":arithmetic",
        ":assert",
        ":buffering",
        ":constexpr",
        ":dependency",
        ":estimated_allocated_size",
        ":object",
        ":reset",
        ":type_id",
        ":types",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_absl//absl/utility",
    ],
)

cc_library(
    name = "memory_estimator",
    srcs = ["memory_estimator.cc"],
    hdrs = ["memory_estimator.h"],
    deps = [
        ":arithmetic",
        ":estimated_allocated_size",
        "@com_google_absl//absl/container:flat_hash_set",
    ],
)

cc_library(
    name = "intrusive_ref_count",
    hdrs = ["intrusive_ref_count.h"],
    deps = [
        ":assert",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "buffer",
    srcs = ["buffer.cc"],
    hdrs = ["buffer.h"],
    deps = [
        ":assert",
        ":buffering",
        ":cord_utils",
        ":estimated_allocated_size",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "shared_buffer",
    srcs = ["shared_buffer.cc"],
    hdrs = ["shared_buffer.h"],
    deps = [
        ":assert",
        ":buffering",
        ":cord_utils",
        ":intrusive_ref_count",
        ":new_aligned",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "chain",
    srcs = ["chain.cc"],
    hdrs = ["chain.h"],
    deps = [
        ":arithmetic",
        ":assert",
        ":buffering",
        ":cord_utils",
        ":intrusive_ref_count",
        ":memory_estimator",
        ":new_aligned",
        ":no_destructor",
        ":string_utils",
        ":zeros",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:compare",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "binary_search",
    hdrs = ["binary_search.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/types:compare",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "recycling_pool",
    hdrs = ["recycling_pool.h"],
    deps = [
        ":assert",
        ":no_destructor",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "parallelism",
    srcs = ["parallelism.cc"],
    hdrs = ["parallelism.h"],
    visibility = ["//riegeli:__subpackages__"],
    deps = [
        ":assert",
        ":no_destructor",
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
        ":assert",
        ":object",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)
