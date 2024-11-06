package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "type_traits",
    hdrs = ["type_traits.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "constexpr",
    srcs = ["port.h"],
    hdrs = ["constexpr.h"],
    deps = [":type_traits"],
)

cc_library(
    name = "global",
    hdrs = ["global.h"],
    deps = [
        ":type_traits",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

cc_library(
    name = "compare",
    hdrs = ["compare.h"],
    deps = [
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:compare",
    ],
)

cc_library(
    name = "stream_utils",
    srcs = ["stream_utils.cc"],
    hdrs = ["stream_utils.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "debug",
    srcs = ["debug.cc"],
    hdrs = ["debug.h"],
    deps = [
        ":stream_utils",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "assert",
    srcs = [
        "assert.cc",
        "port.h",
    ],
    hdrs = ["assert.h"],
    deps = [
        ":debug",
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
        "@com_google_absl//absl/numeric:int128",
    ],
)

cc_library(
    name = "buffering",
    hdrs = ["buffering.h"],
    deps = [
        ":arithmetic",
        ":assert",
        ":constexpr",
        ":types",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "estimated_allocated_size",
    hdrs = ["estimated_allocated_size.h"],
    deps = [
        ":arithmetic",
        "@com_google_absl//absl/base:core_headers",
    ],
)

cc_library(
    name = "new_aligned",
    hdrs = ["new_aligned.h"],
    deps = [
        ":arithmetic",
        ":assert",
        ":estimated_allocated_size",
        "@com_google_absl//absl/base:core_headers",
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
        ":arithmetic",
        ":buffering",
        ":constexpr",
        ":string_utils",
        "@com_google_absl//absl/numeric:bits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "unicode",
    srcs = ["unicode.cc"],
    hdrs = ["unicode.h"],
    features = select({
        # unicode.cc has #define before #include to influence what the included
        # files provide.
        "@platforms//os:windows": ["-use_header_modules"],
        "//conditions:default": [],
    }),
    deps = select({
        "@platforms//os:windows": [
            ":arithmetic",
            "@com_google_absl//absl/base:core_headers",
            "@com_google_absl//absl/strings",
            "@com_google_absl//absl/types:span",
        ],
        "//conditions:default": [],
    }),
)

cc_library(
    name = "type_id",
    hdrs = ["type_id.h"],
    deps = [":compare"],
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
    name = "type_erased_ref",
    hdrs = ["type_erased_ref.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

cc_library(
    name = "initializer",
    srcs = ["initializer_internal.h"],
    hdrs = [
        "initializer.h",
        "invoker.h",
        "maker.h",
        "temporary_storage.h",
    ],
    deps = [
        ":assert",
        ":reset",
        ":type_erased_ref",
        ":type_traits",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/utility",
    ],
)

cc_library(
    name = "to_string_view",
    hdrs = ["to_string_view.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "memory_estimator",
    srcs = ["memory_estimator.cc"],
    hdrs = ["memory_estimator.h"],
    deps = [
        ":arithmetic",
        ":estimated_allocated_size",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/container:node_hash_map",
        "@com_google_absl//absl/container:node_hash_set",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:variant",
    ],
)

cc_library(
    name = "closing_ptr",
    hdrs = ["closing_ptr.h"],
    deps = ["@com_google_absl//absl/base:core_headers"],
)

cc_library(
    name = "dependency",
    hdrs = [
        "dependency.h",
        "dependency_base.h",
        "dependency_manager.h",
    ],
    deps = [
        ":assert",
        ":compare",
        ":initializer",
        ":reset",
        ":to_string_view",
        ":type_id",
        ":type_traits",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "moving_dependency",
    hdrs = ["moving_dependency.h"],
    deps = [
        ":dependency",
        ":type_traits",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

cc_library(
    name = "stable_dependency",
    hdrs = ["stable_dependency.h"],
    deps = [
        ":dependency",
        ":initializer",
        ":type_traits",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

cc_library(
    name = "any",
    srcs = ["any_internal.h"],
    hdrs = [
        "any.h",
        "any_initializer.h",
    ],
    deps = [
        ":arithmetic",
        ":assert",
        ":compare",
        ":dependency",
        ":initializer",
        ":memory_estimator",
        ":type_erased_ref",
        ":type_id",
        ":type_traits",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "iterable",
    hdrs = ["iterable.h"],
    deps = [
        ":dependency",
        "@com_google_absl//absl/meta:type_traits",
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
    features = select({
        # errno_mapping.cc has #define before #include to influence what the
        # included files provide.
        "@platforms//os:windows": ["-use_header_modules"],
        "//conditions:default": [],
    }),
    deps = [
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ] + select({
        "@platforms//os:windows": [
            ":arithmetic",
            ":unicode",
            "@com_google_absl//absl/types:span",
        ],
        "//conditions:default": [],
    }),
)

cc_library(
    name = "object",
    srcs = ["object.cc"],
    hdrs = ["object.h"],
    deps = [
        ":assert",
        ":constexpr",
        ":initializer",
        ":type_id",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
    ],
)

cc_library(
    name = "external_data",
    srcs = ["external_data.cc"],
    hdrs = ["external_data.h"],
    deps = ["@com_google_absl//absl/strings"],
)

cc_library(
    name = "external_ref_support",
    hdrs = ["external_ref_support.h"],
    deps = [
        ":external_data",
        ":to_string_view",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

cc_library(
    name = "shared_ptr",
    hdrs = [
        "intrusive_shared_ptr.h",
        "ownership.h",
        "ref_count.h",
        "shared_ptr.h",
    ],
    deps = [
        ":arithmetic",
        ":assert",
        ":compare",
        ":constexpr",
        ":external_data",
        ":external_ref_support",
        ":initializer",
        ":new_aligned",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/meta:type_traits",
    ],
)

cc_library(
    name = "buffer",
    srcs = ["buffer.cc"],
    hdrs = ["buffer.h"],
    deps = [
        ":arithmetic",
        ":assert",
        ":buffering",
        ":estimated_allocated_size",
        ":external_data",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "shared_buffer",
    srcs = ["shared_buffer.cc"],
    hdrs = ["shared_buffer.h"],
    deps = [
        ":arithmetic",
        ":assert",
        ":buffer",
        ":external_data",
        ":initializer",
        ":shared_ptr",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "sized_shared_buffer",
    srcs = ["sized_shared_buffer.cc"],
    hdrs = ["sized_shared_buffer.h"],
    deps = [
        ":arithmetic",
        ":assert",
        ":buffering",
        ":shared_buffer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "chain_and_external_ref",
    srcs = ["chain.cc"],
    hdrs = [
        "chain_base.h",
        "chain_details.h",
        "external_ref_base.h",
    ],
    visibility = ["//visibility:private"],
    deps = [
        ":arithmetic",
        ":assert",
        ":buffering",
        ":compare",
        ":cord_utils",
        ":external_data",
        ":external_ref_support",
        ":initializer",
        ":memory_estimator",
        ":new_aligned",
        ":shared_ptr",
        ":stream_utils",
        ":string_utils",
        ":to_string_view",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "chain",
    hdrs = ["chain.h"],
    deps = [
        ":chain_and_external_ref",
        ":external_ref_support",
    ],
)

cc_library(
    name = "external_ref",
    hdrs = ["external_ref.h"],
    deps = [
        ":chain_and_external_ref",
        ":external_ref_support",
    ],
)

cc_library(
    name = "byte_fill",
    srcs = ["byte_fill.cc"],
    hdrs = ["byte_fill.h"],
    deps = [
        ":arithmetic",
        ":assert",
        ":chain",
        ":compare",
        ":cord_utils",
        ":external_data",
        ":external_ref",
        ":global",
        ":shared_buffer",
        ":types",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/numeric:bits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:span",
        "@com_google_absl//absl/types:variant",
    ],
)

cc_library(
    name = "compact_string",
    srcs = ["compact_string.cc"],
    hdrs = ["compact_string.h"],
    deps = [
        ":arithmetic",
        ":assert",
        ":compare",
        ":estimated_allocated_size",
        ":external_data",
        ":new_aligned",
        "@com_google_absl//absl/base:config",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "binary_search",
    hdrs = ["binary_search.h"],
    deps = [
        ":compare",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "parallelism",
    srcs = ["parallelism.cc"],
    hdrs = ["parallelism.h"],
    visibility = ["//riegeli:__subpackages__"],
    deps = [
        ":assert",
        ":global",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "background_cleaning",
    srcs = ["background_cleaning.cc"],
    hdrs = ["background_cleaning.h"],
    deps = [
        ":assert",
        ":global",
        ":parallelism",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "recycling_pool",
    srcs = ["recycling_pool.cc"],
    hdrs = ["recycling_pool.h"],
    deps = [
        ":arithmetic",
        ":assert",
        ":background_cleaning",
        ":compare",
        ":global",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/container:node_hash_map",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/numeric:bits",
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
        ":initializer",
        ":object",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)
