package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "file_reader",
    srcs = ["file_reader.cc"],
    hdrs = ["file_reader.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffering",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:external_ref",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:reset",
        "//riegeli/base:sized_shared_buffer",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:backward_writer",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:path_ref",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:span",
        "@local_config_tf//:tf_header_lib",
    ],
)

cc_library(
    name = "file_writer",
    srcs = ["file_writer.cc"],
    hdrs = ["file_writer.h"],
    deps = [
        ":file_reader",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffering",
        "//riegeli/base:byte_fill",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:external_ref",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:reset",
        "//riegeli/base:shared_buffer",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:path_ref",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@local_config_tf//:tf_header_lib",
    ],
)

cc_library(
    name = "tstring_writer",
    hdrs = ["tstring_writer.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/bytes:resizable_writer",
        "@com_google_absl//absl/meta:type_traits",
        "@local_config_tf//:tf_header_lib",
    ],
)
