package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "writer",
    hdrs = ["writer.h"],
    deps = [
        ":reader_and_writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "backward_writer",
    hdrs = ["backward_writer.h"],
    deps = [
        ":reader_and_writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "reader",
    hdrs = ["reader.h"],
    deps = [
        ":reader_and_writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "reader_and_writer",
    srcs = [
        "backward_writer.cc",
        "reader.cc",
        "writer.cc",
    ],
    hdrs = [
        "backward_writer.h",
        "reader.h",
        "writer.h",
    ],
    visibility = ["//visibility:private"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "pushable_writer",
    srcs = ["pushable_writer.cc"],
    hdrs = ["pushable_writer.h"],
    deps = [
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "pushable_backward_writer",
    srcs = ["pushable_backward_writer.cc"],
    hdrs = ["pushable_backward_writer.h"],
    deps = [
        ":backward_writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "pullable_reader",
    srcs = ["pullable_reader.cc"],
    hdrs = ["pullable_reader.h"],
    deps = [
        ":backward_writer",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "buffered_writer",
    srcs = ["buffered_writer.cc"],
    hdrs = ["buffered_writer.h"],
    deps = [
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "buffered_reader",
    srcs = ["buffered_reader.cc"],
    hdrs = ["buffered_reader.h"],
    deps = [
        ":backward_writer",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "wrapped_writer",
    srcs = ["wrapped_writer.cc"],
    hdrs = ["wrapped_writer.h"],
    deps = [
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "wrapped_backward_writer",
    srcs = ["wrapped_backward_writer.cc"],
    hdrs = ["wrapped_backward_writer.h"],
    deps = [
        ":backward_writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "wrapped_reader",
    srcs = ["wrapped_reader.cc"],
    hdrs = ["wrapped_reader.h"],
    deps = [
        ":backward_writer",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "digesting_writer",
    srcs = [
        "digesting_common.h",
        "digesting_writer.cc",
    ],
    hdrs = ["digesting_writer.h"],
    deps = [
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "digesting_reader",
    srcs = [
        "digesting_common.h",
        "digesting_reader.cc",
    ],
    hdrs = ["digesting_reader.h"],
    deps = [
        ":reader",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "limiting_writer",
    srcs = ["limiting_writer.cc"],
    hdrs = ["limiting_writer.h"],
    deps = [
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "limiting_backward_writer",
    srcs = ["limiting_backward_writer.cc"],
    hdrs = ["limiting_backward_writer.h"],
    deps = [
        ":backward_writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "limiting_reader",
    srcs = ["limiting_reader.cc"],
    hdrs = ["limiting_reader.h"],
    deps = [
        ":backward_writer",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "prefix_limiting_writer",
    srcs = ["prefix_limiting_writer.cc"],
    hdrs = ["prefix_limiting_writer.h"],
    deps = [
        ":prefix_limiting_reader",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "prefix_limiting_reader",
    srcs = ["prefix_limiting_reader.cc"],
    hdrs = ["prefix_limiting_reader.h"],
    deps = [
        ":backward_writer",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "null_writer",
    srcs = ["null_writer.cc"],
    hdrs = ["null_writer.h"],
    deps = [
        ":writer",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "null_backward_writer",
    srcs = ["null_backward_writer.cc"],
    hdrs = ["null_backward_writer.h"],
    deps = [
        ":backward_writer",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "splitting_writer",
    srcs = ["splitting_writer.cc"],
    hdrs = ["splitting_writer.h"],
    deps = [
        ":chain_reader",
        ":cord_reader",
        ":pushable_writer",
        ":string_reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "joining_reader",
    srcs = ["joining_reader.cc"],
    hdrs = ["joining_reader.h"],
    deps = [
        ":pullable_reader",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "reader_factory",
    srcs = ["reader_factory.cc"],
    hdrs = ["reader_factory.h"],
    deps = [
        ":pullable_reader",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "array_writer",
    srcs = [
        "array_writer.cc",
        "span_dependency.h",
    ],
    hdrs = ["array_writer.h"],
    deps = [
        ":pushable_writer",
        ":reader",
        ":string_reader",
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "array_backward_writer",
    srcs = [
        "array_backward_writer.cc",
        "span_dependency.h",
    ],
    hdrs = ["array_backward_writer.h"],
    deps = [
        ":pushable_backward_writer",
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "string_writer",
    srcs = ["string_writer.cc"],
    hdrs = ["string_writer.h"],
    deps = [
        ":reader",
        ":string_reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "string_reader",
    srcs = [
        "string_reader.cc",
        "string_view_dependency.h",
    ],
    hdrs = ["string_reader.h"],
    deps = [
        ":reader",
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "chain_writer",
    srcs = ["chain_writer.cc"],
    hdrs = ["chain_writer.h"],
    deps = [
        ":chain_reader",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "chain_backward_writer",
    srcs = ["chain_backward_writer.cc"],
    hdrs = ["chain_backward_writer.h"],
    deps = [
        ":backward_writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "chain_reader",
    srcs = ["chain_reader.cc"],
    hdrs = ["chain_reader.h"],
    deps = [
        ":backward_writer",
        ":pullable_reader",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "cord_writer",
    srcs = ["cord_writer.cc"],
    hdrs = ["cord_writer.h"],
    deps = [
        ":cord_reader",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "cord_backward_writer",
    srcs = ["cord_backward_writer.cc"],
    hdrs = ["cord_backward_writer.h"],
    deps = [
        ":backward_writer",
        "//riegeli/base",
        "//riegeli/base:buffer",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "cord_reader",
    srcs = ["cord_reader.cc"],
    hdrs = ["cord_reader.h"],
    deps = [
        ":backward_writer",
        ":pullable_reader",
        ":reader",
        ":writer",
        "//riegeli/base",
        "//riegeli/base:chain",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "fd_writer",
    srcs = ["fd_writer.cc"],
    hdrs = ["fd_writer.h"],
    deps = [
        ":buffered_writer",
        ":fd_dependency",
        ":fd_reader",
        ":reader",
        "//riegeli/base",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "fd_reader",
    srcs = ["fd_reader.cc"],
    hdrs = ["fd_reader.h"],
    deps = [
        ":buffered_reader",
        ":chain_reader",
        ":fd_dependency",
        ":reader",
        "//riegeli/base",
        "//riegeli/base:chain",
        "//riegeli/base:memory_estimator",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "std_io",
    srcs = ["std_io.cc"],
    hdrs = ["std_io.h"],
    deps = [
        ":fd_reader",
        ":fd_writer",
        ":reader",
        ":writer",
        "//riegeli/base",
    ],
)

cc_library(
    name = "fd_dependency",
    srcs = ["fd_dependency.cc"],
    hdrs = ["fd_dependency.h"],
    visibility = ["//visibility:private"],
    deps = [
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "ostream_writer",
    srcs = [
        "ostream_writer.cc",
        "stream_dependency.h",
    ],
    hdrs = ["ostream_writer.h"],
    deps = [
        ":buffered_writer",
        ":istream_reader",
        ":reader",
        "//riegeli/base",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "istream_reader",
    srcs = [
        "istream_reader.cc",
        "stream_dependency.h",
    ],
    hdrs = ["istream_reader.h"],
    deps = [
        ":buffered_reader",
        "//riegeli/base",
        "//riegeli/base:status",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "writer_ostream",
    srcs = ["writer_ostream.cc"],
    hdrs = ["writer_ostream.h"],
    deps = [
        ":reader",
        ":writer",
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "reader_istream",
    srcs = ["reader_istream.cc"],
    hdrs = ["reader_istream.h"],
    deps = [
        ":reader",
        "//riegeli/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/types:optional",
    ],
)
