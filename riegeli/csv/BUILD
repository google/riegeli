package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "csv_reader",
    srcs = ["csv_reader.cc"],
    hdrs = ["csv_reader.h"],
    deps = [
        ":csv_record",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:object",
        "//riegeli/base:status",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:string_reader",
        "//riegeli/bytes:string_writer",
        "//riegeli/lines:line_reading",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "csv_writer",
    srcs = ["csv_writer.cc"],
    hdrs = ["csv_writer.h"],
    deps = [
        ":csv_record",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:dependency",
        "//riegeli/base:object",
        "//riegeli/base:status",
        "//riegeli/base:type_traits",
        "//riegeli/bytes:string_writer",
        "//riegeli/bytes:writer",
        "//riegeli/lines:line_writing",
        "//riegeli/lines:newline",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "csv_record",
    srcs = ["csv_record.cc"],
    hdrs = ["csv_record.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:compare",
        "//riegeli/base:intrusive_ref_count",
        "//riegeli/base:no_destructor",
        "//riegeli/base:type_traits",
        "//riegeli/bytes:absl_stringify_writer",
        "//riegeli/bytes:ostream_writer",
        "//riegeli/bytes:string_writer",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)
