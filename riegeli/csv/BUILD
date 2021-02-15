package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "csv_writer",
    srcs = ["csv_writer.cc"],
    hdrs = ["csv_writer.h"],
    deps = [
        ":csv_record",
        "//riegeli/base",
        "//riegeli/base:status",
        "//riegeli/bytes:string_writer",
        "//riegeli/bytes:writer",
        "//riegeli/lines:line_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "csv_reader",
    srcs = ["csv_reader.cc"],
    hdrs = ["csv_reader.h"],
    deps = [
        ":csv_record",
        "//riegeli/base",
        "//riegeli/base:status",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:string_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "csv_record",
    srcs = ["csv_record.cc"],
    hdrs = ["csv_record.h"],
    deps = [
        "//riegeli/base",
        "//riegeli/base:intrusive_ref_count",
        "//riegeli/bytes:string_writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
    ],
)
