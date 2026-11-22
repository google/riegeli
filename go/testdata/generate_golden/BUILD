load("@rules_cc//cc:defs.bzl", "cc_binary")

package(features = ["header_modules"])

licenses(["notice"])

cc_binary(
    name = "generate_golden_files",
    srcs = ["generate_golden_files.cc"],
    deps = [
        "//riegeli/bytes:fd_writer",
        "//riegeli/records:record_writer",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
    ],
)

cc_binary(
    name = "read_records",
    srcs = ["read_records.cc"],
    deps = [
        "//riegeli/bytes:fd_reader",
        "//riegeli/records:record_reader",
        "@com_google_absl//absl/strings:string_view",
    ],
)
