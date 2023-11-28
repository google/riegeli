package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "write_int",
    srcs = ["write_int.cc"],
    hdrs = ["write_int.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/bytes:ostream_writer",
        "//riegeli/bytes:write_int_internal",
        "//riegeli/bytes:writer",
        "//riegeli/endian:endian_writing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/numeric:bits",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "ascii_align",
    hdrs = ["ascii_align.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "//riegeli/base:chain",
        "//riegeli/base:type_traits",
        "//riegeli/base:types",
        "//riegeli/bytes:absl_stringify_writer",
        "//riegeli/bytes:ostream_writer",
        "//riegeli/bytes:restricted_chain_writer",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
    ],
)
