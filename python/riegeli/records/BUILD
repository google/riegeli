load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:proto.bzl", "py_proto_library")
load("//python/riegeli:py_extension.bzl", "py_extension")

package(
    default_visibility = ["//python/riegeli:__subpackages__"],
    features = ["header_modules"],
)

licenses(["notice"])

py_extension(
    name = "record_reader",
    srcs = ["record_reader.cc"],
    # Python modules imported from C++.
    data = [
        ":records_metadata_py_pb2",
        ":skipped_region",
        "@com_google_protobuf//:protobuf_python",
    ],
    # record_reader.cc has #define before #include to influence what the
    # included files provide.
    features = ["-use_header_modules"],
    deps = [
        ":record_position_cc",
        "//python/riegeli/base:utils",
        "//python/riegeli/bytes:python_reader",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:compare",
        "//riegeli/base:types",
        "//riegeli/chunk_encoding:field_projection",
        "//riegeli/records:record_position",
        "//riegeli/records:record_reader",
        "//riegeli/records:skipped_region",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@rules_python//python/cc:current_py_cc_headers",
    ],
)

py_extension(
    name = "record_writer",
    srcs = ["record_writer.cc"],
    # record_writer.cc has #define before #include to influence what the
    # included files provide.
    features = ["-use_header_modules"],
    deps = [
        ":record_position_cc",
        "//python/riegeli/base:utils",
        "//python/riegeli/bytes:python_writer",
        "//riegeli/base:assert",
        "//riegeli/base:chain",
        "//riegeli/base:types",
        "//riegeli/records:record_writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@rules_python//python/cc:current_py_cc_headers",
    ],
)

py_extension(
    name = "record_position",
    srcs = ["record_position.cc"],
    hdrs = ["record_position.h"],
    # record_position.cc has #define before #include to influence what the
    # included files provide.
    features = ["-use_header_modules"],
    deps = [
        "//python/riegeli/base:utils",
        "//riegeli/base:arithmetic",
        "//riegeli/base:constexpr",
        "//riegeli/base:types",
        "//riegeli/records:record_position",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@rules_python//python/cc:current_py_cc_headers",
    ],
)

py_library(
    name = "skipped_region",
    srcs = ["skipped_region.py"],
    srcs_version = "PY3",
)

proto_library(
    name = "records_metadata_proto",
    srcs = ["records_metadata.proto"],
    deps = ["@com_google_protobuf//:descriptor_proto"],
)

py_proto_library(
    name = "records_metadata_py_pb2",
    deps = [":records_metadata_proto"],
)
