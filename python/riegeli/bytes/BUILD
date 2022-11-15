package(default_visibility = ["//python/riegeli:__subpackages__"])

licenses(["notice"])

cc_library(
    name = "python_reader",
    srcs = ["python_reader.cc"],
    hdrs = ["python_reader.h"],
    # python_reader.cc has #define before #include to influence what the
    # included files provide.
    features = ["-use_header_modules"],
    deps = [
        "//python/riegeli/base:utils",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:no_destructor",
        "//riegeli/base:object",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
        "@local_config_python//:python_headers",
    ],
)

cc_library(
    name = "python_writer",
    srcs = ["python_writer.cc"],
    hdrs = ["python_writer.h"],
    # python_writer.cc has #define before #include to influence what the
    # included files provide.
    features = ["-use_header_modules"],
    deps = [
        "//python/riegeli/base:utils",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:no_destructor",
        "//riegeli/base:object",
        "//riegeli/base:types",
        "//riegeli/bytes:buffer_options",
        "//riegeli/bytes:buffered_writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
        "@local_config_python//:python_headers",
    ],
)
