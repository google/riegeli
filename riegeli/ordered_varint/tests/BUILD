package(default_visibility = ["//riegeli:__subpackages__"])

licenses(["notice"])

cc_test(
    name = "ordered_varint_test",
    size = "small",
    srcs = ["ordered_varint_test.cc"],
    deps = [
        "//riegeli/base",
        "//riegeli/bytes:string_reader",
        "//riegeli/bytes:string_writer",
        "//riegeli/bytes/tests:test_utils",
        "//riegeli/ordered_varint:ordered_varint_reading",
        "//riegeli/ordered_varint:ordered_varint_writing",
        "//testing/base/public:gunit_main",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_test(
    name = "ordered_varint_benchmark",
    srcs = ["ordered_varint_benchmark.cc"],
    deps = [
        "//riegeli/base",
        "//riegeli/bytes:string_reader",
        "//riegeli/bytes:string_writer",
        "//riegeli/ordered_varint:ordered_varint_reading",
        "//riegeli/ordered_varint:ordered_varint_writing",
        "//riegeli/varint:varint_reading",
        "//riegeli/varint:varint_writing",
        "//testing/base/public:gunit_main_no_heapcheck",
        "//util/coding:prefixvarint",
        "@com_google_absl//absl/types:optional",
    ],
)
