package(default_visibility = ["//visibility:public"])

licenses(["notice"])  # BSD

cc_library(
    name = "zstdlib",
    srcs = glob([
        "common/*.c",
        "common/*.h",
        "compress/*.c",
        "compress/*.h",
        "decompress/*.c",
    ]),
    hdrs = ["zstd.h"],
    includes = [
        ".",
        "common",
    ],
)
