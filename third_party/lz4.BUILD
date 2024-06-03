package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

exports_files(["LICENSE"])

cc_library(
    name = "lz4",
    srcs = [
        "lz4.c",
        "xxhash.c",
        "xxhash.h",
    ],
    hdrs = ["lz4.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "lz4_lz4c_include",
    hdrs = ["lz4.c"],
)

cc_library(
    name = "lz4_hc",
    srcs = ["lz4hc.c"],
    hdrs = ["lz4hc.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":lz4",
        ":lz4_lz4c_include",
    ],
)

cc_library(
    name = "lz4_frame",
    srcs = [
        "lz4frame.c",
        "lz4frame_static.h",
    ],
    hdrs = ["lz4frame.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":lz4",
        ":lz4_hc",
    ],
)
