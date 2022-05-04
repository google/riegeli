package(default_visibility = ["//visibility:public"])

licenses(["notice"])

exports_files(["LICENSE"])

cc_library(
    name = "lz4",
    srcs = [
        "lz4.c",
        "lz4frame.c",
        "lz4hc.c",
    ],
    hdrs = [
        "lz4.h",
        "lz4frame.h",
        "lz4hc.h",
    ],
    local_defines = [
        # Inline XXH for better performance and to avoid exposing symbols.
        "XXH_INLINE_ALL",
    ],
    deps = [":lz4_internal"],
)

# *.c files that are #included.
cc_library(
    name = "lz4_internal",
    hdrs = [
        "lz4.c",
        "xxhash.c",
        "xxhash.h",
    ],
    visibility = ["//visibility:private"],
)
