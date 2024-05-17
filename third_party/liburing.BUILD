licenses(["notice"])

genrule(
    name = "liburingconfigure",
    tools = [
        "configure",
    ],
    outs = [
        "config-host.h",
        "config-host.mak",
        "config.log",
        "src/include/liburing/compat.h",
    ],
    cmd = "tempdir=$(@D)/tmp.XXXXX; rm -rf $$tempdir; mkdir -p $$tempdir; cp -r external/liburing/* $$tempdir/; pushd $$tempdir; ./configure; popd; cp $$tempdir/config-host.h $$tempdir/config-host.mak $$tempdir/config.log $(@D); cp $$tempdir/src/include/liburing/compat.h $(@D)/src/include/liburing; rm -rf $$tempdir;",
    local = 1,
)

genrule(
    name = "foo",
    outs = ["foo.h"],
    cmd = "ls -al",
)

cc_library(
    name = "liburing",
    visibility = [
        "//visibility:public"
    ],
    hdrs = glob([
        "src/*.h",
        "src/include/*.h",
        "src/include/liburing/*.h",
    ]) + [":liburingconfigure"],
    srcs = glob([
        "src/*.c",
    ]),
    includes = ["src/include"]
)