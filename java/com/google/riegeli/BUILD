load("@fmeum_rules_jni//jni:defs.bzl", "cc_jni_library", "java_jni_library", "jni_headers")

# Java interface for that will be implemented using JNI later.
java_library(
    name = "wrapper",
    srcs = [
        "RecordReader.java",
        "RecordWriter.java",
    ],
)

# Generate the native header.
jni_headers(
    name = "header",
    lib = ":wrapper",
)

cc_jni_library(
    name = "riegeli_jni",
    srcs = [
        "jni_record_reader.cc",
        "jni_record_writer.cc",
    ],
    visibility = [
    ],
    deps = [
        ":header",
        "//riegeli/bytes:fd_reader",
        "//riegeli/bytes:fd_writer",
        "//riegeli/records:record_reader",
        "//riegeli/records:record_writer",
    ],
)

java_jni_library(
    name = "loader",
    srcs = [
        "Loader.java",
    ],
    native_libs = [
        ":riegeli_jni",
    ],
    visibility = [
    ],
    deps = [
        ":wrapper",
    ],
)

java_library(
    name = "tests",
    testonly = 1,
    srcs = [
        "RecordReadWriteTest.java",
    ],
    deps = [
        ":loader",
        ":wrapper",
        "@junit_long",
        "@org_hamcrest_core",
    ],
)

java_test(
    name = "RecordReadWriteTest",
    runtime_deps = [
        ":tests",
    ],
)
