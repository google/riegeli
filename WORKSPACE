workspace(name = "com_google_riegeli")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

# Import Abseil (2018-12-04).
http_archive(
    name = "com_google_absl",
    sha256 = "fe4d9e424dc25ee57695509cf6c5a7dd582a7ac1ca1efb92713fb439b3e8b1c6",
    strip_prefix = "abseil-cpp-20181200",
    urls = [
        "https://mirror.bazel.build/github.com/abseil/abseil-cpp/archive/20181200.zip",
        "https://github.com/abseil/abseil-cpp/archive/20181200.zip",
    ],
)

# Import Brotli (2018-10-23).
http_archive(
    name = "org_brotli",
    sha256 = "6e69be238ff61cef589a3fa88da11b649c7ff7a5932cb12d1e6251c8c2e17a2f",
    strip_prefix = "brotli-1.0.7",
    urls = [
        "https://mirror.bazel.build/github.com/google/brotli/archive/v1.0.7.zip",
        "https://github.com/google/brotli/archive/v1.0.7.zip",
    ],
)

# Import Zstd (2018-10-17).
http_archive(
    name = "net_zstd",
    build_file = "//:net_zstd.BUILD",
    sha256 = "00cf0539c61373f1450f5a09b2e3704e5cc6396404dffe248816732510d692ec",
    strip_prefix = "zstd-1.3.7/lib",
    urls = [
        "https://mirror.bazel.build/github.com/facebook/zstd/archive/v1.3.7.zip",
        "https://github.com/facebook/zstd/archive/v1.3.7.zip",
    ],
)

# Import zlib (2017-01-15).
http_archive(
    name = "zlib_archive",
    build_file = "//:zlib.BUILD",
    sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
    strip_prefix = "zlib-1.2.11",
    urls = [
        "http://mirror.bazel.build/zlib.net/fossils/zlib-1.2.11.tar.gz",
        "http://zlib.net/fossils/zlib-1.2.11.tar.gz",
    ],
)

# Import HighwayHash (2019-02-22).
http_archive(
    name = "highwayhash",
    build_file = "//:highwayhash.BUILD",
    sha256 = "cf891e024699c82aabce528a024adbe16e529f2b4e57f954455e0bf53efae585",
    strip_prefix = "highwayhash-276dd7b4b6d330e4734b756e97ccfb1b69cc2e12",
    urls = [
        "https://mirror.bazel.build/github.com/google/highwayhash/archive/276dd7b4b6d330e4734b756e97ccfb1b69cc2e12.zip",
        "https://github.com/google/highwayhash/archive/276dd7b4b6d330e4734b756e97ccfb1b69cc2e12.zip",
    ],
)

# Import TensorFlow (2019-03-05), Protobuf (@com_google_protobuf, 2018-12-05),
# Abseil-Python (@absl_py, 2019-01-11), and configure @local_config_python.

http_archive(
    name = "org_tensorflow",
    sha256 = "dfd0a8157a0f59bfb24a5b9b087d1a23dbd5155272b3a4fec0a747881b45f4e1",
    strip_prefix = "tensorflow-2.0.0-alpha0",
    urls = [
        "https://mirror.bazel.build/github.com/tensorflow/tensorflow/archive/v2.0.0-alpha0.zip",
        "https://github.com/tensorflow/tensorflow/archive/v2.0.0-alpha0.zip",
    ],
)

# Import Closure Rules for Bazel (2018-12-21).
http_archive(
    name = "io_bazel_rules_closure",
    sha256 = "43c9b882fa921923bcba764453f4058d102bece35a37c9f6383c713004aacff1",
    strip_prefix = "rules_closure-9889e2348259a5aad7e805547c1a0cf311cfcd91",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_closure/archive/9889e2348259a5aad7e805547c1a0cf311cfcd91.tar.gz",
        "https://github.com/bazelbuild/rules_closure/archive/9889e2348259a5aad7e805547c1a0cf311cfcd91.tar.gz",
    ],
)

load("@org_tensorflow//tensorflow:workspace.bzl", "tf_workspace")

tf_workspace("", "@org_tensorflow")
