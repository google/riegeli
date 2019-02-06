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

# Import HighwayHash (2019-01-29).
http_archive(
    name = "highwayhash",
    build_file = "//:highwayhash.BUILD",
    sha256 = "017d766fa5f130702eafcc2c9807582c67bdf99614f702d68edb510213428a23",
    strip_prefix = "highwayhash-e96ab3b409eb0cdec19c066aef1fd7e60e74eae3",
    urls = [
        "https://mirror.bazel.build/github.com/google/highwayhash/archive/e96ab3b409eb0cdec19c066aef1fd7e60e74eae3.zip",
        "https://github.com/google/highwayhash/archive/e96ab3b409eb0cdec19c066aef1fd7e60e74eae3.zip",
    ],
)

# Import Tensorflow (2019-02-03), Protobuf (2018-12-05), and configure
# @local_config_python.

http_archive(
    name = "org_tensorflow",
    sha256 = "55e25c577f337dfb02e9b4ba675b9a8d3d6d210bdad80f1674c2d3c0cf646a9e",
    strip_prefix = "tensorflow-226398bd70d6369dd97fa9cf4bee94af9a05f1d0",
    urls = [
        "https://mirror.bazel.build/github.com/tensorflow/tensorflow/archive/226398bd70d6369dd97fa9cf4bee94af9a05f1d0.zip",
        "https://github.com/tensorflow/tensorflow/archive/226398bd70d6369dd97fa9cf4bee94af9a05f1d0.zip",
    ],
)

http_archive(
    name = "io_bazel_rules_closure",
    sha256 = "013b820c64874dae78f3dbb561f1f6ee2b3367bbdc10f086534c0acddbd434e7",
    strip_prefix = "rules_closure-0.8.0",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_closure/archive/0.8.0.zip",
        "https://github.com/bazelbuild/rules_closure/archive/0.8.0.zip",  # 2018-06-23
    ],
)

load("@org_tensorflow//tensorflow:workspace.bzl", "tf_workspace")

tf_workspace("", "@org_tensorflow")
