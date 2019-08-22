workspace(name = "com_google_riegeli")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "com_google_absl",
    sha256 = "0b62fc2d00c2b2bc3761a892a17ac3b8af3578bd28535d90b4c914b0a7460d4e",
    strip_prefix = "abseil-cpp-20190808",
    urls = [
        "https://mirror.bazel.build/github.com/abseil/abseil-cpp/archive/20190808.zip",
        "https://github.com/abseil/abseil-cpp/archive/20190808.zip",  # 2019-08-08
    ],
)

http_archive(
    name = "org_brotli",
    sha256 = "6e69be238ff61cef589a3fa88da11b649c7ff7a5932cb12d1e6251c8c2e17a2f",
    strip_prefix = "brotli-1.0.7",
    urls = [
        "https://mirror.bazel.build/github.com/google/brotli/archive/v1.0.7.zip",
        "https://github.com/google/brotli/archive/v1.0.7.zip",  # 2018-10-23
    ],
)

http_archive(
    name = "net_zstd",
    build_file = "//third_party:net_zstd.BUILD",
    sha256 = "26fcd509af38789185f250c16caaf45c669f2c484533ad9c46eeceb204c81435",
    strip_prefix = "zstd-1.4.3/lib",
    urls = [
        "https://mirror.bazel.build/github.com/facebook/zstd/archive/v1.4.3.zip",
        "https://github.com/facebook/zstd/archive/v1.4.3.zip",  # 2019-08-19
    ],
)

http_archive(
    name = "zlib_archive",
    build_file = "//third_party:zlib.BUILD",
    sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
    strip_prefix = "zlib-1.2.11",
    urls = [
        "http://mirror.bazel.build/zlib.net/fossils/zlib-1.2.11.tar.gz",
        "http://zlib.net/fossils/zlib-1.2.11.tar.gz",  # 2017-01-15
    ],
)

http_archive(
    name = "highwayhash",
    build_file = "//third_party:highwayhash.BUILD",
    sha256 = "cf891e024699c82aabce528a024adbe16e529f2b4e57f954455e0bf53efae585",
    strip_prefix = "highwayhash-276dd7b4b6d330e4734b756e97ccfb1b69cc2e12",
    urls = [
        "https://mirror.bazel.build/github.com/google/highwayhash/archive/276dd7b4b6d330e4734b756e97ccfb1b69cc2e12.zip",
        "https://github.com/google/highwayhash/archive/276dd7b4b6d330e4734b756e97ccfb1b69cc2e12.zip",  # 2019-02-22
    ],
)

# This includes @com_google_protobuf, @six_archive, @absl_py,
# and @local_config_python.
http_archive(
    name = "org_tensorflow",
    sha256 = "4e574181721b366cc0b807a8683a57122465760a24c6c8422228997afa6178d0",
    strip_prefix = "tensorflow-2.0.0-beta1",
    urls = [
        "https://mirror.bazel.build/github.com/tensorflow/tensorflow/archive/v2.0.0-beta1.zip",
        "https://github.com/tensorflow/tensorflow/archive/v2.0.0-beta1.zip",  # 2019-06-13
    ],
)

# Needed for @org_tensorflow.
http_archive(
    name = "io_bazel_rules_closure",
    sha256 = "e0a111000aeed2051f29fcc7a3f83be3ad8c6c93c186e64beb1ad313f0c7f9f9",
    strip_prefix = "rules_closure-cf1e44edb908e9616030cc83d085989b8e6cd6df",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_closure/archive/cf1e44edb908e9616030cc83d085989b8e6cd6df.tar.gz",
        "https://github.com/bazelbuild/rules_closure/archive/cf1e44edb908e9616030cc83d085989b8e6cd6df.tar.gz",  # 2019-04-04
    ],
)

# Needed for @com_google_protobuf.
# TODO: @com_google_protobuf >= 3.8.0 will provide protobuf_deps()
# in @com_google_protobuf//:protobuf_deps.bzl.
http_archive(
    name = "bazel_skylib",
    sha256 = "2ef429f5d7ce7111263289644d233707dba35e39696377ebab8b0bc701f7818e",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/0.8.0/bazel-skylib.0.8.0.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/0.8.0/bazel-skylib.0.8.0.tar.gz",  # 2019-03-20
    ],
)

load("@org_tensorflow//tensorflow:workspace.bzl", "tf_workspace")

tf_workspace("", "@org_tensorflow")
