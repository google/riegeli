workspace(name = "com_google_riegeli")

# Import Abseil (2018-09-27).
http_archive(
    name = "com_google_absl",
    sha256 = "57b7817ad91856399a77300349eda39d2edae2c3c037d453fee0a16b85e92a4d",
    strip_prefix = "abseil-cpp-48cd2c3f351ff188bc85684b84a91b6e6d17d896",
    urls = [
        "https://mirror.bazel.build/github.com/abseil/abseil-cpp/archive/48cd2c3f351ff188bc85684b84a91b6e6d17d896.zip",
        "https://github.com/abseil/abseil-cpp/archive/48cd2c3f351ff188bc85684b84a91b6e6d17d896.zip",
    ],
)

# Import Brotli (2018-09-13).
http_archive(
    name = "org_brotli",
    sha256 = "eec7b86bff510480b0c7450ace937077b8cc3c1ea38f4517b7f17572fcbf0430",
    strip_prefix = "brotli-1.0.6",
    urls = [
        "https://mirror.bazel.build/github.com/google/brotli/archive/v1.0.6.zip",
        "https://github.com/google/brotli/archive/v1.0.6.zip",
    ],
)

# Import Zstd (2018-06-28).
new_http_archive(
    name = "net_zstd",
    build_file = "net_zstd.BUILD",
    sha256 = "81a60ad270909406d2b01cd8f99d0506f6f05409ed35649b54d8f2b610be41d3",
    strip_prefix = "zstd-1.3.5/lib",
    urls = [
        "https://mirror.bazel.build/github.com/facebook/zstd/archive/v1.3.5.zip",
        "https://github.com/facebook/zstd/archive/v1.3.5.zip",
    ],
)

# Import zlib (2017-01-15).
new_http_archive(
    name = "zlib_archive",
    build_file = "zlib.BUILD",
    sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
    strip_prefix = "zlib-1.2.11",
    urls = [
        "http://mirror.bazel.build/zlib.net/fossils/zlib-1.2.11.tar.gz",
        "http://zlib.net/fossils/zlib-1.2.11.tar.gz",
    ],
)

# Import HighwayHash (2018-06-26).
new_http_archive(
    name = "com_google_highwayhash",
    build_file = "com_google_highwayhash.BUILD",
    sha256 = "6298342c5c25fe2c6403afd02e7e6dc65edd15290af664fa6d410f600784a360",
    strip_prefix = "highwayhash-9099074416ebc926c9e5e6f5143db92ebd9b4c03",
    urls = [
        "https://mirror.bazel.build/github.com/google/highwayhash/archive/9099074416ebc926c9e5e6f5143db92ebd9b4c03.zip",
        "https://github.com/google/highwayhash/archive/9099074416ebc926c9e5e6f5143db92ebd9b4c03.zip",
    ],
)

# Import Tensorflow (2018-09-25) and Protobuf (2018-06-06).

http_archive(
    name = "org_tensorflow",
    sha256 = "dfee0f57366a6fab16a103d3a6d190c327f01f9a12651e45a128051eaf612f20",
    strip_prefix = "tensorflow-1.11.0",
    urls = [
        "https://mirror.bazel.build/github.com/tensorflow/tensorflow/archive/v1.11.0.zip",
        "https://github.com/tensorflow/tensorflow/archive/v1.11.0.zip",
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
