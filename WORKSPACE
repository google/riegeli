workspace(name = "com_google_riegeli")

# Import Abseil (2018-06-28).
http_archive(
    name = "com_google_absl",
    sha256 = "6dda700b67b3d8eafd31e419c888cc65c1525219eace9396c1ae884b08ed8caa",
    strip_prefix = "abseil-cpp-ba8d6cf07766263723e86736f20a51c1c9c67b19",
    urls = [
        "https://mirror.bazel.build/github.com/abseil/abseil-cpp/archive/ba8d6cf07766263723e86736f20a51c1c9c67b19.zip",
        "https://github.com/abseil/abseil-cpp/archive/ba8d6cf07766263723e86736f20a51c1c9c67b19.zip",
    ],
)

# Import Brotli (2018-03-29).

http_archive(
    name = "org_brotli",
    sha256 = "221811f83a48307fcb6d5515e9590ddfbe2d74970393721e55cbafd67499402d",
    strip_prefix = "brotli-1.0.4",
    urls = [
        "https://mirror.bazel.build/github.com/google/brotli/archive/v1.0.4.zip",
        "https://github.com/google/brotli/archive/v1.0.4.zip",
    ],
)

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "cb9d2fe03a5c2097f3cce8e4329e235158208414a65a824c8bcc42785b34036d",
    strip_prefix = "rules_go-0.9.0",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/archive/0.9.0.zip",
        "https://github.com/bazelbuild/rules_go/archive/0.9.0.zip",
    ],
)

http_archive(
    name = "io_bazel_rules_closure",
    sha256 = "6691c58a2cd30a86776dd9bb34898b041e37136f2dc7e24cadaeaf599c95c657",
    strip_prefix = "rules_closure-08039ba8ca59f64248bb3b6ae016460fe9c9914f",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_closure/archive/08039ba8ca59f64248bb3b6ae016460fe9c9914f.tar.gz",
        "https://github.com/bazelbuild/rules_closure/archive/08039ba8ca59f64248bb3b6ae016460fe9c9914f.tar.gz",  # 2018-01-16
    ],
)

# Import Zstd (2018-03-27).
new_http_archive(
    name = "net_zstd",
    build_file = "net_zstd.BUILD",
    sha256 = "76f3e6cadd9e1714cad7b550e53b82d4a5a5f9e08fbc0b3eb54abd0d0ebeb42d",
    strip_prefix = "zstd-1.3.4/lib",
    urls = [
        "https://mirror.bazel.build/github.com/facebook/zstd/archive/v1.3.4.zip",
        "https://github.com/facebook/zstd/archive/v1.3.4.zip",
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

# Import HighwayHash (2018-02-09).
new_http_archive(
    name = "com_google_highwayhash",
    build_file = "com_google_highwayhash.BUILD",
    sha256 = "2cb637aad1befdfdb9c17a7a417b05e8a7033207fdd42fba2ffdf0704dc62004",
    strip_prefix = "highwayhash-14dedecd1de87cb662f7a882ea1578d2384feb2f",
    urls = [
        "https://mirror.bazel.build/github.com/google/highwayhash/archive/14dedecd1de87cb662f7a882ea1578d2384feb2f.zip",
        "https://github.com/google/highwayhash/archive/14dedecd1de87cb662f7a882ea1578d2384feb2f.zip",
    ],
)

# Import Tensorflow (2018-04-12) and Protobuf (2017-12-15).
http_archive(
    name = "org_tensorflow",
    sha256 = "6b4765e32c275000bad9a0c4729b380927fd01324baa98c6ed11b2840f130f99",
    strip_prefix = "tensorflow-1.8.0-rc0",
    urls = [
        "https://mirror.bazel.build/github.com/tensorflow/tensorflow/archive/v1.8.0-rc0.zip",
        "https://github.com/tensorflow/tensorflow/archive/v1.8.0-rc0.zip",
    ],
)

load("@org_tensorflow//tensorflow:workspace.bzl", "tf_workspace")

tf_workspace("", "@org_tensorflow")
