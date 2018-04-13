workspace(name = "com_google_riegeli")

# Import Abseil (2018-03-27).
http_archive(
    name = "com_google_absl",
    strip_prefix = "abseil-cpp-70b5fa948d920ccca86d143057497132f63a44f3",
    urls = ["https://github.com/abseil/abseil-cpp/archive/70b5fa948d920ccca86d143057497132f63a44f3.zip"],
)

# Import CCTZ needed for absl/time (2018-02-06).
http_archive(
    name = "com_googlesource_code_cctz",
    urls = ["https://github.com/google/cctz/archive/v2.2.zip"],
    strip_prefix = "cctz-2.2",
)

# Import Brotli (2018-03-29).

http_archive(
    name = "org_brotli",
    strip_prefix = "brotli-1.0.4",
    urls = ["https://github.com/google/brotli/archive/v1.0.4.zip"],
)

git_repository(
    name = "io_bazel_rules_go",
    remote = "https://github.com/bazelbuild/rules_go.git",
    tag = "0.9.0",
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
    strip_prefix = "zstd-1.3.4/lib",
    urls = ["https://github.com/facebook/zstd/archive/v1.3.4.zip"],
)

# Import zlib (2017-01-15).
new_http_archive(
    name = "zlib_archive",
    build_file = "zlib.BUILD",
    sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
    strip_prefix = "zlib-1.2.11",
    urls = ["http://zlib.net/fossils/zlib-1.2.11.tar.gz"],
)

# Import HighwayHash (2018-02-09).
new_http_archive(
    name = "com_google_highwayhash",
    build_file = "com_google_highwayhash.BUILD",
    strip_prefix = "highwayhash-14dedecd1de87cb662f7a882ea1578d2384feb2f",
    urls = ["https://github.com/google/highwayhash/archive/14dedecd1de87cb662f7a882ea1578d2384feb2f.zip"],
)

# Import Tensorflow (2018-04-12) and Protobuf (2017-12-15).
http_archive(
    name = "org_tensorflow",
    strip_prefix = "tensorflow-1.8.0-rc0",
    urls = ["https://github.com/tensorflow/tensorflow/archive/v1.8.0-rc0.zip"],
)

load("@org_tensorflow//tensorflow:workspace.bzl", "tf_workspace")

tf_workspace("", "@org_tensorflow")
