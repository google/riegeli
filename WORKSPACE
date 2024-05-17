workspace(name = "com_google_riegeli")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//python/riegeli:python_configure.bzl", "python_configure")
load("//tf_dependency:tf_configure.bzl", "tf_configure")

http_archive(
    name = "com_google_absl",
    sha256 = "54707f411cb62a26a776dad5fd60829098c181700edcd022ea5c2ca49e9b7ef1",
    strip_prefix = "abseil-cpp-20220623.1",
    urls = ["https://github.com/abseil/abseil-cpp/archive/refs/tags/20220623.1.zip"],  # 2022-06-23
)

http_archive(
  name = "com_google_googletest",
  sha256 = "5cf189eb6847b4f8fc603a3ffff3b0771c08eec7dd4bd961bfd45477dd13eb73",
  strip_prefix = "googletest-609281088cfefc76f9d0ce82e1ff6c30cc3591e5",
  urls = ["https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip"],
)

http_archive(
    name = "org_brotli",
    sha256 = "84a9a68ada813a59db94d83ea10c54155f1d34399baf377842ff3ab9b3b3256e",
    strip_prefix = "brotli-3914999fcc1fda92e750ef9190aa6db9bf7bdb07",
    urls = ["https://github.com/google/brotli/archive/3914999fcc1fda92e750ef9190aa6db9bf7bdb07.zip"],  # 2022-11-17
)

http_archive(
    name = "net_zstd",
    build_file = "//third_party:net_zstd.BUILD",
    sha256 = "b6c537b53356a3af3ca3e621457751fa9a6ba96daf3aebb3526ae0f610863532",
    strip_prefix = "zstd-1.4.5/lib",
    urls = ["https://github.com/facebook/zstd/archive/v1.4.5.zip"],  # 2020-05-22
)

http_archive(
    name = "lz4",
    build_file = "//third_party:lz4.BUILD",
    sha256 = "4ec935d99aa4950eadfefbd49c9fad863185ac24c32001162c44a683ef61b580",
    strip_prefix = "lz4-1.9.3/lib",
    urls = ["https://github.com/lz4/lz4/archive/refs/tags/v1.9.3.zip"],  # 2020-11-16
)

http_archive(
    name = "snappy",
    build_file = "//third_party:snappy.BUILD",
    sha256 = "7ee7540b23ae04df961af24309a55484e7016106e979f83323536a1322cedf1b",
    strip_prefix = "snappy-1.2.0",
    urls = ["https://github.com/google/snappy/archive/1.2.0.zip"],  # 2024-04-05
)

http_archive(
    name = "crc32c",
    build_file = "//third_party:crc32.BUILD",
    sha256 = "338f1d9d95753dc3cdd882dfb6e176bbb4b18353c29c411ebcb7b890f361722e",
    strip_prefix = "crc32c-1.1.0",
    urls = ["https://github.com/google/crc32c/archive/1.1.0.zip"],  # 2019-05-24
)

http_archive(
    name = "zlib",
    build_file = "//third_party:zlib.BUILD",
    sha256 = "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1",
    strip_prefix = "zlib-1.2.11",
    urls = ["http://zlib.net/fossils/zlib-1.2.11.tar.gz"],  # 2017-01-15
)

http_archive(
    name = "bzip2",
    build_file = "//third_party:bzip2.BUILD",
    sha256 = "ab5a03176ee106d3f0fa90e381da478ddae405918153cca248e682cd0c4a2269",
    strip_prefix = "bzip2-1.0.8",
    urls = ["https://sourceware.org/pub/bzip2/bzip2-1.0.8.tar.gz"],  # 2019-07-13
)

http_archive(
    name = "xz",
    build_file = "//third_party:xz.BUILD",
    sha256 = "e4b0f81582efa155ccf27bb88275254a429d44968e488fc94b806f2a61cd3e22",
    strip_prefix = "xz-5.4.1",
    urls = ["https://tukaani.org/xz/xz-5.4.1.tar.gz"],  # 2023-01-11
)

http_archive(
    name = "liburing",
    build_file = "//third_party:liburing.BUILD",
    sha256 = "ca069ecc4aa1baf1031bd772e4e97f7e26dfb6bb733d79f70159589b22ab4dc0",
    strip_prefix = "liburing-liburing-2.0",
    urls = [
        "https://github.com/axboe/liburing/archive/refs/tags/liburing-2.0.tar.gz",
    ],
)

http_archive(
    name = "highwayhash",
    build_file = "//third_party:highwayhash.BUILD",
    sha256 = "cf891e024699c82aabce528a024adbe16e529f2b4e57f954455e0bf53efae585",
    strip_prefix = "highwayhash-276dd7b4b6d330e4734b756e97ccfb1b69cc2e12",
    urls = ["https://github.com/google/highwayhash/archive/276dd7b4b6d330e4734b756e97ccfb1b69cc2e12.zip"],  # 2019-02-22
)

http_archive(
    name = "boringssl",
    sha256 = "10a04bde6b953a94e8006c1122b11b209bb33575be40e5370d0da88888042819",
    strip_prefix = "boringssl-1b6fe250a3b8b66aa4990a6ec59f20f5bf52df5d",
    urls = ["https://github.com/google/boringssl/archive/1b6fe250a3b8b66aa4990a6ec59f20f5bf52df5d.zip"],  # 2024-02-17 (main-with-bazel)
)

http_archive(
    name = "com_google_protobuf",
    patch_args = ["-p1"],
    patches = ["//third_party:protobuf.patch"],
    sha256 = "cfcba2df10feec52a84208693937c17a4b5df7775e1635c1e3baffc487b24c9b",
    strip_prefix = "protobuf-3.9.2",
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v3.9.2.zip"],  # 2019-09-20
)

http_archive(
    name = "six_archive",
    build_file = "//third_party:six.BUILD",
    sha256 = "d16a0141ec1a18405cd4ce8b4613101da75da0e9a7aec5bdd4fa804d0e0eba73",
    strip_prefix = "six-1.12.0",
    urls = ["https://pypi.python.org/packages/source/s/six/six-1.12.0.tar.gz"],  # 2018-12-10
)

http_archive(
    name = "absl_py",
    sha256 = "39baf348f9358346dde9b021b54f7d2b02a416246f147f2329f59515b6b10c70",
    strip_prefix = "abseil-py-pypi-v0.15.0",
    urls = ["https://github.com/abseil/abseil-py/archive/refs/tags/pypi-v0.15.0.zip"],  # 2021-10-19
)

# Needed by @com_google_absl and soon other packages:
# https://github.com/abseil/abseil-cpp/commit/36910d3d7e9fccadd6603f232d0c4f54dcd47c7e
http_archive(
    name = "rules_cc",
    sha256 = "67412176974bfce3f4cf8bdaff39784a72ed709fc58def599d1f68710b58d68b",
    strip_prefix = "rules_cc-b7fe9697c0c76ab2fd431a891dbb9a6a32ed7c3e",
    urls = ["https://github.com/bazelbuild/rules_cc/archive/b7fe9697c0c76ab2fd431a891dbb9a6a32ed7c3e.zip"],
)

# Needed by @com_google_protobuf.
http_archive(
    name = "bazel_skylib",
    sha256 = "28f81e36692e1d87823623a99966b2daf85af3fdc1b40f98e37bd5294f3dd185",
    strip_prefix = "bazel-skylib-1.0.3",
    urls = ["https://github.com/bazelbuild/bazel-skylib/archive/1.0.3.zip"],  # 2020-08-27
)

# Needed by @com_google_protobuf.
bind(
    name = "python_headers",
    actual = "@local_config_python//:python_headers",
)

# Needed by @com_google_protobuf.
# TODO: @com_google_protobuf >= 3.10.0 will not need this
# (it will use @six//:six instead of //external:six). Use this:
# load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")
# protobuf_deps()
bind(
    name = "six",
    actual = "@six_archive//:six",
)

python_configure(name = "local_config_python")

register_toolchains("@local_config_python//:toolchain")

tf_configure(name = "local_config_tf")
