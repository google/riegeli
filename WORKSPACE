workspace(name = "com_google_riegeli")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//python/riegeli:python_configure.bzl", "python_configure")
load("//tf_dependency:tf_configure.bzl", "tf_configure")

http_archive(
    name = "com_google_absl",
    sha256 = "6a6eee57ce3375e26d49c542d191ceb90822984d5b587f0ea8e0d0c77016b02b",
    strip_prefix = "abseil-cpp-20200225.3",
    urls = [
        "https://mirror.bazel.build/github.com/abseil/abseil-cpp/archive/20200225.3.zip",
        "https://github.com/abseil/abseil-cpp/archive/20200225.3.zip",  # 2020-10-09
    ],
)

http_archive(
    name = "org_brotli",
    sha256 = "fec5a1d26f3dd102c542548aaa704f655fecec3622a24ec6e97768dcb3c235ff",
    strip_prefix = "brotli-68f1b90ad0d204907beb58304d0bd06391001a4d",
    urls = [
        "https://mirror.bazel.build/github.com/google/brotli/archive/68f1b90ad0d204907beb58304d0bd06391001a4d.zip",
        "https://github.com/google/brotli/archive/68f1b90ad0d204907beb58304d0bd06391001a4d.zip",  # 2021-08-18
    ],
)

http_archive(
    name = "net_zstd",
    build_file = "//third_party:net_zstd.BUILD",
    sha256 = "b6c537b53356a3af3ca3e621457751fa9a6ba96daf3aebb3526ae0f610863532",
    strip_prefix = "zstd-1.4.5/lib",
    urls = [
        "https://mirror.bazel.build/github.com/facebook/zstd/archive/v1.4.5.zip",
        "https://github.com/facebook/zstd/archive/v1.4.5.zip",  # 2020-05-22
    ],
)

http_archive(
    name = "snappy",
    build_file = "//third_party:snappy.BUILD",
    sha256 = "38b4aabf88eb480131ed45bfb89c19ca3e2a62daeb081bdf001cfb17ec4cd303",
    strip_prefix = "snappy-1.1.8",
    urls = [
        "https://mirror.bazel.build/github.com/google/snappy/archive/1.1.8.zip",
        "https://github.com/google/snappy/archive/1.1.8.zip",  # 2020-01-14
    ],
)

http_archive(
    name = "crc32c",
    build_file = "//third_party:crc32.BUILD",
    sha256 = "338f1d9d95753dc3cdd882dfb6e176bbb4b18353c29c411ebcb7b890f361722e",
    strip_prefix = "crc32c-1.1.0",
    urls = [
        "https://mirror.bazel.build/github.com/google/crc32c/archive/1.1.0.zip",
        "https://github.com/google/crc32c/archive/1.1.0.zip",  # 2019-05-24
    ],
)

http_archive(
    name = "zlib",
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

http_archive(
    name = "com_google_protobuf",
    sha256 = "25f1292d4ea6666f460a2a30038eef121e6c3937ae0f61d610611dfb14b0bd32",
    strip_prefix = "protobuf-3.19.1",
    urls = [
        "http://mirror.bazel.build/github.com/protocolbuffers/protobuf/archive/v3.19.1.zip",
        "https://github.com/protocolbuffers/protobuf/archive/v3.19.1.zip",  # 2021-10-28
    ],
)

http_archive(
    name = "six_archive",
    build_file = "//third_party:six.BUILD",
    sha256 = "d16a0141ec1a18405cd4ce8b4613101da75da0e9a7aec5bdd4fa804d0e0eba73",
    strip_prefix = "six-1.12.0",
    urls = [
        "http://mirror.bazel.build/pypi.python.org/packages/source/s/six/six-1.12.0.tar.gz",
        "https://pypi.python.org/packages/source/s/six/six-1.12.0.tar.gz",  # 2018-12-10
    ],
)

http_archive(
    name = "absl_py",
    sha256 = "3d0f39e0920379ff1393de04b573bca3484d82a5f8b939e9e83b20b6106c9bbe",
    strip_prefix = "abseil-py-pypi-v0.7.1",
    urls = [
        "http://mirror.bazel.build/github.com/abseil/abseil-py/archive/pypi-v0.7.1.tar.gz",
        "https://github.com/abseil/abseil-py/archive/pypi-v0.7.1.tar.gz",  # 2019-03-12
    ],
)

# Needed by @com_google_absl and soon other packages:
# https://github.com/abseil/abseil-cpp/commit/36910d3d7e9fccadd6603f232d0c4f54dcd47c7e
http_archive(
    name = "rules_cc",
    sha256 = "67412176974bfce3f4cf8bdaff39784a72ed709fc58def599d1f68710b58d68b",
    strip_prefix = "rules_cc-b7fe9697c0c76ab2fd431a891dbb9a6a32ed7c3e",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_cc/archive/b7fe9697c0c76ab2fd431a891dbb9a6a32ed7c3e.zip",
        "https://github.com/bazelbuild/rules_cc/archive/b7fe9697c0c76ab2fd431a891dbb9a6a32ed7c3e.zip",
    ],
)

http_archive(
    name = "rules_python",
    url = "https://github.com/bazelbuild/rules_python/releases/download/0.5.0/rules_python-0.5.0.tar.gz",
    sha256 = "cd6730ed53a002c56ce4e2f396ba3b3be262fd7cb68339f0377a45e8227fe332",
)

# Needed by @com_google_protobuf.
http_archive(
    name = "bazel_skylib",
    sha256 = "28f81e36692e1d87823623a99966b2daf85af3fdc1b40f98e37bd5294f3dd185",
    strip_prefix = "bazel-skylib-1.0.3",
    urls = [
        "http://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/archive/1.0.3.zip",
        "https://github.com/bazelbuild/bazel-skylib/archive/1.0.3.zip",  # 2020-08-27
    ],
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
