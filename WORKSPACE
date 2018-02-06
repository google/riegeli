workspace(name = "com_google_riegeli")

# Import Brotli (2017-11-28).
http_archive(
    name = "org_brotli",
    strip_prefix = "brotli-1.0.2",
    urls = ["https://github.com/google/brotli/archive/v1.0.2.zip"],
)

git_repository(
    name = "io_bazel_rules_go",
    remote = "https://github.com/bazelbuild/rules_go.git",
    tag = "0.5.5",
)

http_archive(
    name = "io_bazel_rules_closure",
    sha256 = "25f5399f18d8bf9ce435f85c6bbf671ec4820bc4396b3022cc5dc4bc66303609",
    strip_prefix = "rules_closure-0.4.2",
    urls = [
        "http://mirror.bazel.build/github.com/bazelbuild/rules_closure/archive/0.4.2.tar.gz",
        "https://github.com/bazelbuild/rules_closure/archive/0.4.2.tar.gz",
    ],
)

# Import Zstd (2017-12-21).
new_http_archive(
    name = "net_zstd",
    build_file = "net_zstd.BUILD",
    strip_prefix = "zstd-1.3.3/lib",
    urls = ["https://github.com/facebook/zstd/archive/v1.3.3.zip"],
)

# Import Protobuf (2018-01-05).
http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-3.5.1.1",
    urls = ["https://github.com/google/protobuf/archive/v3.5.1.1.zip"],
)

# Import HighwayHash (2018-01-03).
new_http_archive(
    name = "com_google_highwayhash",
    build_file = "com_google_highwayhash.BUILD",
    strip_prefix = "highwayhash-eeea4463df1639c7ce271a1d0fdfa8ae5e81a49f",
    urls = ["https://github.com/google/highwayhash/archive/eeea4463df1639c7ce271a1d0fdfa8ae5e81a49f.zip"],
)
