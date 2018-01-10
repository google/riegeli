workspace(name = "com_google_riegeli")

# Import Brotli.
http_archive(
    name = "org_brotli",
    strip_prefix = "brotli-1.0.1",
    urls = ["https://github.com/google/brotli/archive/v1.0.1.zip"],
)

git_repository(
    name = "io_bazel_rules_go",
    remote = "https://github.com/bazelbuild/rules_go.git",
    tag = "0.4.4",
)

http_archive(
    name = "io_bazel_rules_closure",
    strip_prefix = "rules_closure-0.4.1",
    sha256 = "ba5e2e10cdc4027702f96e9bdc536c6595decafa94847d08ae28c6cb48225124",
    url = "http://mirror.bazel.build/github.com/bazelbuild/rules_closure/archive/0.4.1.tar.gz",
)

# Import Zstd.
new_http_archive(
    name = "net_zstd",
    build_file = "net_zstd.BUILD",
    strip_prefix = "zstd-1.3.2/lib",
    urls = ["https://github.com/facebook/zstd/archive/v1.3.2.zip"],
)

# Import Protobuf.
http_archive(
    name = "com_google_protobuf",
    strip_prefix = "protobuf-3.4.1",
    urls = ["https://github.com/google/protobuf/archive/v3.4.1.zip"],
)

# Import HighwayHash.
new_http_archive(
    name = "com_google_highwayhash",
    build_file = "com_google_highwayhash.BUILD",
    strip_prefix = "highwayhash-be5491d449e9cc411a1b4b80a128f5684d50eb4c",
    # This is a commit from Sep 12, 2017.
    urls = ["https://github.com/google/highwayhash/archive/be5491d449e9cc411a1b4b80a128f5684d50eb4c.zip"],
)
