package(
    default_visibility = ["//visibility:public"],
    features = ["header_modules"],
)

licenses(["notice"])

cc_library(
    name = "digest_converter",
    hdrs = ["digest_converter.h"],
    deps = [
        "//riegeli/endian:endian_writing",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/numeric:int128",
    ],
)

cc_library(
    name = "digester_handle",
    srcs = ["digester_handle.cc"],
    hdrs = ["digester_handle.h"],
    deps = [
        ":digest_converter",
        "//riegeli/base:arithmetic",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:type_traits",
        "//riegeli/base:types",
        "//riegeli/base:zeros",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/numeric:int128",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "wrapping_digester",
    hdrs = ["wrapping_digester.h"],
    deps = [
        ":digest_converter",
        ":digester_handle",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:types",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
    ],
)

cc_library(
    name = "digesting_reader",
    srcs = ["digesting_reader.cc"],
    hdrs = ["digesting_reader.h"],
    deps = [
        ":digest_converter",
        ":digester_handle",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffering",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:status",
        "//riegeli/base:types",
        "//riegeli/bytes:reader",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "digesting_writer",
    srcs = ["digesting_writer.cc"],
    hdrs = ["digesting_writer.h"],
    deps = [
        ":digest_converter",
        ":digester_handle",
        "//riegeli/base:arithmetic",
        "//riegeli/base:assert",
        "//riegeli/base:buffering",
        "//riegeli/base:chain",
        "//riegeli/base:dependency",
        "//riegeli/base:initializer",
        "//riegeli/base:object",
        "//riegeli/base:type_traits",
        "//riegeli/base:types",
        "//riegeli/bytes:null_writer",
        "//riegeli/bytes:reader",
        "//riegeli/bytes:writer",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/meta:type_traits",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/types:optional",
    ],
)

cc_library(
    name = "crc32c_digester",
    hdrs = ["crc32c_digester.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "@com_google_absl//absl/strings",
        "@crc32c",
    ],
)

cc_library(
    name = "crc32_digester",
    srcs = ["crc32_digester.cc"],
    hdrs = ["crc32_digester.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@zlib",
    ],
)

cc_library(
    name = "adler32_digester",
    srcs = ["adler32_digester.cc"],
    hdrs = ["adler32_digester.h"],
    deps = [
        "//riegeli/base:arithmetic",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@zlib",
    ],
)

cc_library(
    name = "openssl_digester",
    hdrs = ["openssl_digester.h"],
    deps = ["@com_google_absl//absl/strings"],
)

# Warning: MD5 as a cryptographic hash function is broken.
# Use this only if a preexisting format has already decided to use MD5.
cc_library(
    name = "md5_digester",
    hdrs = ["md5_digester.h"],
    deps = [
        ":openssl_digester",
        "@boringssl//:crypto",
    ],
)

# Warning: SHA-1 as a cryptographic hash function is broken.
# Use this only if a preexisting format has already decided to use SHA-1.
cc_library(
    name = "sha1_digester",
    hdrs = ["sha1_digester.h"],
    deps = [
        ":openssl_digester",
        "@boringssl//:crypto",
    ],
)

cc_library(
    name = "sha256_digester",
    hdrs = ["sha256_digester.h"],
    deps = [
        ":openssl_digester",
        "@boringssl//:crypto",
    ],
)

cc_library(
    name = "sha512_digester",
    hdrs = ["sha512_digester.h"],
    deps = [
        ":openssl_digester",
        "@boringssl//:crypto",
    ],
)

cc_library(
    name = "sha512_256_digester",
    hdrs = ["sha512_256_digester.h"],
    deps = [
        ":openssl_digester",
        "@boringssl//:crypto",
    ],
)
