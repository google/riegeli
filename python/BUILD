load("@rules_python//python:defs.bzl", "py_binary")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

# These dependencies are gathered in a py_binary, instead of directly in
# sh_binary data, so that bazel links __init__.py files to runfiles.
py_binary(
    name = "dummy_binary",
    srcs = ["dummy_binary.py"],
    srcs_version = "PY3",
    deps = [
        "//python/riegeli",
        "//python/riegeli/tensorflow:riegeli_dataset_ops",
    ],
)

sh_binary(
    name = "build_pip_package",
    srcs = ["build_pip_package.sh"],
    data = [
        "MANIFEST.in",
        "README.md",
        "setup.py",
        ":dummy_binary",
    ],
)
