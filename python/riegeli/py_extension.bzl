"""Supports writing Python modules in C++."""

load("@rules_python//python:defs.bzl", "py_library")
load("//third_party/bazel_rules/rules_cc/cc:cc_binary.bzl", "cc_binary")
load("//third_party/bazel_rules/rules_cc/cc:cc_library.bzl", "cc_library")

def py_extension(
        name = None,
        srcs = None,
        hdrs = None,
        data = None,
        features = None,
        visibility = None,
        deps = None):
    """Creates a Python module implemented in C++.

    Python modules can depend on a py_extension. Other py_extensions can depend
    on a generated C++ library named with "_cc" suffix.

    Args:
      name: Name for this target.
      srcs: C++ source files.
      hdrs: C++ header files, for other py_extensions which depend on this.
      data: Files needed at runtime. This may include Python libraries.
      features: Passed to cc_library.
      visibility: Controls which rules can depend on this.
      deps: Other C++ libraries that this library depends upon.
    """

    cc_library_name = name + "_cc"
    cc_binary_name = name + ".so"
    cc_library(
        name = cc_library_name,
        srcs = srcs,
        hdrs = hdrs,
        data = data,
        features = features,
        visibility = visibility,
        deps = deps,
        alwayslink = True,
    )
    cc_binary(
        name = cc_binary_name,
        linkshared = True,
        linkstatic = True,
        visibility = ["//visibility:private"],
        deps = [cc_library_name],
    )

    py_library(
        name = name,
        data = [cc_binary_name],
        visibility = visibility,
    )
