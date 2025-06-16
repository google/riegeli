# Bazel files

## Why patch file is needed

We need the patch file (`googleapis.modules.patch`) in this directory to manage
the googleapis dependency versioning that google_cloud_cpp uses. See:
https://github.com/googleapis/google-cloud-cpp/blob/v2.38.x/MODULE.bazel#L55 The
patching is done in riegeli's MODULE.bazel file.

We need this until https://github.com/googleapis/google-cloud-cpp/issues/14803
is solved.

## Managing the patch file

The `googleapis.modules.patch` file only needs to be changed when upgrading the
`google_cloud_cpp` dependency. Here are the steps to do so:

1.  Check
    https://github.com/googleapis/google-cloud-cpp/blob/\<version\>/MODULE.bazel
    to see if the patching is still needed.

1.  Make sure the name of the patch file is still googleapis.modules.patch by
    checking the `patches` field of the `archive_override` rule. If not, change
    the name in both `MODULES.bazel` of riegeli and the filename in `bazel/`

1.  If yes, then replace the `googleapis.modules.patch` file with the contents
    of
    https://github.com/googleapis/google-cloud-cpp/blob/\<version\>/bazel/googleapis.modules.patch
