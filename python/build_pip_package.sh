#!/bin/bash
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds a pip package for riegeli.
#
# Usage (where DEST is a where to write the output, e.g. ~/riegeli-dist):
# $ bazel build -c opt python:build_pip_package
# $ bazel-bin/python/build_pip_package --dest DEST --sdist --bdist

set -e

function is_absolute {
  [[ "$1" = /* ]] || [[ "$1" =~ ^[a-zA-Z]:[/\\].* ]]
}

function real_path() {
  if is_absolute "$1"; then
    printf "%s" "$1"
  else
    printf "%s/%s" "$PWD" "${1#./}"
  fi
}

function build_sdist() {
  local dest=$1
  python python/setup.py sdist --dist-dir "$dest"
}

function build_bdist() {
  local dest=$1
  cd bazel-bin/python/build_pip_package.runfiles/com_google_riegeli/python
  python setup.py bdist_wheel --dist-dir "$dest"
  cd -
}

function main() {
  local dest=
  local sdist=false
  local bdist=false
  while [[ $# -gt 0 ]]; do
    if [[ $1 == --dest ]]; then
      shift
      dest=$(real_path "$1")
    elif [[ $1 == --sdist ]]; then
      sdist=true
    elif [[ $1 == --bdist ]]; then
      bdist=true
    else
      printf "Unknown flag: %s\n" "$1" >&2
      exit 1
    fi
    shift
  done
  if [[ -z $dest ]]; then
    printf "Missing required flag: --dest DIRECTORY\n" >&2
    exit 1
  fi
  if [[ $sdist != true ]] && [[ $bdist != true ]]; then
    printf "Nothing to do: missing --sdist or --bdist\n" >&2
    exit 1
  fi
  mkdir -p -- "$dest"
  if [[ $sdist = true ]]; then
    build_sdist "$dest"
  fi
  if [[ $bdist = true ]]; then
    build_bdist "$dest"
  fi
}

main "$@"
