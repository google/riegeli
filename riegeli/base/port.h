// Copyright 2017 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RIEGELI_BASE_PORT_H_
#define RIEGELI_BASE_PORT_H_

// Clang has `__has_builtin()`. Other compilers need other means to detect
// availability of builtins.
#ifdef __has_builtin
#define RIEGELI_INTERNAL_HAS_BUILTIN(x) __has_builtin(x)
#else
#define RIEGELI_INTERNAL_HAS_BUILTIN(x) 0
#endif

#define RIEGELI_INTERNAL_IS_GCC_VERSION(major, minor) \
  (__GNUC__ > (major) || (__GNUC__ == (major) && __GNUC_MINOR__ >= (minor)))

#endif  // RIEGELI_BASE_PORT_H_
