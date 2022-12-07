// Copyright 2020 Google LLC
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

#include "riegeli/bytes/std_io.h"

#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/no_destructor.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/bytes/fd_writer.h"

namespace riegeli {

namespace {

int std_in_fd = 0;
int std_out_fd = 1;
int std_err_fd = 2;

ChainBlock& StdInPending() {
  static NoDestructor<ChainBlock> pending;
  return *pending;
}

}  // namespace

StdIn::StdIn(Options options) : FdReader(std_in_fd, std::move(options)) {
  ChainBlock& pending = StdInPending();
  if (!pending.empty()) RestoreBuffer(std::move(pending));
}

void StdIn::Reset(Options options) {
  FdReader::Reset(std_in_fd, std::move(options));
  ChainBlock& pending = StdInPending();
  if (!pending.empty()) RestoreBuffer(std::move(pending));
}

void StdIn::Done() {
  RIEGELI_ASSERT(StdInPending().empty())
      << "Multiple instances of StdIn in use at a time";
  if (available() > 0 && !SupportsRandomAccess()) {
    StdInPending() = SaveBuffer();
  }
  FdReader::Done();
}

StdOut::StdOut(Options options) : FdWriter(std_out_fd, std::move(options)) {}

void StdOut::Reset(Options options) {
  FdWriter::Reset(std_out_fd, std::move(options));
}

StdErr::StdErr(Options options) : FdWriter(std_err_fd, std::move(options)) {}

void StdErr::Reset(Options options) {
  FdWriter::Reset(std_err_fd, std::move(options));
}

InjectedStdInFd::InjectedStdInFd(int fd)
    : old_fd_(std::exchange(std_in_fd, fd)),
      old_pending_(std::move(StdInPending())) {}

InjectedStdInFd::~InjectedStdInFd() {
  std_in_fd = old_fd_;
  StdInPending() = std::move(old_pending_);
}

InjectedStdOutFd::InjectedStdOutFd(int fd)
    : old_fd_(std::exchange(std_out_fd, fd)) {}

InjectedStdOutFd::~InjectedStdOutFd() { std_out_fd = old_fd_; }

InjectedStdErrFd::InjectedStdErrFd(int fd)
    : old_fd_(std::exchange(std_err_fd, fd)) {}

InjectedStdErrFd::~InjectedStdErrFd() { std_err_fd = old_fd_; }

}  // namespace riegeli
