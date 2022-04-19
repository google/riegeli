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

#include <unistd.h>

#include <cstdlib>
#include <memory>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/chain.h"
#include "riegeli/base/memory.h"
#include "riegeli/bytes/fd_reader.h"
#include "riegeli/bytes/fd_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace {

int std_in_fd = STDIN_FILENO;
int std_out_fd = STDOUT_FILENO;
int std_err_fd = STDERR_FILENO;

ChainBlock& StdInPending() {
  static NoDestructor<ChainBlock> pending;
  return *pending;
}

}  // namespace

NewStdIn::NewStdIn(Options options) : FdReader(std_in_fd, std::move(options)) {
  ChainBlock& pending = StdInPending();
  if (!pending.empty()) RestoreBuffer(std::move(pending));
}

void NewStdIn::Reset(Options options) {
  FdReader::Reset(std_in_fd, std::move(options));
  ChainBlock& pending = StdInPending();
  if (!pending.empty()) RestoreBuffer(std::move(pending));
}

void NewStdIn::Done() {
  RIEGELI_ASSERT(StdInPending().empty())
      << "Multiple instances of NewStdIn in use at a time";
  if (available() > 0 && !supports_random_access()) {
    StdInPending() = SaveBuffer();
  }
  FdReader::Done();
}

NewStdOut::NewStdOut(Options options)
    : FdWriter(std_out_fd, std::move(options)) {}

void NewStdOut::Reset(Options options) {
  FdWriter::Reset(std_out_fd, std::move(options));
}

NewStdErr::NewStdErr(Options options)
    : FdWriter(std_err_fd, std::move(options)) {}

void NewStdErr::Reset(Options options) {
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

namespace {

class StandardStreams {
 public:
  StandardStreams();

  StandardStreams(const StandardStreams&) = delete;
  StandardStreams& operator=(const StandardStreams&) = delete;

  ~StandardStreams() = delete;

  Reader& StdIn() { return *std_in_; }
  Writer& StdOut() { return *std_out_; }
  Writer& StdErr() { return *std_err_; }
  std::unique_ptr<Reader> SetStdIn(std::unique_ptr<Reader> value) {
    return std::exchange(std_in_, std::move(value));
  }
  std::unique_ptr<Writer> SetStdOut(std::unique_ptr<Writer> value) {
    return std::exchange(std_out_, std::move(value));
  }
  std::unique_ptr<Writer> SetStdErr(std::unique_ptr<Writer> value) {
    return std::exchange(std_err_, std::move(value));
  }

 private:
  void FlushAll() {
    std_out_->Flush();
    std_err_->Flush();
  }

  std::unique_ptr<Reader> std_in_;
  std::unique_ptr<Writer> std_out_;
  std::unique_ptr<Writer> std_err_;
};

StandardStreams::StandardStreams()
    : std_in_(std::make_unique<FdReader<UnownedFd>>(STDIN_FILENO)),
      std_out_(std::make_unique<FdWriter<UnownedFd>>(STDOUT_FILENO)),
      std_err_(std::make_unique<FdWriter<UnownedFd>>(STDERR_FILENO)) {
  static StandardStreams* singleton;
  singleton = this;
  std::atexit(+[] { singleton->FlushAll(); });
}

inline StandardStreams& GetStandardStreams() {
  static NoDestructor<StandardStreams> kStandardStreams;
  return *kStandardStreams;
}

}  // namespace

Reader& StdIn() { return GetStandardStreams().StdIn(); }
Writer& StdOut() { return GetStandardStreams().StdOut(); }
Writer& StdErr() { return GetStandardStreams().StdErr(); }
std::unique_ptr<Reader> SetStdIn(std::unique_ptr<Reader> value) {
  return GetStandardStreams().SetStdIn(std::move(value));
}
std::unique_ptr<Writer> SetStdOut(std::unique_ptr<Writer> value) {
  return GetStandardStreams().SetStdOut(std::move(value));
}
std::unique_ptr<Writer> SetStdErr(std::unique_ptr<Writer> value) {
  return GetStandardStreams().SetStdErr(std::move(value));
}

}  // namespace riegeli
