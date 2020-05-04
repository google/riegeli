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

#include <cstdlib>
#include <iostream>
#include <memory>
#include <utility>

#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"
#include "riegeli/bytes/istream_reader.h"
#include "riegeli/bytes/ostream_writer.h"
#include "riegeli/bytes/reader.h"
#include "riegeli/bytes/writer.h"

namespace riegeli {

namespace {

class StandardStreams {
 public:
  StandardStreams();

  StandardStreams(const StandardStreams&) = delete;
  StandardStreams& operator=(const StandardStreams&) = delete;

  ~StandardStreams() = delete;

  Reader* StdIn() {
    std_out_->Flush(FlushType::kFromProcess);
    return std_in_.get();
  }
  Writer* StdOut() { return std_out_.get(); }
  FlushingWriterPtr StdErr() {
    std_out_->Flush(FlushType::kFromProcess);
    return FlushingWriterPtr(std_err_.get());
  }
  Reader* JustStdIn() { return std_in_.get(); }
  Writer* JustStdErr() { return std_err_.get(); }
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
    std_out_->Flush(FlushType::kFromProcess);
    std_err_->Flush(FlushType::kFromProcess);
  }

  std::unique_ptr<Reader> std_in_;
  std::unique_ptr<Writer> std_out_;
  std::unique_ptr<Writer> std_err_;
};

StandardStreams::StandardStreams()
    : std_in_(std::make_unique<IstreamReader<>>(
          &std::cin, IstreamReaderBase::Options().set_assumed_pos(0))),
      std_out_(std::make_unique<OstreamWriter<>>(
          &std::cout, OstreamWriterBase::Options().set_assumed_pos(0))),
      std_err_(std::make_unique<OstreamWriter<>>(
          &std::cerr, OstreamWriterBase::Options().set_assumed_pos(0))) {
  static StandardStreams* singleton;
  singleton = this;
  std::atexit(+[] { singleton->FlushAll(); });
}

inline StandardStreams* GetStandardStreams() {
  static NoDestructor<StandardStreams> kStandardStreams;
  return kStandardStreams.get();
}

}  // namespace

Reader* StdIn() { return GetStandardStreams()->StdIn(); }
Writer* StdOut() { return GetStandardStreams()->StdOut(); }
FlushingWriterPtr StdErr() { return GetStandardStreams()->StdErr(); }
Reader* JustStdIn() { return GetStandardStreams()->JustStdIn(); }
Writer* JustStdErr() { return GetStandardStreams()->JustStdErr(); }
std::unique_ptr<Reader> SetStdIn(std::unique_ptr<Reader> value) {
  return GetStandardStreams()->SetStdIn(std::move(value));
}
std::unique_ptr<Writer> SetStdOut(std::unique_ptr<Writer> value) {
  return GetStandardStreams()->SetStdOut(std::move(value));
}
std::unique_ptr<Writer> SetStdErr(std::unique_ptr<Writer> value) {
  return GetStandardStreams()->SetStdErr(std::move(value));
}

}  // namespace riegeli
