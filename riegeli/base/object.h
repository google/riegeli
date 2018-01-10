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

#ifndef RIEGELI_BASE_OBJECT_H_
#define RIEGELI_BASE_OBJECT_H_

#include <stddef.h>
#include <stdint.h>
#include <ios>
#include <ostream>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/string_view.h"

namespace riegeli {

// TypeId::For<A>() is a token which is equal to TypeId::For<B>() whenever
// A and B are the same type. TypeId() is another value not equal to any other.
class TypeId {
 public:
  TypeId() : ptr_(nullptr) {}

  template <typename T>
  static TypeId For();

  friend bool operator==(TypeId a, TypeId b) { return a.ptr_ == b.ptr_; }
  friend bool operator!=(TypeId a, TypeId b) { return a.ptr_ != b.ptr_; }

 private:
  explicit TypeId(void* ptr) : ptr_(ptr) {}

  void* ptr_;
};

// Object is an abstract base class for data readers and writers, managing their
// state: whether they are closed, and whether they failed (with an associated
// human-readable message). An Object is healthy when it is not closed nor
// failed. When an object is cancelled, it is both failed with message
// "Cancelled" and closed.
//
// An Object becomes closed after calling Close() or Cancel(), when constructed
// as cancelled (with no parameters), or when moved from.
//
// An Object becomes failed when it could not perform an operation due to an
// unexpected reason, after calling Cancel() (if it was not already closed),
// when constructed as cancelled (with no parameters), or when moved from.
//
// Derived Object classes can be movable but not copyable. Assignment operator
// cancels the target Object before the move. The source Object is left
// cancelled.
class Object {
 public:
  Object(const Object&) = delete;
  Object& operator=(const Object&) = delete;

  // Cancels the Object.
  //
  // This is implemented by derived classes.
  virtual ~Object() = default;

  // Indicates that the Object is no longer needed, but in the case of a writer
  // that its destination is needed.
  //
  // In the case of a writer, pushes buffered data to the destination. In the
  // case of a reader, verifies that the source is not truncated at the current
  // position, i.e. that it either has more data or ends cleanly (for sources
  // where truncation can be distinguished from a clean end).
  //
  // If the Object is healthy, closes its destination or source if it is
  // owned by the Object. In the case of a writer, if the writer is unhealthy,
  // cancels its destination, no matter whether it is owned.
  //
  // Returns true on success, or if the Object was successfully closed
  // beforehand.
  bool Close();

  // Indicates that the Object is no longer needed, and in the case of a writer
  // that its destination is not needed either.
  //
  // This is equivalent to marking the Object as unhealthy and then calling
  // Close(), ignoring its result.
  void Cancel();

  // Returns true if the Object is healthy, i.e. not closed nor failed.
  bool healthy() const { return state_ == State::kHealthy; }

  // Returns a human-readable message describing the Object health.
  //
  // This is "Healthy", "Closed", "Cancelled", or a failure message.
  const std::string& Message() const;

  // Returns a token which allows to detect the class of the Object at runtime.
  //
  // By default returns TypeId(). In order for a class to participate in class
  // detection at runtime, it must override GetTypeId():
  //
  //   TypeId A::GetTypeId() const override { return TypeId::For<A>(); }
  //
  // This solution is more limited but faster than typeid or dynamic_cast.
  virtual TypeId GetTypeId() const;

 protected:
  Object() = default;

  // Moves the part of the object defined in the Object class.
  //
  // Precondition: &src != this
  Object(Object&& src) noexcept;
  void operator=(Object&& src) noexcept;

  // Marks the Object as healthy. This can be used if the Object supports
  // resetting to a clean state after a failure (apart from assignment).
  void MarkHealthy();

  // Marks the Object as cancelled. This is used for constructing an already
  // cancelled Object.
  //
  // Precondition: healthy()
  void MarkCancelled();

  // Implementation of Close() and Cancel(), called when the Object is not
  // closed yet.
  //
  // Close() returns early if the Object was already closed, otherwise calls
  // Done(), and marks the Object as closed.
  //
  // Cancel() returns early if the Object was already closed, otherwise marks
  // the Object as failed, calls Done(), and marks the Object as closed.
  virtual void Done() = 0;

  // Marks the Object as failed with the specified message. Always returns
  // false.
  //
  // Precondition: healthy()
  RIEGELI_ATTRIBUTE_COLD bool Fail(string_view message);

 private:
  enum class State : int {
    kHealthy = 0,
    kClosed = 1,
    // Cancellation is a case of failure. In State cancellation is treated
    // separately to avoid setting message_ on cancellation.
    kCancelling = 2,
    kCancelled = 3,        // kCancelling | kClosed
    kFailed = 6,           // 4 | kCancelling
    kFailedAndClosed = 7,  // kFailed | kClosed
  };

  State state_ = State::kHealthy;
  std::string message_;
};

// Implementation details follow.

template <typename T>
TypeId TypeId::For() {
  static char token;
  return TypeId(&token);
}

inline void Object::MarkHealthy() {
  state_ = State::kHealthy;
  message_.clear();
}

inline void Object::MarkCancelled() {
  RIEGELI_ASSERT(healthy());
  state_ = State::kCancelled;
}

inline Object::Object(Object&& src) noexcept
    : state_(riegeli::exchange(src.state_, State::kCancelled)),
      message_(std::move(src.message_)) {
  src.message_.clear();
}

inline void Object::operator=(Object&& src) noexcept {
  RIEGELI_ASSERT(&src != this);
  Cancel();
  state_ = riegeli::exchange(src.state_, State::kCancelled);
  message_ = std::move(src.message_);
  src.message_.clear();
}

inline bool Object::Close() {
  if ((static_cast<int>(state_) & static_cast<int>(State::kClosed)) == 0) {
    Done();
    state_ = static_cast<State>(static_cast<int>(state_) |
                                static_cast<int>(State::kClosed));
  }
  return state_ == State::kClosed;  // Returns true if not failed.
}

inline void Object::Cancel() {
  if ((static_cast<int>(state_) & static_cast<int>(State::kClosed)) == 0) {
    state_ = static_cast<State>(static_cast<int>(state_) |
                                static_cast<int>(State::kCancelling));
    Done();
    state_ = static_cast<State>(static_cast<int>(state_) |
                                static_cast<int>(State::kClosed));
  }
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_OBJECT_H_
