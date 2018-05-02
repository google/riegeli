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
#include <atomic>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/strings/string_view.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"

namespace riegeli {

// TypeId::For<A>() is a token which is equal to TypeId::For<B>() whenever
// A and B are the same type. TypeId() is another value not equal to any other.
class TypeId {
 public:
  TypeId() noexcept {}

  template <typename T>
  static TypeId For();

  friend bool operator==(TypeId a, TypeId b) { return a.ptr_ == b.ptr_; }
  friend bool operator!=(TypeId a, TypeId b) { return a.ptr_ != b.ptr_; }

 private:
  explicit TypeId(void* ptr) : ptr_(ptr) {}

  void* ptr_ = nullptr;
};

// Object is an abstract base class for data readers and writers, managing their
// state: whether they are closed, and whether they failed (with an associated
// human-readable message). An Object is healthy when it is not closed nor
// failed.
//
// An Object becomes closed when Close() finishes, when constructed as closed
// (usually with no parameters), or when moved from.
//
// An Object fails when it could not perform an operation due to an unexpected
// reason.
//
// Derived Object classes can be movable but not copyable. The source Object is
// left closed.
//
// Derived Object classes should be thread-compatible: const member functions
// may be called concurrently with const member functions, but non-const member
// functions may not be called concurrently with any member functions unless
// specified otherwise.
//
// A derived class may use background threads which may cause the Object to
// fail asynchronously. See comments in the protected section for rules
// regarding background threads and asynchronous Fail() calls.
class Object {
 public:
  Object(const Object&) = delete;
  Object& operator=(const Object&) = delete;

  // If a derived class uses background threads, its destructor must cause
  // background threads to stop interacting with the Object.
  virtual ~Object();

  // Indicates that the Object is no longer needed, but in the case of a writer
  // that its destination is needed.
  //
  // If closed(), does nothing. Otherwise:
  //
  // In the case of a writer, pushes buffered data to the destination. In the
  // case of a reader, verifies that the source is not truncated at the current
  // position, i.e. that it either has more data or ends cleanly (for sources
  // where truncation can be distinguished from a clean end).
  //
  // If the Object is healthy and owns a resource which is itself an Object,
  // closes the resource.
  //
  // Frees all owned resources.
  //
  // Returns true if the Object did not fail, i.e. if it was healthy just before
  // becoming closed.
  bool Close();

  // Returns true if the Object is healthy, i.e. not closed nor failed.
  bool healthy() const;

  // Returns true if the Object is closed.
  bool closed() const;

  // Returns a human-readable message describing the Object health.
  //
  // This is "Healthy", "Closed", or a failure message.
  absl::string_view message() const;

  // Returns a token which allows to detect the class of the Object at runtime.
  //
  // By default returns TypeId(). In order for a class to participate in class
  // detection at runtime, it must override GetTypeId():
  //
  //   TypeId A::GetTypeId() const override { return TypeId::For<A>(); }
  //
  // Then, to actually cast:
  //
  //   if (object->GetTypeId() == TypeId::For<A>()) {
  //     A* a = static_cast<A*>(object);
  //     ...
  //   }
  //
  // This solution is more limited but faster than typeid or dynamic_cast.
  virtual TypeId GetTypeId() const;

 protected:
  // Initial state of the Object.
  enum class State : uintptr_t {
    kOpen = 0,
    kClosed = 1,
  };

  // Creates an Object with the given initial state.
  explicit Object(State state) noexcept;

  // Moves the part of the object defined in the Object class.
  //
  // If a derived class uses background threads, its assignment operator, if
  // defined, should cause background threads of the target Object to stop
  // interacting with the Object before Object::Object(Object&&) is called.
  // Also, its move constructor and move assignment operator, if defined, should
  // cause background threads of the source Object to pause interacting with the
  // Object while Object::operator=(Object&&) is called, and then continue
  // interacting with the target Object instead.
  Object(Object&& src) noexcept;
  Object& operator=(Object&& src) noexcept;

  // Marks the Object as healthy. This can be used if the Object supports
  // resetting to a clean state after a failure (apart from assignment).
  //
  // If a derived class uses background threads, its methods which call
  // MarkHealthy() should cause background threads to stop interacting with the
  // Object before MarkHealthy() is called.
  void MarkHealthy();

  // Marks the Object as not failed, keeping its closed() status unchanged.
  // This can be used if the Object supports recovery after some failures.
  //
  // If a derived class uses background threads, its methods which call
  // MarkNotFailed() should cause background threads to stop interacting with
  // the Object before MarkNotFailed() is called.
  void MarkNotFailed();

  // Implementation of Close(), called if the Object is not closed yet.
  //
  // Close() returns early if closed(), otherwise calls Done() and marks the
  // Object as closed. See Close() for details of the responsibility of Done().
  //
  // If a derived class uses background threads, Done() should cause background
  // threads to stop interacting with the Object.
  //
  // Precondition: !closed()
  virtual void Done() = 0;

  // Marks the Object as failed with the specified message.
  //
  // Fail() always returns false, for convenience of reporting the failure as a
  // false result of a failing function.
  //
  // Even though Fail() is not const, it may be called concurrently with public
  // member functions, with const member functions, and with other Fail() calls.
  // If Fail() is called multiple times, the first message wins.
  //
  // Precondition: !closed()
  ABSL_ATTRIBUTE_COLD bool Fail(absl::string_view message);

  // If src.healthy(), equivalent to Fail(message), otherwise equivalent to
  // Fail(StrCat(message, ": ", src.message())).
  //
  // Precondition: !closed()
  ABSL_ATTRIBUTE_COLD bool Fail(absl::string_view message, const Object& src);

  // Equivalent to Fail(src.message()).
  //
  // Preconditions:
  //   !src.healthy()
  //   !closed()
  ABSL_ATTRIBUTE_COLD bool Fail(const Object& src);

 private:
  struct FailedStatus {
    explicit FailedStatus(absl::string_view message);

    size_t message_size;
    // The closed flag may be changed from false to true by Close(). This
    // happens after Done() finishes, and thus any background threads should
    // have stopped interacting with the Object.
    bool closed = false;
    // Beginning of message (actual message has size message_size).
    char message_data[1];
  };

  static constexpr uintptr_t kHealthy() {
    return static_cast<uintptr_t>(State::kOpen);
  }

  static constexpr uintptr_t kClosedSuccessfully() {
    return static_cast<uintptr_t>(State::kClosed);
  }

  static void DeleteStatus(uintptr_t status);

  // status_ is either kHealthy(), or kClosedSuccessfully(), or FailedStatus*
  // reinterpret_cast to uintptr_t.
  std::atomic<uintptr_t> status_;
};

// Implementation details follow.

template <typename T>
TypeId TypeId::For() {
  static char token;
  return TypeId(&token);
}

inline Object::Object(State state) noexcept
    : status_(static_cast<uintptr_t>(state)) {
  RIEGELI_ASSERT(state == State::kOpen || state == State::kClosed)
      << "Unknown state: " << static_cast<uintptr_t>(state);
}

inline Object::Object(Object&& src) noexcept
    : status_(src.status_.exchange(kClosedSuccessfully(),
                                   std::memory_order_relaxed)) {}

inline Object& Object::operator=(Object&& src) noexcept {
  DeleteStatus(status_.exchange(
      src.status_.exchange(kClosedSuccessfully(), std::memory_order_relaxed),
      std::memory_order_relaxed));
  return *this;
}

inline Object::~Object() {
  DeleteStatus(status_.load(std::memory_order_relaxed));
}

inline bool Object::Close() {
  const uintptr_t status_before = status_.load(std::memory_order_acquire);
  if (ABSL_PREDICT_FALSE(status_before != kHealthy())) {
    if (ABSL_PREDICT_TRUE(status_before == kClosedSuccessfully())) return true;
    if (reinterpret_cast<const FailedStatus*>(status_before)->closed) {
      return false;
    }
  }
  Done();
  const uintptr_t status_after = status_.load(std::memory_order_relaxed);
  if (ABSL_PREDICT_TRUE(status_after == kHealthy())) {
    status_.store(kClosedSuccessfully(), std::memory_order_relaxed);
    return true;
  }
  RIEGELI_ASSERT(status_after != kClosedSuccessfully() &&
                 !reinterpret_cast<const FailedStatus*>(status_after)->closed)
      << "Object marked as closed during Done()";
  reinterpret_cast<FailedStatus*>(status_after)->closed = true;
  return false;
}

inline void Object::DeleteStatus(uintptr_t status) {
  if (ABSL_PREDICT_FALSE(status != kHealthy() &&
                         status != kClosedSuccessfully())) {
    DeleteAligned(
        reinterpret_cast<FailedStatus*>(status),
        offsetof(FailedStatus, message_data) +
            reinterpret_cast<const FailedStatus*>(status)->message_size);
  }
}

inline bool Object::healthy() const {
  return status_.load(std::memory_order_acquire) == kHealthy();
}

inline bool Object::closed() const {
  const uintptr_t status = status_.load(std::memory_order_acquire);
  if (ABSL_PREDICT_TRUE(status == kHealthy())) return false;
  if (ABSL_PREDICT_TRUE(status == kClosedSuccessfully())) return true;
  return reinterpret_cast<const FailedStatus*>(status)->closed;
}

inline absl::string_view Object::message() const {
  const uintptr_t status = status_.load(std::memory_order_acquire);
  switch (status) {
    case kHealthy():
      return "Healthy";
    case kClosedSuccessfully():
      return "Closed";
    default:
      return absl::string_view(
          reinterpret_cast<const FailedStatus*>(status)->message_data,
          reinterpret_cast<const FailedStatus*>(status)->message_size);
  }
}

inline void Object::MarkHealthy() {
  DeleteStatus(status_.exchange(kHealthy(), std::memory_order_relaxed));
}

inline void Object::MarkNotFailed() {
  DeleteStatus(status_.exchange(closed() ? kClosedSuccessfully() : kHealthy(),
                                std::memory_order_relaxed));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_OBJECT_H_
