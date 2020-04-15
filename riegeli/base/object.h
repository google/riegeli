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

#include <stdint.h>

#include <atomic>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "riegeli/base/base.h"

namespace riegeli {

// `TypeId::For<A>()` is a token which is equal to `TypeId::For<B>()` whenever
// `A` and `B` are the same type. `TypeId()` is another value not equal to any
// other.
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

// `Object` is an abstract base class for data readers and writers, managing
// their state: whether they are closed, and whether they failed (with an
// associated `absl::Status`). An `Object` is healthy when it is not closed nor
// failed.
//
// An `Object` becomes closed when `Close()` finishes, when constructed as
// closed (usually with no parameters), or when moved from.
//
// An `Object` fails when it could not perform an operation due to an unexpected
// reason.
//
// Derived `Object` classes can be movable but not copyable. After a move the
// source `Object` is left closed.
//
// Derived `Object` classes should be thread-compatible: const member functions
// may be called concurrently with const member functions, but non-const member
// functions may not be called concurrently with any member functions unless
// specified otherwise.
//
// A derived class may use background threads which may cause the `Object` to
// fail asynchronously. See comments in the protected section for rules
// regarding background threads and asynchronous `Fail()` calls.
class Object {
 public:
  Object(const Object&) = delete;
  Object& operator=(const Object&) = delete;

  // If a derived class uses background threads, its destructor must cause
  // background threads to stop interacting with the `Object`.
  virtual ~Object();

  // Indicates that the `Object` is no longer needed, but in the case of a
  // writer that its destination is needed.
  //
  // If `closed()`, does nothing. Otherwise:
  //
  // In the case of a writer, pushes buffered data to the destination. In the
  // case of a reader, verifies that the source is not truncated at the current
  // position, i.e. that it either has more data or ends cleanly (for sources
  // where truncation can be distinguished from a clean end).
  //
  // Closes owned dependencies.
  //
  // Returns `true` if the `Object` did not fail, i.e. if it was healthy just
  // before becoming closed.
  bool Close();

  // Returns `true` if the `Object` is healthy, i.e. not closed nor failed.
  bool healthy() const;

  // Returns `true` if the `Object` is closed.
  bool closed() const;

  // Returns an `absl::Status` describing the failure if the `Object` is failed,
  // or an `absl::FailedPreconditionError()` if the `Object` is closed, or
  // `absl::OkStatus()` if the `Object` is healthy.
  absl::Status status() const;

  // Marks the `Object` as failed with the specified `absl::Status`.
  //
  // Derived classes may override `Fail()` to annotate the `absl::Status` with
  // some context or update other state.
  //
  // Even though `Fail()` is not const, if the derived class allows this, it may
  // be called concurrently with public member functions, with const member
  // functions, and with other `Fail()` calls.
  //
  // If `Fail()` is called multiple times, the first `absl::Status` wins.
  //
  // `Fail()` always returns `false`, for convenience of reporting the failure
  // as a `false` result of a failing function.
  //
  // `Fail()` is normally called by other methods of the same `Object`, but it
  // is public to allow injecting a failure related to the `Object` (such as
  // unexpected data returned by it) if that failure does not have to be
  // distinguished from failures of the `Object` itself.
  //
  // Preconditions:
  //   `!status.ok()`
  //   `!closed()`
  ABSL_ATTRIBUTE_COLD virtual bool Fail(absl::Status status);

  // Propagates failure from another `Object`.
  //
  // Equivalent to `Fail(dependency.status())`.
  //
  // Preconditions:
  //   `!dependency.healthy()`
  //   `!closed()`
  ABSL_ATTRIBUTE_COLD bool Fail(const Object& dependency);

  // Returns a token which allows to detect the class of the `Object` at
  // runtime.
  //
  // By default returns `TypeId()`. In order for a class to participate in class
  // detection at runtime, it must override `GetTypeId()`:
  // ```
  //   TypeId A::GetTypeId() const override { return TypeId::For<A>(); }
  // ```
  //
  // Then, to actually cast:
  // ```
  //   if (object->GetTypeId() == TypeId::For<A>()) {
  //     A* a = static_cast<A*>(object);
  //     ...
  //   }
  // ```
  //
  // This solution is more limited but faster than `typeid` or `dynamic_cast`.
  virtual TypeId GetTypeId() const;

 protected:
  // By a common convention default-constructed objects are closed, and objects
  // constructed with non-empty parameter lists are open.
  //
  // This convention is not applicable to abstract classes with no natural
  // constructor parameters. Instead, these classes (such as `Object` itself)
  // have no default constructor, and constructors with a dummy parameter of
  // type `InitiallyClosed` or `InitiallyOpen` disambiguate the intent.
  struct InitiallyClosed {};
  struct InitiallyOpen {};

  static constexpr InitiallyClosed kInitiallyClosed{};
  static constexpr InitiallyOpen kInitiallyOpen{};

  // Creates an `Object` with the given initial state.
  explicit Object(InitiallyClosed) noexcept;
  explicit Object(InitiallyOpen) noexcept;

  // Moves the part of the object defined in the `Object` class.
  //
  // If a derived class uses background threads, its assignment operator, if
  // defined, should cause background threads of the target Object to stop
  // interacting with the `Object` before `Object::Object(Object&&)` is called.
  // Also, its move constructor and move assignment operator, if defined,
  // should cause background threads of the source Object to pause interacting
  // with the `Object` while `Object::operator=(Object&&)` is called, and then
  // continue interacting with the target `Object` instead.
  Object(Object&& that) noexcept;
  Object& operator=(Object&& that) noexcept;

  // Makes `*this` equivalent to a newly constructed `Object`. This avoids
  // constructing a temporary `Object` and moving from it. Derived classes which
  // override `Reset()` should include a call to `Object::Reset()`.
  //
  // If a derived class uses background threads, its methods which call
  // `Object::Reset()` should cause background threads to stop interacting with
  // the `Object` before `Object::Reset()` is called.
  void Reset(InitiallyClosed);
  void Reset(InitiallyOpen);

  // Marks the `Object` as not failed, keeping its `closed()` status unchanged.
  // This can be used if the `Object` supports recovery after some failures.
  //
  // If a derived class uses background threads, its methods which call
  // `MarkNotFailed()` should cause background threads to stop interacting with
  // the `Object` before `MarkNotFailed()` is called.
  void MarkNotFailed();

  // Implementation of `Close()`, called if the `Object` is not closed yet.
  //
  // `Close()` returns early if `closed()`, otherwise calls `Done()` and marks
  // the `Object` as closed. See `Close()` for details of the responsibility of
  // `Done()`.
  //
  // If a derived class uses background threads, `Done()` should cause
  // background threads to stop interacting with the `Object`.
  //
  // Precondition: `!closed()`
  virtual void Done() {}

 private:
  struct FailedStatus {
    explicit FailedStatus(absl::Status&& status);

    // The `closed` flag may be changed from `false` to `true` by `Close()`.
    // This happens after `Done()` finishes, and thus any background threads
    // should have stopped interacting with the `Object`.
    bool closed = false;
    // The actual `absl::Status`, never `absl::OkStatus()`.
    absl::Status status;
  };

  static constexpr uintptr_t kHealthy = 0;
  static constexpr uintptr_t kClosedSuccessfully = 1;

  static void DeleteStatus(uintptr_t status_ptr);

  // `status_ptr_` is either `kHealthy`, or `kClosedSuccessfully`, or
  // `FailedStatus*` `reinterpret_cast` to `uintptr_t`.
  std::atomic<uintptr_t> status_ptr_;
};

// Implementation details follow.

template <typename T>
inline TypeId TypeId::For() {
  static char token;
  return TypeId(&token);
}

inline Object::Object(InitiallyClosed) noexcept
    : status_ptr_(kClosedSuccessfully) {}

inline Object::Object(InitiallyOpen) noexcept : status_ptr_(kHealthy) {}

inline Object::Object(Object&& that) noexcept
    : status_ptr_(that.status_ptr_.exchange(kClosedSuccessfully,
                                            std::memory_order_relaxed)) {}

inline Object& Object::operator=(Object&& that) noexcept {
  DeleteStatus(status_ptr_.exchange(
      that.status_ptr_.exchange(kClosedSuccessfully, std::memory_order_relaxed),
      std::memory_order_relaxed));
  return *this;
}

inline Object::~Object() {
  DeleteStatus(status_ptr_.load(std::memory_order_relaxed));
}

inline bool Object::Close() {
  const uintptr_t status_ptr_before =
      status_ptr_.load(std::memory_order_acquire);
  if (ABSL_PREDICT_FALSE(status_ptr_before != kHealthy)) {
    if (ABSL_PREDICT_TRUE(status_ptr_before == kClosedSuccessfully)) {
      return true;
    }
    if (reinterpret_cast<const FailedStatus*>(status_ptr_before)->closed) {
      return false;
    }
  }
  Done();
  const uintptr_t status_ptr_after =
      status_ptr_.load(std::memory_order_relaxed);
  if (ABSL_PREDICT_TRUE(status_ptr_after == kHealthy)) {
    status_ptr_.store(kClosedSuccessfully, std::memory_order_relaxed);
    return true;
  }
  RIEGELI_ASSERT(
      status_ptr_after != kClosedSuccessfully &&
      !reinterpret_cast<const FailedStatus*>(status_ptr_after)->closed)
      << "Object marked as closed during Done()";
  reinterpret_cast<FailedStatus*>(status_ptr_after)->closed = true;
  return false;
}

inline void Object::DeleteStatus(uintptr_t status_ptr) {
  if (ABSL_PREDICT_FALSE(status_ptr != kHealthy &&
                         status_ptr != kClosedSuccessfully)) {
    delete reinterpret_cast<FailedStatus*>(status_ptr);
  }
}

inline bool Object::healthy() const {
  return status_ptr_.load(std::memory_order_acquire) == kHealthy;
}

inline bool Object::closed() const {
  const uintptr_t status_ptr = status_ptr_.load(std::memory_order_acquire);
  if (ABSL_PREDICT_TRUE(status_ptr == kHealthy)) return false;
  if (ABSL_PREDICT_TRUE(status_ptr == kClosedSuccessfully)) return true;
  return reinterpret_cast<const FailedStatus*>(status_ptr)->closed;
}

inline absl::Status Object::status() const {
  const uintptr_t status_ptr = status_ptr_.load(std::memory_order_acquire);
  if (status_ptr == kHealthy) return absl::OkStatus();
  if (status_ptr == kClosedSuccessfully) {
    return absl::FailedPreconditionError("Object closed");
  }
  return reinterpret_cast<const FailedStatus*>(status_ptr)->status;
}

inline void Object::Reset(InitiallyClosed) {
  DeleteStatus(
      status_ptr_.exchange(kClosedSuccessfully, std::memory_order_relaxed));
}

inline void Object::Reset(InitiallyOpen) {
  DeleteStatus(status_ptr_.exchange(kHealthy, std::memory_order_relaxed));
}

inline void Object::MarkNotFailed() {
  DeleteStatus(status_ptr_.exchange(closed() ? kClosedSuccessfully : kHealthy,
                                    std::memory_order_relaxed));
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_OBJECT_H_
