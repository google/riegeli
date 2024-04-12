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

#include <type_traits>
#include <utility>

#include "absl/base/attributes.h"
#include "absl/base/optimization.h"
#include "absl/status/status.h"
#include "riegeli/base/constexpr.h"
#include "riegeli/base/maker.h"
#include "riegeli/base/type_id.h"

namespace riegeli {

// By convention, a constructor with a single parameter of type `Closed`
// constructs the object as closed.
struct Closed {};
RIEGELI_INLINE_CONSTEXPR(Closed, kClosed, Closed());

// Internal representation of the basic state of class `Object` and similar
// classes: whether the object is open or closed, and whether it is not failed
// or failed (with an associated `absl::Status` for failure details).
class
#ifdef ABSL_ATTRIBUTE_TRIVIAL_ABI
    ABSL_ATTRIBUTE_TRIVIAL_ABI
#endif
        ObjectState {
 public:
  // Creates a closed `ObjectState`.
  explicit ObjectState(Closed) noexcept : status_ptr_(kClosedSuccessfully) {}

  // Creates an open `ObjectState`.
  ObjectState() = default;

  ObjectState(const ObjectState& that) = delete;
  ObjectState& operator=(const ObjectState& that) = delete;

  ObjectState(ObjectState&& that) noexcept;
  ObjectState& operator=(ObjectState&& that) noexcept;

  ~ObjectState();

  // Makes `*this` equivalent to a newly constructed `ObjectState`. This avoids
  // constructing a temporary `ObjectState` and moving from it.
  void Reset(Closed);
  void Reset();

  // Returns `true` if the `ObjectState` is OK, i.e. open and not failed.
  bool ok() const;

  // Returns `true` if the `ObjectState` is open, i.e. not closed.
  bool is_open() const;

  // Returns `true` if the `ObjectState` is not failed.
  bool not_failed() const;

  // Returns an `absl::Status` describing the failure if the `ObjectState` is
  // failed, or an `absl::FailedPreconditionError()` if the `ObjectState` is
  // successfully closed, or `absl::OkStatus()` if the `ObjectState` is OK.
  absl::Status status() const;

  // Marks the `ObjectState` as closed, keeping its `not_failed()` state
  // unchanged.
  //
  // Returns `not_failed()`.
  bool MarkClosed();

  // Marks the `ObjectState` as failed with the given `status`, keeping its
  // `is_open()` state unchanged.
  //
  // Always returns `false`.
  //
  // Precondition: `!status.ok()`
  ABSL_ATTRIBUTE_COLD bool Fail(absl::Status status);

  // Marks the `ObjectState` as not failed, keeping its `is_open()` state
  // unchanged.
  void MarkNotFailed();

  // Replaces the status of an already failed `ObjectState`.
  //
  // Preconditions:
  //   `!status.ok()`
  //   `!not_failed()`
  void SetStatus(absl::Status status);

 private:
  struct FailedStatus {
    // The `closed` flag may be changed from `false` to `true` by `Close()`.
    bool closed;
    // The actual `absl::Status`, never `absl::OkStatus()`.
    absl::Status status;
  };

  static constexpr uintptr_t kOk = 0;
  static constexpr uintptr_t kClosedSuccessfully = 1;

  static void DeleteStatus(uintptr_t status_ptr);

  // `status_ptr_` is `kOk`, or `kClosedSuccessfully`, or a `FailedStatus*`
  // `reinterpret_cast` to `uintptr_t`.
  uintptr_t status_ptr_ = kOk;
};

// `Object` is an abstract base class for data readers and writers, managing
// their state: whether they are open or closed, and whether they are not failed
// or failed (with an associated `absl::Status` for failure details). An
// `Object` is OK when it is open and not failed.
//
// An `Object` becomes closed when `Close()` finishes, when constructed as
// closed (usually with no parameters), or when moved from.
//
// An `Object` fails when it could not perform an operation due to an unexpected
// reason.
//
// Derived `Object` classes can be movable but not copyable. After a move the
// source `Object` is left closed.
class Object {
 public:
  Object(const Object&) = delete;
  Object& operator=(const Object&) = delete;

  virtual ~Object() {}

  // Indicates that the `Object` is no longer needed, but in the case of a
  // writer that its destination is needed. `Close()` may also report new
  // failures.
  //
  // If `!is_open()`, does nothing. Otherwise:
  //  * In the case of a writer, pushes buffered data to the destination.
  //  * In the case of a reader, verifies that the source is not truncated at
  //    the current position, i.e. that it either has more data or ends cleanly
  //    (for sources where truncation can be distinguished from a clean end).
  //  * Synchronizes the current position to the source or destination (if
  //    applicable).
  //  * Closes owned dependencies.
  //
  // Returns `true` if the `Object` did not fail, i.e. if it was OK just before
  // becoming closed.
  //
  // It is necessary to call `Close()` at the end of a successful writing
  // session, and it is recommended to call `Close()` at the end of a successful
  // reading session. It is not needed to call `Close()` on early returns,
  // assuming that contents of the destination do not matter after all, e.g.
  // because a failure is being reported instead; the destructor releases
  // resources in any case.
  bool Close();

  // Returns `true` if the `Object` is OK, i.e. open and not failed.
  bool ok() const { return state_.ok(); }

  // Returns `true` if the `Object` is open, i.e. not closed.
  bool is_open() const { return state_.is_open(); }

  // Returns `true` if the `Object` is not failed.
  bool not_failed() const { return state_.not_failed(); }

  // Returns an `absl::Status` describing the failure if the `Object` is failed,
  // or an `absl::FailedPreconditionError()` if the `Object` is successfully
  // closed, or `absl::OkStatus()` if the `Object` is OK.
  absl::Status status() const { return state_.status(); }

  // Marks the `Object` as failed with the given `status`, keeping its
  // `is_open()` state unchanged.
  //
  // In derived classes `Fail()` may have additional effects. In particular the
  // status can be annotated with some details using `AnnotateStatus()`, and
  // `OnFail()` may update other state.
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
  // Precondition: `!status.ok()`
  ABSL_ATTRIBUTE_COLD bool Fail(absl::Status status);

  // Can annotate `status` with some context, appropriately for the derived
  // class.
  //
  // This is called by `Fail()`, and can also be called externally to annotate a
  // failure related to this `Object`.
  ABSL_ATTRIBUTE_COLD absl::Status AnnotateStatus(absl::Status status);

  // Returns `status()` if `!ok()`, otherwise returns
  // `AnnotateStatus(std::move(other_status))`.
  //
  // This is typically used after a failed call which does not necessarily
  // make the object not OK, e.g. after reading functions, to return an
  // explanation if the source ends prematurely or has unexpected contents.
  ABSL_ATTRIBUTE_COLD absl::Status StatusOrAnnotate(absl::Status other_status);

  // Returns a token which allows to detect the class of the `Object` at
  // runtime.
  //
  // By default returns `TypeId()`. In order for a class to participate in class
  // detection at runtime, it must override `GetTypeId()`:
  // ```
  //   riegeli::TypeId A::GetTypeId() const override {
  //     return riegeli::TypeId::For<A>();
  //   }
  // ```
  //
  // Then, to actually cast:
  // ```
  //   if (A* const a = object->GetIf<A>(); a != nullptr) {
  //     ...
  //   }
  // ```
  //
  // This solution is more limited but faster than `typeid` or `dynamic_cast`.
  virtual TypeId GetTypeId() const;

  // Casts the runtime type of `this`, as determined by `GetTypeId()`, down to
  // `Target*`. Returns `nullptr` if the type does not match.
  template <typename Target>
  Target* GetIf();
  template <typename Target>
  const Target* GetIf() const;

 protected:
  // Creates a closed `Object`.
  explicit Object(Closed) noexcept : state_(kClosed) {}

  // Creates an open `Object`.
  Object() = default;

  // Moves the part of the object defined in the `Object` class.
  Object(Object&& that) = default;
  Object& operator=(Object&& that) = default;

  // Makes `*this` equivalent to a newly constructed `Object`. This avoids
  // constructing a temporary `Object` and moving from it. Derived classes which
  // redefine `Reset()` should include a call to `Object::Reset()`.
  void Reset(Closed) { state_.Reset(kClosed); }
  void Reset() { state_.Reset(); }

  // Marks the `Object` as not failed, keeping its `is_open()` state unchanged.
  // This can be used if the `Object` supports recovery after some failures.
  void MarkNotFailed() { state_.MarkNotFailed(); }

  // Replaces the status of an already failed `Object`.
  //
  // Preconditions:
  //   `!status.ok()`
  //   `!not_failed()`
  void SetStatus(absl::Status status);

  // Implementation of `Close()`, called if the `Object` is not closed yet.
  //
  // `Close()` returns early if `!is_open()`, otherwise calls `Done()` and marks
  // the `Object` as closed. See `Close()` for details of the responsibility of
  // `Done()`.
  //
  // The default implementation in `Object::Done()` does nothing.
  //
  // Precondition: `is_open()`
  virtual void Done();

  // Called by `Fail()` and `FailWithoutAnnotation()`. Can update some other
  // state, appropriately for the derived class.
  //
  // The default implementation in `Object::OnFail()` does nothing.
  ABSL_ATTRIBUTE_COLD virtual void OnFail();

  // Implementation of `AnnotateStatus()`.
  //
  // The default implementation in `Object::AnnotateStatusImpl()` returns
  // `status` unchanged.
  ABSL_ATTRIBUTE_COLD virtual absl::Status AnnotateStatusImpl(
      absl::Status status);

  // Exposes a variant of `Fail()` which does not call `AnnotateStatus()`.
  //
  // This can be called instead of `Fail()` if the annotation supplied by
  // `AnnotateStatus()` would be irrelevant or duplicated in a particular case.
  ABSL_ATTRIBUTE_COLD bool FailWithoutAnnotation(absl::Status status);

  // Support `Dependency`.
  friend MakerType<Closed> RiegeliDependencySentinel(Object*) {
    return {kClosed};
  }

 private:
  ObjectState state_;
};

// Implementation details follow.

inline ObjectState::ObjectState(ObjectState&& that) noexcept
    : status_ptr_(std::exchange(that.status_ptr_, kClosedSuccessfully)) {}

inline ObjectState& ObjectState::operator=(ObjectState&& that) noexcept {
  DeleteStatus(std::exchange(
      status_ptr_, std::exchange(that.status_ptr_, kClosedSuccessfully)));
  return *this;
}

inline ObjectState::~ObjectState() { DeleteStatus(status_ptr_); }

inline void ObjectState::Reset(Closed) {
  DeleteStatus(std::exchange(status_ptr_, kClosedSuccessfully));
}

inline void ObjectState::Reset() {
  DeleteStatus(std::exchange(status_ptr_, kOk));
}

inline void ObjectState::DeleteStatus(uintptr_t status_ptr) {
  if (ABSL_PREDICT_FALSE(status_ptr != kOk &&
                         status_ptr != kClosedSuccessfully)) {
    delete reinterpret_cast<FailedStatus*>(status_ptr);
  }
}

inline bool ObjectState::ok() const { return status_ptr_ == kOk; }

inline bool ObjectState::is_open() const {
  if (ABSL_PREDICT_TRUE(status_ptr_ == kOk)) return true;
  if (ABSL_PREDICT_TRUE(status_ptr_ == kClosedSuccessfully)) return false;
  return !reinterpret_cast<const FailedStatus*>(status_ptr_)->closed;
}

inline bool ObjectState::not_failed() const {
  return status_ptr_ == kOk || status_ptr_ == kClosedSuccessfully;
}

inline bool ObjectState::MarkClosed() {
  if (ABSL_PREDICT_TRUE(status_ptr_ == kOk ||
                        status_ptr_ == kClosedSuccessfully)) {
    status_ptr_ = kClosedSuccessfully;
    return true;
  }
  reinterpret_cast<FailedStatus*>(status_ptr_)->closed = true;
  return false;
}

inline void ObjectState::MarkNotFailed() {
  DeleteStatus(
      std::exchange(status_ptr_, is_open() ? kOk : kClosedSuccessfully));
}

inline bool Object::Close() {
  if (ABSL_PREDICT_FALSE(!state_.is_open())) return state_.not_failed();
  Done();
  return state_.MarkClosed();
}

inline absl::Status Object::AnnotateStatus(absl::Status status) {
  return AnnotateStatusImpl(std::move(status));
}

template <typename Target>
inline Target* Object::GetIf() {
  static_assert(std::is_base_of<Object, Target>::value,
                "GetIf() supports only downcasts");
  if (GetTypeId() != TypeId::For<Target>()) return nullptr;
  return static_cast<Target*>(this);
}

template <typename Target>
inline const Target* Object::GetIf() const {
  static_assert(std::is_base_of<Object, Target>::value,
                "GetIf() supports only downcasts");
  if (GetTypeId() != TypeId::For<Target>()) return nullptr;
  return static_cast<const Target*>(this);
}

}  // namespace riegeli

#endif  // RIEGELI_BASE_OBJECT_H_
