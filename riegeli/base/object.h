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
#include <atomic>
#include <string>

#include "riegeli/base/assert.h"
#include "riegeli/base/base.h"
#include "riegeli/base/memory.h"
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
  // Creates the Object as healthy.
  //
  // To create the Object as closed, see MarkClosed().
  Object() = default;

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

  // Marks the Object as closed.
  //
  // This is intended for default constructors of derived classes to create a
  // closed Object. If a derived class uses background threads, they should not
  // be running yet when MarkClosed() is called.
  //
  // Precondition: healthy()
  void MarkClosed();

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
  RIEGELI_ATTRIBUTE_COLD bool Fail(string_view message);

 private:
  struct Failed {
    // The closed flag may be changed from false to true by Close(). This
    // happens after Done() finishes, and thus any background threads should
    // have stopped interacting with the Object.
    bool closed;
    const std::string message;
  };

  static constexpr uintptr_t kHealthy() { return 0; }
  static constexpr uintptr_t kClosedSuccessfully() { return 1; }

  static void DeleteStatus(uintptr_t status);

  // status_ is either kHealthy(), or kClosedSuccessfully(), or Failed*
  // reinterpret_cast to uintptr_t.
  std::atomic<uintptr_t> status_{kHealthy()};
};

// Implementation details follow.

template <typename T>
TypeId TypeId::For() {
  static char token;
  return TypeId(&token);
}

inline void Object::DeleteStatus(uintptr_t status) {
  switch (status) {
    case kHealthy():
    case kClosedSuccessfully():
      break;
    default:
      delete reinterpret_cast<Failed*>(status);
      break;
  }
}

inline bool Object::healthy() const {
  return status_.load(std::memory_order_acquire) == kHealthy();
}

inline bool Object::closed() const {
  const uintptr_t status = status_.load(std::memory_order_acquire);
  switch (status) {
    case kHealthy():
      return false;
    case kClosedSuccessfully():
      return true;
    default:
      return reinterpret_cast<Failed*>(status)->closed;
  }
}

inline void Object::MarkHealthy() {
  DeleteStatus(status_.exchange(kHealthy(), std::memory_order_relaxed));
}

inline void Object::MarkClosed() {
  RIEGELI_ASSERT(healthy());
  status_.store(kClosedSuccessfully(), std::memory_order_relaxed);
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

}  // namespace riegeli

#endif  // RIEGELI_BASE_OBJECT_H_
