#ifndef INCLUDE_TRANSFUSE_DEFER_H
#define INCLUDE_TRANSFUSE_DEFER_H

#include <functional>

namespace transfuse {

struct Defer {
    /// The deferred function.
    std::function<void()> fn;

    /// Constructor.
    explicit Defer(std::function<void()> fn)
        : fn(std::move(fn)) {}

    /// Destructor.
    /// Calls the deferred function.
    ~Defer() { fn(); }

    /// Runs the deferred funciton.
    void run() { fn(); fn = [](){}; }
};


}  // namespace transfuse

#endif
