/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#pragma once

#include "common/EventList.h"
#include "common/Type.h"
#include <array>
#include <atomic>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace NetworkAnalytical {

/**
 * EventQueue manages scheduled EventLists.
 */
class EventQueue {
  public:
    /**
     * Constructor.
     */
    EventQueue() noexcept;

    /**
     * Destructor.
     */
    ~EventQueue() noexcept;

    /**
     * Get current event time of the event queue.
     *
     * @return current event time
     */
    [[nodiscard]] EventTime get_current_time() const noexcept;

    /**
     * Check all registered events are invoked.
     * i.e., check if the event queue is empty.
     *
     * @return true if the event queue is empty, false otherwise
     */
    [[nodiscard]] bool finished() const noexcept;

    /**
     * Proceed the event queue.
     * i.e., first update the current event time to the next registered event
     * time, and then invoke all events registered at the current updated event
     * time.
     */
    void proceed() noexcept;

    /**
     * Schedule an event with a given event time.
     *
     * @param event_time time of event
     * @param callback callback function pointer
     * @param callback_arg argument of the callback function
     */
    void schedule_event(EventTime event_time, Callback callback, CallbackArg callback_arg) noexcept;

    /**
     * Register a callback whose events can be executed concurrently when the
     * callback arguments differ.
     *
     * Callbacks not registered here keep the original serial execution order.
     * Registration is lock-free and expected to happen once during startup
     * before any simulation work begins.
     *
     * @param callback callback function pointer
     */
    static void register_parallel_safe_callback(Callback callback) noexcept;

  private:
    struct ScheduledEvent {
        EventTime event_time;
        Callback callback;
        CallbackArg callback_arg;
    };

    struct ThreadLocalScheduleBuffer {
        EventQueue* owner;
        std::vector<ScheduledEvent> events;
    };

    static thread_local ThreadLocalScheduleBuffer* thread_local_schedule_buffer;

    // Whitelist of callbacks that may execute in parallel across groups with
    // distinct callback arguments. The whitelist is tiny (bounded by the
    // concrete simulator) and is written once at startup, so we use a
    // fixed-size lock-free array that readers can scan without synchronization.
    static constexpr std::size_t max_parallel_safe_callbacks = 16;
    static std::array<std::atomic<Callback>, max_parallel_safe_callbacks> parallel_safe_callbacks;
    static std::atomic<std::size_t> parallel_safe_callback_count;

    struct Stats {
        uint64_t schedule_calls = 0;
        uint64_t new_event_lists = 0;
        uint64_t proceed_calls = 0;
        uint64_t drained_batches = 0;
        uint64_t drained_events = 0;
        uint64_t serial_events = 0;
        uint64_t parallel_safe_events = 0;
        uint64_t parallel_runs = 0;
        uint64_t parallel_events = 0;
        uint64_t parallel_groups = 0;
        std::size_t max_queue_size = 0;
        std::size_t max_batch_size = 0;
        std::size_t max_parallel_groups = 0;
    };

    class WorkerPool;

    /// current time of the event queue
    std::atomic<EventTime> current_time;

    /// EventLists indexed by event time
    std::map<EventTime, EventList> event_queue;

    /// guards schedule_event's fallback path for unexpected external callers.
    /// proceed() and the in-batch flushback run single-threaded so they do not
    /// take this mutex.
    mutable std::mutex schedule_mutex;

    /// optional instrumentation counters
    Stats stats;

    /// persistent worker pool for same-timestamp batch parallelism.
    /// Created lazily the first time a parallel run actually dispatches work,
    /// and torn down in the destructor.
    std::unique_ptr<WorkerPool> worker_pool;
    std::size_t worker_pool_size = 0;

    /**
     * Schedule an event directly into event_queue. The caller must be
     * single-threaded with respect to event_queue (main thread only) or hold
     * schedule_mutex.
     */
    void schedule_event_locked(EventTime event_time,
                               Callback callback,
                               CallbackArg callback_arg) noexcept;

    /**
     * Invoke a stable batch of same-timestamp events.
     */
    void invoke_event_batch(std::vector<Event>& events) noexcept;

    /**
     * Invoke a range of callbacks that are known to be parallel-safe.
     */
    void invoke_parallel_safe_run(std::vector<Event>& events,
                                  std::size_t begin,
                                  std::size_t end,
                                  std::vector<std::vector<ScheduledEvent>>& scheduled_by_event) noexcept;

    /**
     * Ensure the worker pool exists with at least the requested number of
     * workers. Workers are persistent; invocation reuses them across all
     * parallel runs to avoid per-batch thread start/join cost.
     */
    void ensure_worker_pool(std::size_t worker_count) noexcept;

    /**
     * Check whether the callback is registered as parallel-safe. Lock-free.
     */
    [[nodiscard]] static bool is_parallel_safe_callback(Callback callback) noexcept;
};

}  // namespace NetworkAnalytical
