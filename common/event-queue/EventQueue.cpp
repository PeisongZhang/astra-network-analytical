/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "common/EventQueue.h"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace NetworkAnalytical;

thread_local EventQueue::ThreadLocalScheduleBuffer* EventQueue::thread_local_schedule_buffer = nullptr;
std::array<std::atomic<Callback>, EventQueue::max_parallel_safe_callbacks>
    EventQueue::parallel_safe_callbacks{};
std::atomic<std::size_t> EventQueue::parallel_safe_callback_count{0};

namespace {

std::size_t read_size_env(const char* const name, const std::size_t default_value) noexcept {
    const auto* const value = std::getenv(name);
    if (value == nullptr) {
        return default_value;
    }

    const auto parsed_value = std::strtoull(value, nullptr, 10);
    if (parsed_value == 0) {
        return default_value;
    }

    return static_cast<std::size_t>(parsed_value);
}

bool is_env_enabled(const char* const name) noexcept {
    const auto* const value = std::getenv(name);
    return value != nullptr && *value != '\0' && std::string(value) != "0";
}

}  // namespace

namespace NetworkAnalytical {

// Persistent worker pool. A single dispatch/wait pair reuses the same OS
// threads for the whole simulation, amortizing thread start/join cost across
// every same-timestamp parallel run.
class EventQueue::WorkerPool {
  public:
    explicit WorkerPool(const std::size_t worker_count) {
        workers.reserve(worker_count);
        for (std::size_t i = 0; i < worker_count; ++i) {
            workers.emplace_back([this, i] { worker_loop(i); });
        }
    }

    ~WorkerPool() noexcept {
        {
            const auto lock = std::lock_guard<std::mutex>(mtx);
            stopping = true;
            ++generation;
        }
        cv_start.notify_all();
        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return workers.size();
    }

    // Fire off `worker_count` workers to run `fn(worker_id)`. Non-blocking.
    // `worker_count` must be <= size(). Worker ids are [0, worker_count).
    void dispatch(std::function<void(std::size_t)> fn, const std::size_t worker_count) {
        assert(worker_count <= workers.size());
        {
            const auto lock = std::lock_guard<std::mutex>(mtx);
            body = std::move(fn);
            active_workers = worker_count;
            remaining_workers = worker_count;
            ++generation;
        }
        cv_start.notify_all();
    }

    // Wait for the last dispatch to complete.
    void wait() {
        auto lock = std::unique_lock<std::mutex>(mtx);
        cv_done.wait(lock, [this] { return remaining_workers == 0; });
        body = nullptr;
    }

  private:
    void worker_loop(const std::size_t my_id) noexcept {
        std::uint64_t last_gen = 0;
        while (true) {
            std::function<void(std::size_t)> local_body;
            {
                auto lock = std::unique_lock<std::mutex>(mtx);
                cv_start.wait(lock, [this, last_gen] {
                    return stopping || generation != last_gen;
                });
                last_gen = generation;
                if (stopping) {
                    return;
                }
                if (my_id >= active_workers) {
                    continue;
                }
                local_body = body;
            }
            local_body(my_id);
            {
                const auto lock = std::lock_guard<std::mutex>(mtx);
                if (--remaining_workers == 0) {
                    cv_done.notify_all();
                }
            }
        }
    }

    std::mutex mtx;
    std::condition_variable cv_start;
    std::condition_variable cv_done;
    std::function<void(std::size_t)> body;
    std::size_t active_workers = 0;
    std::size_t remaining_workers = 0;
    std::uint64_t generation = 0;
    bool stopping = false;
    std::vector<std::thread> workers;
};

}  // namespace NetworkAnalytical

EventQueue::EventQueue() noexcept : current_time(0) {
    // create empty event queue
    event_queue = std::map<EventTime, EventList>();
}

EventQueue::~EventQueue() noexcept {
    // worker_pool is destroyed here (unique_ptr), joining its worker threads.
    worker_pool.reset();

    if (!is_env_enabled("ASTRA_EVENT_QUEUE_STATS")) {
        return;
    }

    std::cerr << "[event-queue-stats]"
              << " schedule_calls=" << stats.schedule_calls
              << " new_event_lists=" << stats.new_event_lists
              << " proceed_calls=" << stats.proceed_calls
              << " drained_batches=" << stats.drained_batches
              << " drained_events=" << stats.drained_events
              << " serial_events=" << stats.serial_events
              << " parallel_safe_events=" << stats.parallel_safe_events
              << " parallel_runs=" << stats.parallel_runs
              << " parallel_events=" << stats.parallel_events
              << " parallel_groups=" << stats.parallel_groups
              << " max_queue_size=" << stats.max_queue_size
              << " max_batch_size=" << stats.max_batch_size
              << " max_parallel_groups=" << stats.max_parallel_groups
              << std::endl;
}

EventTime EventQueue::get_current_time() const noexcept {
    return current_time.load(std::memory_order_relaxed);
}

bool EventQueue::finished() const noexcept {
    // proceed()/schedule_event() from main thread are the only mutators once
    // simulation is running, and finished() is only called between proceed()
    // calls, so no synchronization is required.
    return event_queue.empty();
}

void EventQueue::proceed() noexcept {
    ++stats.proceed_calls;

    // proceed() runs on the main thread. Worker threads only mutate their
    // thread-local schedule buffer during invoke_event_batch, and flushback is
    // performed by the main thread after workers have joined. There is no
    // contention on event_queue here.
    while (true) {
        assert(!event_queue.empty());

        auto event_list_it = event_queue.begin();
        const auto event_time = event_list_it->first;
        const auto old_time = current_time.load(std::memory_order_relaxed);

        assert(event_time >= old_time);
        if (event_time > old_time) {
            current_time.store(event_time, std::memory_order_relaxed);
        }

        auto events = event_list_it->second.drain_events();
        if (events.empty()) {
            event_queue.erase(event_list_it);
            return;
        }

        ++stats.drained_batches;
        stats.drained_events += events.size();
        stats.max_batch_size = std::max(stats.max_batch_size, events.size());

        invoke_event_batch(events);
    }
}

void EventQueue::schedule_event(const EventTime event_time,
                                const Callback callback,
                                const CallbackArg callback_arg) noexcept {
    if (thread_local_schedule_buffer != nullptr && thread_local_schedule_buffer->owner == this) {
        thread_local_schedule_buffer->events.push_back({event_time, callback, callback_arg});
        return;
    }

    // Defensive fallback for any caller outside the main event loop.
    const auto lock = std::lock_guard<std::mutex>(schedule_mutex);
    schedule_event_locked(event_time, callback, callback_arg);
}

void EventQueue::register_parallel_safe_callback(const Callback callback) noexcept {
    assert(callback != nullptr);

    // Check for duplicates so repeated registrations (e.g. multiple Topology
    // constructions in a single process) do not grow the array unboundedly.
    const auto current_count = parallel_safe_callback_count.load(std::memory_order_acquire);
    for (std::size_t i = 0; i < current_count; ++i) {
        if (parallel_safe_callbacks[i].load(std::memory_order_relaxed) == callback) {
            return;
        }
    }

    const auto slot = parallel_safe_callback_count.fetch_add(1, std::memory_order_acq_rel);
    assert(slot < max_parallel_safe_callbacks);
    if (slot >= max_parallel_safe_callbacks) {
        // Silently drop if the whitelist overflows; readers fall back to the
        // serial path, which remains correct.
        parallel_safe_callback_count.store(max_parallel_safe_callbacks, std::memory_order_release);
        return;
    }
    parallel_safe_callbacks[slot].store(callback, std::memory_order_release);
}

void EventQueue::schedule_event_locked(const EventTime event_time,
                                       const Callback callback,
                                       const CallbackArg callback_arg) noexcept {
    // time should be at least larger than current time
    assert(event_time >= current_time.load(std::memory_order_relaxed));
    assert(callback != nullptr);
    ++stats.schedule_calls;

    auto event_list_it = event_queue.find(event_time);
    if (event_list_it == event_queue.end()) {
        event_list_it = event_queue.emplace(event_time, EventList(event_time)).first;
        ++stats.new_event_lists;
    }

    event_list_it->second.add_event(callback, callback_arg);
    stats.max_queue_size = std::max(stats.max_queue_size, event_queue.size());
}

void EventQueue::invoke_event_batch(std::vector<Event>& events) noexcept {
    auto scheduled_by_event = std::vector<std::vector<ScheduledEvent>>(events.size());

    auto invoke_serial_event = [this, &events, &scheduled_by_event](const std::size_t event_index) noexcept {
        auto buffer = ThreadLocalScheduleBuffer{this, {}};
        thread_local_schedule_buffer = &buffer;
        events[event_index].invoke_event();
        thread_local_schedule_buffer = nullptr;
        scheduled_by_event[event_index] = std::move(buffer.events);
    };

    std::size_t event_index = 0;
    while (event_index < events.size()) {
        const auto [callback, callback_arg] = events[event_index].get_handler_arg();
        (void)callback_arg;

        if (!is_parallel_safe_callback(callback)) {
            ++stats.serial_events;
            invoke_serial_event(event_index);
            ++event_index;
            continue;
        }

        const auto run_begin = event_index;
        while (event_index < events.size()) {
            const auto [run_callback, run_callback_arg] = events[event_index].get_handler_arg();
            (void)run_callback_arg;
            if (!is_parallel_safe_callback(run_callback)) {
                break;
            }
            ++event_index;
        }

        invoke_parallel_safe_run(events, run_begin, event_index, scheduled_by_event);
    }

    // Flushback is single-threaded: workers have joined by the time we reach
    // this point, so no lock is required to move their scheduled events into
    // the main queue.
    for (const auto& scheduled_events : scheduled_by_event) {
        for (const auto& event : scheduled_events) {
            schedule_event_locked(event.event_time, event.callback, event.callback_arg);
        }
    }
}

void EventQueue::ensure_worker_pool(const std::size_t worker_count) noexcept {
    if (worker_pool != nullptr && worker_pool_size >= worker_count) {
        return;
    }
    worker_pool = std::make_unique<WorkerPool>(worker_count);
    worker_pool_size = worker_count;
}

void EventQueue::invoke_parallel_safe_run(
    std::vector<Event>& events,
    const std::size_t begin,
    const std::size_t end,
    std::vector<std::vector<ScheduledEvent>>& scheduled_by_event) noexcept {
    assert(begin <= end);

    const auto min_events = read_size_env("ASTRA_EVENT_PARALLEL_MIN_EVENTS", 8);
    const auto hardware_threads = std::max<std::size_t>(1, std::thread::hardware_concurrency());
    const auto max_threads = std::min(
        read_size_env("ASTRA_EVENT_PARALLEL_THREADS", hardware_threads),
        hardware_threads);

    auto invoke_one = [this, &events, &scheduled_by_event](const std::size_t event_index) noexcept {
        auto buffer = ThreadLocalScheduleBuffer{this, {}};
        thread_local_schedule_buffer = &buffer;
        events[event_index].invoke_event();
        thread_local_schedule_buffer = nullptr;
        scheduled_by_event[event_index] = std::move(buffer.events);
    };

    auto run_serial = [&]() {
        for (auto event_index = begin; event_index < end; ++event_index) {
            ++stats.parallel_safe_events;
            invoke_one(event_index);
        }
    };

    if (end - begin < min_events || max_threads <= 1) {
        run_serial();
        return;
    }

    auto grouped_events = std::map<CallbackArg, std::vector<std::size_t>>();
    for (auto event_index = begin; event_index < end; ++event_index) {
        const auto [callback, callback_arg] = events[event_index].get_handler_arg();
        (void)callback;
        grouped_events[callback_arg].push_back(event_index);
    }

    if (grouped_events.size() <= 1) {
        run_serial();
        return;
    }

    auto groups = std::vector<std::vector<std::size_t>>();
    groups.reserve(grouped_events.size());
    for (auto& [callback_arg, group] : grouped_events) {
        (void)callback_arg;
        groups.emplace_back(std::move(group));
    }

    const auto thread_count = std::min(max_threads, groups.size());
    ++stats.parallel_runs;
    stats.parallel_events += end - begin;
    stats.parallel_safe_events += end - begin;
    stats.parallel_groups += groups.size();
    stats.max_parallel_groups = std::max(stats.max_parallel_groups, groups.size());

    auto next_group_index = std::atomic<std::size_t>(0);
    auto work = [&groups, &next_group_index, &invoke_one](const std::size_t /*worker_id*/) noexcept {
        while (true) {
            const auto group_index = next_group_index.fetch_add(1, std::memory_order_relaxed);
            if (group_index >= groups.size()) {
                return;
            }
            for (const auto event_index : groups[group_index]) {
                invoke_one(event_index);
            }
        }
    };

    // thread_count includes the main thread, so we dispatch thread_count - 1
    // workers and have the main thread join the work loop as well. The pool
    // is always sized to max_threads - 1 so batch-to-batch fluctuation in
    // group count never triggers a pool rebuild.
    const auto pool_size = max_threads - 1;
    const auto worker_count = thread_count - 1;
    if (worker_count > 0) {
        ensure_worker_pool(pool_size);
        worker_pool->dispatch(work, worker_count);
    }
    work(thread_count - 1);
    if (worker_count > 0) {
        worker_pool->wait();
    }
}

bool EventQueue::is_parallel_safe_callback(const Callback callback) noexcept {
    const auto count = parallel_safe_callback_count.load(std::memory_order_acquire);
    for (std::size_t i = 0; i < count; ++i) {
        if (parallel_safe_callbacks[i].load(std::memory_order_relaxed) == callback) {
            return true;
        }
    }
    return false;
}
