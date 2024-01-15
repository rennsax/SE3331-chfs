#ifndef RAFT_TIMER_H
#define RAFT_TIMER_H
#include <chrono>
#include <memory>
#include <random>

namespace chfs {

class Timer {
    using Duration = std::chrono::duration<std::int64_t, std::milli>;

public:
    Timer(const Duration &timeout)
        : begin_time{std::chrono::system_clock::now()}, timeout(timeout) {
    }

    Timer(int timeout) : Timer(Duration{timeout}) {
    }

    /**
     * Check if the function has timed out.
     *
     * @return true if the function has timed out, false otherwise
     */
    bool is_timeout() const noexcept {
        return (std::chrono::system_clock::now() - begin_time >=
                this->get_timeout());
    }

    void start() noexcept {
        this->begin_time = std::chrono::system_clock::now();
    }

    void start(const Duration &timeout) {
        this->timeout = timeout;
        this->begin_time = std::chrono::system_clock::now();
    }

    Duration get_timeout() const noexcept {
        return timeout;
    }

    ~Timer() = default;

    Timer(const Timer &) = delete;

    Timer &operator=(const Timer &) = delete;

private:
    std::chrono::time_point<std::chrono::system_clock> begin_time;
    Duration timeout;
};

static int generate_random_timeout_number(int least, int max) noexcept {
    static std::random_device random_device;
    static std::mt19937 engine{random_device()};
    static std::uniform_int_distribution<> dist(least, max);
    return dist(engine);
}

std::unique_ptr<Timer> make_election_timer() noexcept {
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    return std::make_unique<Timer>(generate_random_timeout_number(300, 500));
}

std::unique_ptr<Timer> make_heartbeat_timer() noexcept {
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
    return std::make_unique<Timer>(generate_random_timeout_number(50, 100));
}

} // namespace chfs

#endif // RAFT_TIMER_H