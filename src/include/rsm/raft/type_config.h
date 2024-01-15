#ifndef RAFT_TYPE_CONFIG_H
#define RAFT_TYPE_CONFIG_H
#include <cstdint>
#include <functional>
#include <mutex>
#include <utility>

namespace chfs {
using RaftNodeId = int;
using RaftTermNumber = uint32_t;
using RaftLogIndex = uint32_t;

constexpr RaftNodeId KRaftNilNodeId = -1;
constexpr RaftLogIndex KRaftDefaultLogIndex = 0;
constexpr RaftLogIndex KRaftFallbackTermNumber = 0;

constexpr std::chrono::milliseconds KRaftThreadSleepingSlice{30};
constexpr std::chrono::milliseconds KRaftHeartbeatInterval{50};
constexpr std::chrono::microseconds KRaftRequestTimeout{200};

struct [[deprecated]] RaftRAII {

    RaftRAII(std::mutex &mtx, std::function<void()> cleanup)
        : lock(mtx), cleanup(std::move(cleanup)) {
    }

    ~RaftRAII() {
        if (cleanup) {
            std::invoke(cleanup);
        }
    }

    RaftRAII(const RaftRAII &) = delete;
    RaftRAII &operator=(const RaftRAII &) = delete;

private:
    std::lock_guard<std::mutex> lock;
    std::function<void()> cleanup;
};

template <typename _Product, typename _Supplier>
struct [[deprecated]] RaftProducer : RaftRAII {

    using produce_type = _Product;
    using supplier_type = _Supplier;

    RaftProducer(std::mutex &mtx, supplier_type _producer,
                 std::function<void()> cleanup)
        : RaftRAII(mtx, std::move(cleanup)), producer(std::move(_producer)) {
    }

    RaftProducer(std::mutex &mtx, supplier_type _producer)
        : RaftProducer{mtx, _producer, {}} {
    }

    template <typename... Args>
    auto get(Args &&...args) const
        -> decltype(std::enable_if_t<std::is_same_v<
                        produce_type,
                        std::invoke_result_t<supplier_type, Args...>>>(),
                    produce_type()) {
        return std::invoke(producer, std::forward<Args>(args)...);
    }

    ~RaftProducer() = default;

    RaftProducer(const RaftProducer &) = delete;
    RaftProducer &operator=(const RaftProducer &) = delete;

private:
    supplier_type producer;
};

} // namespace chfs

#endif // RAFT_TYPE_CONFIG_H