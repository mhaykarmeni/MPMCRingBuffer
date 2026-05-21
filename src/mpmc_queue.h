#pragma once
#include <atomic>
#include <optional>
#include <array>
#include <cstddef>
#include <mutex>

inline constexpr std::size_t kCacheLine = 64;

template <typename T, std::size_t N>
class MPMCQueueLF {
    static_assert(N >= 2 && (N & (N - 1)) == 0, "N must be a power of 2");
public:
    MPMCQueueLF() {
        for (std::size_t i = 0; i < N; ++i)
            m_buffer[i].m_sequence.store(i, std::memory_order_relaxed);
    }
    MPMCQueueLF(const MPMCQueueLF&) = delete;
    MPMCQueueLF& operator=(const MPMCQueueLF&) = delete;
    MPMCQueueLF(MPMCQueueLF&&) = delete;
    MPMCQueueLF& operator=(MPMCQueueLF&&) = delete;

    template<typename U>
    bool try_push(U&& val) {
        std::size_t tail = m_tail.load(std::memory_order_relaxed);

        while (true) {
            Slot& slot = m_buffer[tail & (N - 1)];
            std::size_t seq = slot.m_sequence.load(std::memory_order_acquire);
            std::ptrdiff_t diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(tail);

            if (diff == 0) {
                if (m_tail.compare_exchange_weak(tail, tail + 1, std::memory_order_relaxed))  {
                    slot.m_value = std::forward<U>(val);
                    slot.m_sequence.store(tail + 1, std::memory_order_release);
                    return true;
                }
            } 
            else if (diff < 0) {
                return false;  // full
            } 
            else {
                tail = m_tail.load(std::memory_order_relaxed);  // another producer advanced tail, retry
            }
        }
    }

    std::optional<T> try_pop() {
        std::size_t head = m_head.load(std::memory_order_relaxed);

        while (true) {
            Slot& slot = m_buffer[head & (N - 1)];
            std::size_t seq = slot.m_sequence.load(std::memory_order_acquire);
            std::ptrdiff_t diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(head) - 1;

            if (diff == 0) {
                if (m_head.compare_exchange_weak(head, head + 1, std::memory_order_relaxed)) {
                    T tmpVal = slot.m_value;
                    slot.m_sequence.store(head + N, std::memory_order_release);
                    return std::optional<T>(std::move(tmpVal));
                }
            }
            else if (diff < 0) {
                return std::nullopt;
            }
            else {
                head = m_head.load(std::memory_order_relaxed);
            }
        }
    }

    bool empty() const noexcept {
        return m_tail.load(std::memory_order_relaxed) == m_head.load(std::memory_order_relaxed);
    }

    bool full() const noexcept {
        return m_tail.load(std::memory_order_relaxed) - m_head.load(std::memory_order_relaxed) == N;
    }

    std::size_t size() const noexcept {
        const std::size_t tail = m_tail.load(std::memory_order_relaxed); 
        const std::size_t head = m_head.load(std::memory_order_relaxed);
        //if m_head is loaded after m_tail advances and a consumer races in between, you could get m_head > m_tail momentarily
        return tail > head ? tail - head : 0;
    }

private:
    struct Slot {
        std::atomic<std::size_t> m_sequence;
        T m_value;
    };

    alignas(kCacheLine) std::array<Slot, N> m_buffer;
    alignas(kCacheLine) std::atomic<std::size_t> m_tail{0};
    alignas(kCacheLine) std::atomic<std::size_t> m_head{0};
};

// ── Mutex-based MPMC queue ───────────────────────────────────────────────────

template <typename T, std::size_t N>
class MPMCQueueMtx {
    static_assert(N >= 2 && (N & (N - 1)) == 0, "N must be a power of 2");
public:
    MPMCQueueMtx() = default;
    MPMCQueueMtx(const MPMCQueueMtx&) = delete;
    MPMCQueueMtx& operator=(const MPMCQueueMtx&) = delete;
    MPMCQueueMtx(MPMCQueueMtx&&) = delete;
    MPMCQueueMtx& operator=(MPMCQueueMtx&&) = delete;

    template<typename U>
    bool try_push(U&& val) {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_size == N) return false;
        m_buffer[m_tail & (N - 1)] = std::forward<U>(val);
        ++m_tail;
        ++m_size;
        return true;
    }

    std::optional<T> try_pop() {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_size == 0) return std::nullopt;
        T val = std::move(m_buffer[m_head & (N - 1)]);
        ++m_head;
        --m_size;
        return val;
    }

    bool empty() const noexcept {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_size == 0;
    }

    bool full() const noexcept {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_size == N;
    }

    std::size_t size() const noexcept {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_size;
    }

private:
    std::array<T, N>  m_buffer{};
    std::size_t       m_head{0};
    std::size_t       m_tail{0};
    std::size_t       m_size{0};
    mutable std::mutex m_mutex;
};
