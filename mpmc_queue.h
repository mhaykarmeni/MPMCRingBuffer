#include <atomic>
#include <optional>
#include <array>
#include <cstddef>

inline constexpr std::size_t kCacheLine = std::hardware_destructive_interference_size;

template <typename T, std::size_t N>
class MPMCQueueLF {
    static_assert(N >= 2 && (N & (N - 1)) == 0, "N must be a power of 2");
public:
    MPMCQueueLF() {
        for (std::size_t i = 0; i < N; ++i)
            m_buffer[i].sequence.store(i, std::memory_order_relaxed);
    }
    MPMCQueueLF(const MPMCQueueLF&) = delete;
    MPMCQueueLF& operator=(const MPMCQueueLF&) = delete;
    MPMCQueueLF(MPMCQueueLF&&) = delete;
    MPMCQueueLF& operator=(MPMCQueueLF&&) = delete;

    template<typename U>
    bool try_push(U&& val) {
    }

    std::optional<T> try_pop() {
        
    }

    bool empty() const noexcept {
    }

    bool full() const noexcept {
    }

    std::size_t size() const noexcept {
    }

private:
    struct Slot {
        std::atomic<std::size_t> sequence;
        T value;
    };

    alignas(kCacheLine) std::array<Slot, N> m_buffer;
    alignas(kCacheLine) std::atomic<std::size_t> m_tail{0};
    alignas(kCacheLine) std::atomic<std::size_t> m_head{0};
};