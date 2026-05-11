# MPMC Ring Buffer

![CI](https://github.com/mhaykarmeni/MPMCRingBuffer/actions/workflows/ci.yml/badge.svg)

A bounded, lock-free Multi-Producer Multi-Consumer (MPMC) ring buffer in modern C++20 using Vyukov's sequence number algorithm.

---

## Goal

Demonstrate production-quality concurrent queue design for systems requiring multiple concurrent producers and consumers: correct CAS-based slot claiming, sequence number coordination to avoid ABA, cache-friendly layout, and measurable throughput across varying producer/consumer thread counts.

---

## Implementation

### `MPMCQueue<T, N>` — Lock-Free MPMC

A fixed-capacity, lock-free queue supporting any number of concurrent producers and consumers. Uses Dmitry Vyukov's sequence number algorithm: each slot carries an atomic sequence number that coordinates producers and consumers without locks or ABA issues.

```cpp
template<typename T, std::size_t N>
class MPMCQueue { ... };
```

**Template parameters:**
- `T` — element type (should be trivially copyable for best performance)
- `N` — capacity; must be a power of 2 (enforced with `static_assert`)

---

## API

### 1. `bool try_push(const T& value)` / `bool try_push(T&& value)`
- Enqueues one element. Returns `true` on success, `false` if full.
- Safe to call concurrently from multiple producer threads.

### 2. `std::optional<T> try_pop()`
- Dequeues one element. Returns the value or `std::nullopt` if empty.
- Safe to call concurrently from multiple consumer threads.

### 3. `bool empty() const`
- Returns an approximate snapshot of whether the queue is empty.

### 4. `bool full() const`
- Returns an approximate snapshot of whether the queue is full.

### 5. `std::size_t size() const`
- Returns an approximate element count.

---

## Design Features

- **Vyukov's sequence number algorithm** — each slot carries an `std::atomic<std::size_t> sequence`. Producers claim slots by CAS on `m_tail` and signal readiness by writing `sequence = tail + 1`. Consumers claim slots by CAS on `m_head` and recycle slots by writing `sequence = head + N`. Eliminates the ABA problem without hazard pointers or epochs.
- **CAS-based slot claiming** — multiple producers and consumers compete via `compare_exchange_weak` on `m_head` / `m_tail`; only one thread wins each slot.
- **Cache line isolation** — `m_head` and `m_tail` on separate `alignas(kCacheLine)` cache lines to prevent false sharing between producers and consumers.
- **Index wrapping** — bitmask `index & (N - 1)` instead of modulo; requires N to be a power of 2.
- **No heap allocation** — internal buffer is a plain `std::array<Slot, N>`.

---

## How It Compares to SPSC

| Property | `SPSCQueueLF` | `MPMCQueue` |
|----------|--------------|-------------|
| Producers | 1 | N |
| Consumers | 1 | N |
| Synchronization | Atomic load/store | CAS |
| ABA concern | None (ownership separation) | Handled by sequence numbers |
| Throughput (1P1C) | Higher | Lower (CAS overhead) |

---

## File Structure

```
MPMCRingBuffer/
├── src/
│   └── mpmc_queue.h         # header-only implementation
├── tests/
│   └── mpmc_queue_test.cpp  # Google Test suite
├── bench/
│   └── mpmc_queue_bench.cpp # Google Benchmark suite
├── src/main.cpp
├── CMakeLists.txt
└── README.md
```

---

## Tests (`tests/mpmc_queue_test.cpp`)

| Test | What it verifies |
|------|-----------------|
| `PushPop_SingleElement` | push one, pop one, correct value |
| `PushPop_FullCapacity` | fill to capacity, drain, all values correct and in order |
| `TryPush_WhenFull_ReturnsFalse` | push N elements, next push returns false |
| `TryPop_WhenEmpty_ReturnsNullopt` | pop on empty queue returns `std::nullopt` |
| `FIFO_Order` | elements come out in the same order they went in (single-threaded) |
| `WrapAround` | push/pop more than N elements total, verify wrap-around correctness |
| `Size` | size() reflects pushes and pops correctly |
| `EmptyAndFull` | empty() and full() return correct states |
| `Concurrent_1P1C` | 1 producer + 1 consumer, 65536 items, verify no loss |
| `Concurrent_2P2C` | 2 producers + 2 consumers, verify all items received exactly once |
| `Concurrent_4P4C` | 4 producers + 4 consumers, verify all items received exactly once |

---

## Benchmarks (`bench/mpmc_queue_bench.cpp`)

| Benchmark | What it measures |
|-----------|-----------------|
| `BM_Throughput_1P1C` | 1 producer + 1 consumer sustained throughput |
| `BM_Throughput_2P2C` | 2 producers + 2 consumers sustained throughput |
| `BM_Throughput_4P4C` | 4 producers + 4 consumers sustained throughput |
| `BM_Latency_RTT` | round-trip time: producer pushes, consumer pops and pushes back |
| `BM_SPSC_Throughput_1P1C` | `SPSCQueueLF` 1P1C baseline for direct comparison |
| `BM_SPSC_Latency_RTT` | `SPSCQueueLF` RTT baseline |

### Results (Intel Core Ultra 7 155U, 19GB RAM, 2.7GHz, Ubuntu 24.04 WSL2, GCC 14.2, Release build)

*To be filled after implementation.*

---

## Build Setup

Uses CMake with:
- C++20 standard
- Google Test (via FetchContent)
- Google Benchmark (via FetchContent)
- `MPMC_BUILD_BENCH=OFF` option to skip benchmark build (used for TSan builds)

---

## Building & Testing

### Standard build
```bash
cmake -S . -B build
cmake --build build
cd build && ctest --output-on-failure
```

### Release build (for accurate benchmarks)
```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release
./build-release/mpmc_bench
```

### TSan build (data race detection)

> **WSL2 users:** run `sudo sysctl vm.mmap_rnd_bits=28` once before the TSan build.

```bash
cmake -S . -B build-tsan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_FLAGS="-fsanitize=thread" \
    -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread" \
    -DMPMC_BUILD_BENCH=OFF
cmake --build build-tsan
cd build-tsan && ctest --output-on-failure
```

Expected: all tests pass, zero data race reports.

---

## Acceptance Criteria

- [ ] All tests pass under TSan with zero data race reports
- [ ] Lock-free throughput > 100M ops/sec on modern hardware (1P1C)
- [ ] `try_push` / `try_pop` contain zero heap allocations
- [ ] Code compiles clean under `-Wall -Wextra -Wpedantic` with no warnings
