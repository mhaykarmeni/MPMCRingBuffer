# MPMC Ring Buffer

![CI](https://github.com/mhaykarmeni/MPMCRingBuffer/actions/workflows/ci.yml/badge.svg)

A bounded, lock-free Multi-Producer Multi-Consumer (MPMC) ring buffer in modern C++20 using Vyukov's sequence number algorithm, with a mutex-based reference implementation for comparison.

---

## Goal

Demonstrate production-quality concurrent queue design for systems requiring multiple concurrent producers and consumers: correct CAS-based slot claiming, sequence number coordination to avoid ABA, cache-friendly layout, and measurable throughput across varying producer/consumer thread counts.

---

## Implementations

### `MPMCQueueLF<T, N>` — Lock-Free MPMC

A fixed-capacity, lock-free queue supporting any number of concurrent producers and consumers. Uses Dmitry Vyukov's sequence number algorithm: each slot carries an atomic sequence number that coordinates producers and consumers without locks or ABA issues.

```cpp
template<typename T, std::size_t N>
class MPMCQueueLF { ... };
```

### `MPMCQueueMtx<T, N>` — Mutex-Based MPMC

Same API, same ring buffer layout, but all access is serialized through a single `std::mutex`. Simpler implementation — serves as a correctness baseline and throughput comparison target.

```cpp
template<typename T, std::size_t N>
class MPMCQueueMtx { ... };
```

**Template parameters (both classes):**
- `T` — element type (should be trivially copyable for best performance)
- `N` — capacity; must be a power of 2 (enforced with `static_assert`)

---

## API

Both classes expose identical interfaces:

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

## Vyukov's Sequence Number Algorithm

The core problem in MPMC queues: after winning a CAS to claim a slot index, a thread has no way to know whether the previous occupant has finished with that slot. Without coordination:

- A consumer can read a slot a producer hasn't finished writing yet → reads garbage
- A producer can write a slot a consumer is still reading → data race, undefined behavior

The sequence number on each slot solves both. It acts as a per-slot state machine that tells every thread exactly who owns the slot right now.

### Slot lifecycle

```
seq = i        → ready for producer (empty, lap 0)
seq = i + 1    → ready for consumer (producer finished writing)
seq = i + N    → ready for producer again (consumer finished reading, lap 1)
...
```

### Producer (`try_push`)

```
tail = m_tail.load(relaxed)
loop:
    slot = buffer[tail & (N-1)]
    seq  = slot.sequence.load(acquire)
    diff = (ptrdiff_t)seq - (ptrdiff_t)tail   // signed!

    if diff == 0:    slot is ready → CAS(m_tail, tail, tail+1) to claim
                     write value; slot.sequence = tail+1  (signals consumer)
                     return true
    if diff < 0:     consumer hasn't recycled yet → FULL, return false
    if diff > 0:     another producer claimed this tail → reload, retry
```

### Consumer (`try_pop`)

```
head = m_head.load(relaxed)
loop:
    slot = buffer[head & (N-1)]
    seq  = slot.sequence.load(acquire)
    diff = (ptrdiff_t)seq - (ptrdiff_t)(head+1)

    if diff == 0:    producer finished writing → CAS(m_head, head, head+1) to claim
                     read value; slot.sequence = head+N  (recycles for next lap)
                     return value
    if diff < 0:     producer hasn't written yet → EMPTY, return nullopt
    if diff > 0:     another consumer claimed this head → reload, retry
```

### Why this eliminates ABA

Classic lock-free structures using pointer CAS suffer ABA: a pointer can be freed and reallocated at the same address, making a CAS succeed on stale state. Here no pointers are CAS'd — only monotonically increasing counters. A slot cannot appear to be in an earlier generation because the sequence number strictly increases each lap.

### Why the diff is signed

`m_tail` and `m_head` are `size_t` (unsigned) and increment forever. The subtraction `seq - tail` is cast to `ptrdiff_t` so underflow produces a meaningful negative — without the cast, a behind-schedule slot would give a huge positive number and "full" would never be detected.

---

## Design Features (`MPMCQueueLF`)

- **Vyukov's sequence number algorithm** — eliminates ABA and coordinates producers/consumers without locks
- **CAS-based slot claiming** — `compare_exchange_weak` on `m_head`/`m_tail`; only one thread wins each slot
- **Cache line isolation** — `m_head`, `m_tail`, and `m_buffer` on separate `alignas(64)` cache lines to prevent false sharing
- **Bitmask index wrapping** — `tail & (N-1)` instead of `tail % N`; requires N to be a power of 2
- **No heap allocation** — internal buffer is a plain `std::array<Slot, N>`

---

## How `MPMCQueueLF` Compares to `MPMCQueueMtx`

| Property | `MPMCQueueLF` | `MPMCQueueMtx` |
|----------|--------------|----------------|
| Synchronization | CAS + sequence numbers | Single `std::mutex` |
| Scalability | Producers/consumers work in parallel per-slot | All threads serialize through one lock |
| Latency | Near-zero on uncontended path | Syscall overhead on every op |
| ABA protection | Sequence numbers | Not needed (mutex excludes all races) |
| Code complexity | Higher | Lower |

---

## File Structure

```
MPMCRingBuffer/
├── src/
│   ├── mpmc_queue.h         # header-only: MPMCQueueLF + MPMCQueueMtx
│   └── main.cpp             # smoke test / demo
├── tests/
│   └── mpmc_queue_test.cpp  # Google Test suite (19 tests)
├── bench/
│   └── mpmc_queue_bench.cpp # Google Benchmark suite
├── CMakeLists.txt
└── README.md
```

---

## Tests (`tests/mpmc_queue_test.cpp`)

### `MPMCQueueLF`

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

### `MPMCQueueMtx`

| Test | What it verifies |
|------|-----------------|
| `PushPop_SingleElement` | push one, pop one, correct value |
| `TryPush_WhenFull_ReturnsFalse` | push N elements, next push returns false |
| `TryPop_WhenEmpty_ReturnsNullopt` | pop on empty queue returns `std::nullopt` |
| `FIFO_Order` | elements come out in the same order they went in |
| `EmptyAndFull` | empty() and full() return correct states |
| `Concurrent_1P1C` | 1 producer + 1 consumer, 65536 items, verify no loss |
| `Concurrent_2P2C` | 2 producers + 2 consumers, verify all items received exactly once |
| `Concurrent_4P4C` | 4 producers + 4 consumers, verify all items received exactly once |

---

## Benchmarks (`bench/mpmc_queue_bench.cpp`)

| Benchmark | What it measures |
|-----------|-----------------|
| `BM_LF/Mtx_Throughput_1P1C` | 1 producer + 1 consumer — baseline, most common config |
| `BM_LF/Mtx_Throughput_2P1C` | 2 producers + 1 consumer — fan-in, light |
| `BM_LF/Mtx_Throughput_2P2C` | 2 producers + 2 consumers — symmetric, balanced pipeline |
| `BM_LF/Mtx_Throughput_4P1C` | 4 producers + 1 consumer — fan-in, worker pool → serializer |
| `BM_LF/Mtx_Throughput_1P4C` | 1 producer + 4 consumers — fan-out, dispatcher → worker pool |
| `BM_LF/Mtx_Latency_RTT` | round-trip time: producer pushes, consumer pops and pushes back |

Benchmarks cover the top-5 most common real-world producer/consumer configurations.

### Results (Intel Core Ultra 7 155U, 16 cores @ 2.0GHz, Ubuntu 24.04 WSL2, GCC 14.1, Release build)

| Rank | Scenario | Producers | Consumers | `MPMCQueueLF` (M ops/sec) | `MPMCQueueMtx` (M ops/sec) | LF speedup |
|:----:|----------|:---------:|:---------:|:-------------------------:|:--------------------------:|:----------:|
| 1    | 1P1C     | 1         | 1         | **63.1**                  | 7.2                        | **8.7x**   |
| 2    | 2P1C     | 2         | 1         | **13.9**                  | 7.4                        | 1.9x       |
| 3    | 1P4C     | 1         | 4         | **6.7**                   | 3.7                        | 1.8x       |
| 4    | 2P2C     | 2         | 2         | **10.1**                  | 7.2                        | 1.4x       |
| 5    | 4P1C     | 4         | 1         | **6.4**                   | 3.3                        | 1.9x       |
| —    | RTT      | 1         | 1         | **205 ns**                | 2429 ns                    | **11.8x**  |

#### Key observations

- **1P1C**: lock-free wins 8.7x — no contention, CAS succeeds on the first try almost every time, zero syscall overhead
- **Fan-in (2P1C, 4P1C)**: lock-free wins ~2x — producers race only on `m_tail`, the single consumer never contends on `m_head`; mutex pays the lock cost on every pop regardless
- **Fan-out (1P4C)**: symmetric advantage ~1.8x — single producer never contends on `m_tail`, consumers race on `m_head` via CAS rather than sleeping on a mutex
- **RTT latency**: 11.8x advantage — mutex requires a kernel transition on every lock/unlock; CAS stays entirely in userspace

#### How to reach 100M+ ops/sec (1P1C)

The current 63M ops/sec ceiling is imposed by the environment, not the algorithm. These changes would push past 100M:

| Technique | What it does | Expected gain |
|-----------|-------------|---------------|
| **Bare metal Linux** | Eliminates WSL2 virtualization overhead and scheduler jitter | 1.5–2x |
| **CPU pinning** (`pthread_setaffinity_np`) | Locks producer and consumer to dedicated cores — no OS migrations, cache stays warm | significant on multi-socket |
| **`MADV_HUGEPAGE`** on the buffer | Reduces TLB misses when the buffer spans many pages | 10–20% |
| **`T` = cache-line-sized struct** | Avoids false sharing between adjacent slots when `sizeof(T) < 64` | workload dependent |
| **Padding slots to cache line** | Each `Slot` on its own cache line — eliminates false sharing between adjacent slots entirely | 20–40% on write-heavy workloads |

The single most impactful change is running on bare metal with CPU pinning — WSL2's virtualized scheduler alone accounts for most of the gap between 63M and 100M+.

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
./build/mpmc_tests
```

### Release build (for accurate benchmarks)
```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DMPMC_BUILD_BENCH=ON
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
./build-tsan/mpmc_tests
```

Expected: all tests pass, zero data race reports.

---

## Acceptance Criteria

- [x] All 19 tests pass under TSan with zero data race reports
- [x] Lock-free throughput > 50M ops/sec on modern hardware (1P1C) — achieved 63M ops/sec on WSL2 (virtualized scheduler, no CPU pinning; expect higher on bare metal)
- [x] `try_push` / `try_pop` contain zero heap allocations
- [x] Code compiles clean under `-Wall -Wextra -Wpedantic` with no warnings
