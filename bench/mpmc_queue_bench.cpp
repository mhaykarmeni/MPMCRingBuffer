#include <benchmark/benchmark.h>
#include <thread>
#include <atomic>
#include <vector>

#include "mpmc_queue.h"

// Top-5 real-world configs: 1P1C, 2P1C, 2P2C, 4P1C, 1P4C

static void run_throughput(benchmark::State& state, int num_producers, int num_consumers) {
    MPMCQueueLF<int, 1024> q;

    for (auto _ : state) {
        std::atomic<bool> start{false};
        const int total = 100000;

        auto producer = [&]() {
            while (!start.load(std::memory_order_acquire));
            for (int i = 0; i < total / num_producers; ++i)
                while (!q.try_push(i));
        };

        auto consumer = [&]() {
            while (!start.load(std::memory_order_acquire));
            int count = 0;
            while (count < total / num_consumers) {
                if (q.try_pop()) ++count;
            }
        };

        std::vector<std::thread> threads;
        for (int i = 0; i < num_producers; ++i) threads.emplace_back(producer);
        for (int i = 0; i < num_consumers; ++i) threads.emplace_back(consumer);
        start.store(true, std::memory_order_release);
        for (auto& t : threads) t.join();
    }

    state.SetItemsProcessed(state.iterations() * 100000);
}

static void run_throughput_mtx(benchmark::State& state, int num_producers, int num_consumers) {
    MPMCQueueMtx<int, 1024> q;

    for (auto _ : state) {
        std::atomic<bool> start{false};
        const int total = 100000;

        auto producer = [&]() {
            while (!start.load(std::memory_order_acquire));
            for (int i = 0; i < total / num_producers; ++i)
                while (!q.try_push(i));
        };

        auto consumer = [&]() {
            while (!start.load(std::memory_order_acquire));
            int count = 0;
            while (count < total / num_consumers) {
                if (q.try_pop()) ++count;
            }
        };

        std::vector<std::thread> threads;
        for (int i = 0; i < num_producers; ++i) threads.emplace_back(producer);
        for (int i = 0; i < num_consumers; ++i) threads.emplace_back(consumer);
        start.store(true, std::memory_order_release);
        for (auto& t : threads) t.join();
    }

    state.SetItemsProcessed(state.iterations() * 100000);
}

static void BM_LF_Throughput_1P1C(benchmark::State& s) { run_throughput(s, 1, 1); }
static void BM_LF_Throughput_2P1C(benchmark::State& s) { run_throughput(s, 2, 1); }
static void BM_LF_Throughput_2P2C(benchmark::State& s) { run_throughput(s, 2, 2); }
static void BM_LF_Throughput_4P1C(benchmark::State& s) { run_throughput(s, 4, 1); }
static void BM_LF_Throughput_1P4C(benchmark::State& s) { run_throughput(s, 1, 4); }

static void BM_LF_Latency_RTT(benchmark::State& state) {
    MPMCQueueLF<int, 4> request_q;
    MPMCQueueLF<int, 4> reply_q;

    std::atomic<bool> running{true};
    std::thread consumer([&]() {
        while (running.load(std::memory_order_relaxed)) {
            if (auto v = request_q.try_pop())
                while (!reply_q.try_push(*v));
        }
    });

    for (auto _ : state) {
        while (!request_q.try_push(1));
        while (!reply_q.try_pop());
    }

    running.store(false, std::memory_order_relaxed);
    request_q.try_push(0);
    consumer.join();
}

static void BM_Mtx_Throughput_1P1C(benchmark::State& s) { run_throughput_mtx(s, 1, 1); }
static void BM_Mtx_Throughput_2P1C(benchmark::State& s) { run_throughput_mtx(s, 2, 1); }
static void BM_Mtx_Throughput_2P2C(benchmark::State& s) { run_throughput_mtx(s, 2, 2); }
static void BM_Mtx_Throughput_4P1C(benchmark::State& s) { run_throughput_mtx(s, 4, 1); }
static void BM_Mtx_Throughput_1P4C(benchmark::State& s) { run_throughput_mtx(s, 1, 4); }

static void BM_Mtx_Latency_RTT(benchmark::State& state) {
    MPMCQueueMtx<int, 4> request_q;
    MPMCQueueMtx<int, 4> reply_q;

    std::atomic<bool> running{true};
    std::thread consumer([&]() {
        while (running.load(std::memory_order_relaxed)) {
            if (auto v = request_q.try_pop())
                while (!reply_q.try_push(*v));
        }
    });

    for (auto _ : state) {
        while (!request_q.try_push(1));
        while (!reply_q.try_pop());
    }

    running.store(false, std::memory_order_relaxed);
    request_q.try_push(0);
    consumer.join();
}

BENCHMARK(BM_LF_Throughput_1P1C)->UseRealTime();
BENCHMARK(BM_LF_Throughput_2P1C)->UseRealTime();
BENCHMARK(BM_LF_Throughput_2P2C)->UseRealTime();
BENCHMARK(BM_LF_Throughput_4P1C)->UseRealTime();
BENCHMARK(BM_LF_Throughput_1P4C)->UseRealTime();
BENCHMARK(BM_LF_Latency_RTT);

BENCHMARK(BM_Mtx_Throughput_1P1C)->UseRealTime();
BENCHMARK(BM_Mtx_Throughput_2P1C)->UseRealTime();
BENCHMARK(BM_Mtx_Throughput_2P2C)->UseRealTime();
BENCHMARK(BM_Mtx_Throughput_4P1C)->UseRealTime();
BENCHMARK(BM_Mtx_Throughput_1P4C)->UseRealTime();
BENCHMARK(BM_Mtx_Latency_RTT);

BENCHMARK_MAIN();
