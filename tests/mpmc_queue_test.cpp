#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <numeric>
#include <algorithm>

#include "mpmc_queue.h"

// ── Single-threaded ──────────────────────────────────────────────────────────

TEST(MPMCQueueLF, PushPop_SingleElement) {
    MPMCQueueLF<int, 4> q;
    EXPECT_TRUE(q.try_push(42));
    auto v = q.try_pop();
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, 42);
}

TEST(MPMCQueueLF, PushPop_FullCapacity) {
    MPMCQueueLF<int, 8> q;
    for (int i = 0; i < 8; ++i)
        EXPECT_TRUE(q.try_push(i));
    for (int i = 0; i < 8; ++i) {
        auto v = q.try_pop();
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(*v, i);
    }
}

TEST(MPMCQueueLF, TryPush_WhenFull_ReturnsFalse) {
    MPMCQueueLF<int, 4> q;
    for (int i = 0; i < 4; ++i)
        EXPECT_TRUE(q.try_push(i));
    EXPECT_FALSE(q.try_push(99));
}

TEST(MPMCQueueLF, TryPop_WhenEmpty_ReturnsNullopt) {
    MPMCQueueLF<int, 4> q;
    EXPECT_EQ(q.try_pop(), std::nullopt);
}

TEST(MPMCQueueLF, FIFO_Order) {
    MPMCQueueLF<int, 8> q;
    for (int i = 0; i < 8; ++i)
        q.try_push(i);
    for (int i = 0; i < 8; ++i)
        EXPECT_EQ(*q.try_pop(), i);
}

TEST(MPMCQueueLF, WrapAround) {
    MPMCQueueLF<int, 4> q;
    for (int round = 0; round < 8; ++round) {
        for (int i = 0; i < 4; ++i)
            EXPECT_TRUE(q.try_push(round * 4 + i));
        for (int i = 0; i < 4; ++i)
            EXPECT_EQ(*q.try_pop(), round * 4 + i);
    }
}

TEST(MPMCQueueLF, Size) {
    MPMCQueueLF<int, 8> q;
    EXPECT_EQ(q.size(), 0u);
    q.try_push(1);
    EXPECT_EQ(q.size(), 1u);
    q.try_push(2);
    EXPECT_EQ(q.size(), 2u);
    q.try_pop();
    EXPECT_EQ(q.size(), 1u);
    q.try_pop();
    EXPECT_EQ(q.size(), 0u);
}

TEST(MPMCQueueLF, EmptyAndFull) {
    MPMCQueueLF<int, 4> q;
    EXPECT_TRUE(q.empty());
    EXPECT_FALSE(q.full());
    for (int i = 0; i < 4; ++i)
        q.try_push(i);
    EXPECT_FALSE(q.empty());
    EXPECT_TRUE(q.full());
    q.try_pop();
    EXPECT_FALSE(q.full());
}

// ── Concurrent ───────────────────────────────────────────────────────────────

static void run_concurrent_test(int num_producers, int num_consumers, int total_items) {
    MPMCQueueLF<int, 1024> q;

    std::atomic<int> next_item{0};
    std::atomic<int> received_count{0};
    std::atomic<long long> received_sum{0};

    auto producer = [&]() {
        while (true) {
            int val = next_item.fetch_add(1, std::memory_order_relaxed);
            if (val >= total_items) break;
            while (!q.try_push(val));
        }
    };

    auto consumer = [&]() {
        while (true) {
            int done = received_count.load(std::memory_order_relaxed);
            if (done >= total_items) break;
            auto v = q.try_pop();
            if (v) {
                received_sum.fetch_add(*v, std::memory_order_relaxed);
                received_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_producers; ++i)  threads.emplace_back(producer);
    for (int i = 0; i < num_consumers; ++i)  threads.emplace_back(consumer);
    for (auto& t : threads) t.join();

    EXPECT_EQ(received_count.load(), total_items);
    long long expected_sum = (long long)total_items * (total_items - 1) / 2;
    EXPECT_EQ(received_sum.load(), expected_sum);
}

TEST(MPMCQueueLF, Concurrent_1P1C) { run_concurrent_test(1, 1, 65536); }
TEST(MPMCQueueLF, Concurrent_2P2C) { run_concurrent_test(2, 2, 65536); }
TEST(MPMCQueueLF, Concurrent_4P4C) { run_concurrent_test(4, 4, 65536); }

// ── MPMCQueueMtx ─────────────────────────────────────────────────────────────

TEST(MPMCQueueMtx, PushPop_SingleElement) {
    MPMCQueueMtx<int, 4> q;
    EXPECT_TRUE(q.try_push(42));
    auto v = q.try_pop();
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, 42);
}

TEST(MPMCQueueMtx, TryPush_WhenFull_ReturnsFalse) {
    MPMCQueueMtx<int, 4> q;
    for (int i = 0; i < 4; ++i) EXPECT_TRUE(q.try_push(i));
    EXPECT_FALSE(q.try_push(99));
}

TEST(MPMCQueueMtx, TryPop_WhenEmpty_ReturnsNullopt) {
    MPMCQueueMtx<int, 4> q;
    EXPECT_EQ(q.try_pop(), std::nullopt);
}

TEST(MPMCQueueMtx, FIFO_Order) {
    MPMCQueueMtx<int, 8> q;
    for (int i = 0; i < 8; ++i) q.try_push(i);
    for (int i = 0; i < 8; ++i) EXPECT_EQ(*q.try_pop(), i);
}

TEST(MPMCQueueMtx, EmptyAndFull) {
    MPMCQueueMtx<int, 4> q;
    EXPECT_TRUE(q.empty());
    EXPECT_FALSE(q.full());
    for (int i = 0; i < 4; ++i) q.try_push(i);
    EXPECT_FALSE(q.empty());
    EXPECT_TRUE(q.full());
}

static void run_concurrent_test_mtx(int num_producers, int num_consumers, int total_items) {
    MPMCQueueMtx<int, 1024> q;

    std::atomic<int> next_item{0};
    std::atomic<int> received_count{0};
    std::atomic<long long> received_sum{0};

    auto producer = [&]() {
        while (true) {
            int val = next_item.fetch_add(1, std::memory_order_relaxed);
            if (val >= total_items) break;
            while (!q.try_push(val));
        }
    };

    auto consumer = [&]() {
        while (true) {
            if (received_count.load(std::memory_order_relaxed) >= total_items) break;
            auto v = q.try_pop();
            if (v) {
                received_sum.fetch_add(*v, std::memory_order_relaxed);
                received_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < num_producers; ++i) threads.emplace_back(producer);
    for (int i = 0; i < num_consumers; ++i) threads.emplace_back(consumer);
    for (auto& t : threads) t.join();

    EXPECT_EQ(received_count.load(), total_items);
    long long expected_sum = (long long)total_items * (total_items - 1) / 2;
    EXPECT_EQ(received_sum.load(), expected_sum);
}

TEST(MPMCQueueMtx, Concurrent_1P1C) { run_concurrent_test_mtx(1, 1, 65536); }
TEST(MPMCQueueMtx, Concurrent_2P2C) { run_concurrent_test_mtx(2, 2, 65536); }
TEST(MPMCQueueMtx, Concurrent_4P4C) { run_concurrent_test_mtx(4, 4, 65536); }
