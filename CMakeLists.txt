cmake_minimum_required(VERSION 3.20)
project(MPMCRingBuffer CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MPMC_BUILD_BENCH "Build benchmarks" OFF)

add_compile_options(-Wall -Wextra -Wpedantic)

include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

add_executable(mpmc_tests tests/mpmc_queue_test.cpp)
target_include_directories(mpmc_tests PRIVATE src)
target_link_libraries(mpmc_tests PRIVATE GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(mpmc_tests)

add_executable(mpmc_main src/main.cpp)
target_include_directories(mpmc_main PRIVATE src)

if(MPMC_BUILD_BENCH)
    FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG v1.8.4
    )
    set(BENCHMARK_ENABLE_TESTING OFF)
    FetchContent_MakeAvailable(benchmark)

    add_executable(mpmc_bench bench/mpmc_queue_bench.cpp)
    target_include_directories(mpmc_bench PRIVATE src)
    target_link_libraries(mpmc_bench PRIVATE benchmark::benchmark)
endif()
