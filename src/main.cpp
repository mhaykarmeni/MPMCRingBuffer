#include <iostream>
#include "mpmc_queue.h"

int main() {
    MPMCQueueLF<int, 8> q;

    for (int i = 0; i < 8; ++i)
        q.try_push(i);

    while (auto v = q.try_pop())
        std::cout << *v << "\n";
}
