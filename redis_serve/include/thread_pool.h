#include<stdlib.h>
#include<pthread.h>
#include<vector>
#include<deque>

struct Work{
    void (*f) (void*) = NULL;
    void *args;
};

struct Thread_pool{
    std::vector<pthread_t> thread_pool;
    std::deque<Work> que;
    pthread_mutex_t mu;
    pthread_cond_t not_empty;
};

void thread_pool_init(Thread_pool* tp, size_t thread_num);
void thread_pool_queue(Thread_pool* tp, void (*f)(void*), void* args);