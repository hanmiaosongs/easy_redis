#include "thread_pool.h"
#include<assert.h>

static void* worker(void* args){
    Thread_pool* tp = (Thread_pool *)args;

    while(true){
        pthread_mutex_lock(&tp->mu);
        while(tp->que.empty()){
            pthread_cond_wait(&tp->not_empty, &tp->mu);
        }

        Work w = tp->que.front();
        tp->que.pop_front();
        pthread_mutex_unlock(&tp->mu);
        
        w.f(w.args);
    }
    return NULL;
}

void thread_pool_init(Thread_pool* tp, size_t thread_num){
    assert(thread_num > 0);

    int rv = pthread_mutex_init(&tp->mu, NULL);
    assert(rv == 0);
    rv = pthread_cond_init(&tp->not_empty, NULL);
    assert(rv == 0);

    tp->thread_pool.resize(thread_num);
    for(size_t i = 0; i < thread_num; i++){
        pthread_create(&tp->thread_pool[i], NULL, &worker, tp);
    }
}


void thread_pool_queue(Thread_pool* tp, void (*f)(void*), void* args){
    Work w;
    w.f = f; w.args = args;
    
    pthread_mutex_lock(&tp->mu);
    tp->que.push_back(w);
    pthread_cond_signal(&tp->not_empty);
    pthread_mutex_unlock(&tp->mu);
}