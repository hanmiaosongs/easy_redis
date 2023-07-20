#include "Timer.h"



void dlist_detach(DList *node){
    node->next->prev = node->prev;
    node->prev->next = node->next;
}

void dlist_insert_before(DList *target, DList *node){
    node->next = target;
    node->prev = target->prev;
    node->next->prev = node;
    node->prev->next = node;
}

bool dlist_empty(DList *node){
    return node->next == node;
}

uint64_t get_monotonic_us(){
    timespec tt = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &tt);
    return (uint64_t)tt.tv_sec*1000000 + tt.tv_nsec/1000;
}

uint32_t next_timer_ms(DList *node, Conn **conn){
    node = node->next;
    if(dlist_empty(node)){
        return 10000;
    }
    *conn = container_of(node, Conn, idle_list);
    uint64_t now_us = get_monotonic_us(), next_us = (*conn)->idle_start + k_idle_timeout_ms*1000;
    if(next_us <= now_us) return 0;
    return (uint32_t)((next_us - now_us) / 1000);
}