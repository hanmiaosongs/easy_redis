#include<stdlib.h>
#include<stdint.h>
#include<stddef.h>
#include<time.h>
#include"zset.h"
#include<stdio.h>

const uint32_t k_idle_timeout_ms = 5*1000;

struct DList{
    DList *prev, *next;
};

struct Conn{
    int connfd;
    uint64_t idle_start;
    char rbuf[k_max_msg + 4], wbuf[k_max_msg + 4];
    uint32_t state;
    size_t rbuf_sz, wbuf_sz, wbuf_sent;
    DList idle_list;
};

void dlist_detach(DList *node);
void dlist_insert_before(DList *target, DList *node);
bool dlist_empty(DList *node);

uint32_t next_timer_ms(DList *node, Conn **conn);
uint64_t get_monotonic_us();