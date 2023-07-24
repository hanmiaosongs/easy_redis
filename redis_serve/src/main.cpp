#include<stdio.h>
#include<errno.h>
#include<string.h>
#include<unistd.h>
#include<netinet/ip.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<signal.h>
#include<sys/wait.h>
#include<netinet/in.h>
#include<sys/types.h>
#include<vector>
#include<fcntl.h>
#include<poll.h>
#include<assert.h>
#include<unordered_map>
#include<string>
#include<iostream>
#include"Timer.h"
#include"thread_pool.h"

#define PORT "3490"
#define BACKLOG 10



struct {
    H_map hmap;
    std::vector<Conn*> fd2conn;
    DList timer_list;
    Thread_pool tp;
}g_data;

enum{
    STATE_REQ = 0,
    STATE_RES = 1,
    STATE_END = 2
};

enum{
    T_STR = 0,
    T_ZSET = 1,
};

struct Entry{
    H_node node;
    std::string key, val;
    uint32_t type;
    Z_set* zset = NULL;
    Entry(){};
    Entry(uint32_t cd, std::string &ke, std::string &va){
        node.code = cd;
        key = ke;
        val = va;
    };
    Entry(uint32_t cd, std::string &ke){
        node.code = cd;
        key = ke;
    }
};

void msg(const char* msg){
    fprintf(stderr, "%s\n", msg);
}

void die(const char* msg){
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    exit(1);
}

void set_fd_nb(int connfd){
    errno = 0;
    int flags = fcntl(connfd, F_GETFL, 0);
    if(errno){
        die("fcntl error");
    }
    flags |= O_NONBLOCK;
    (void)fcntl(connfd, F_SETFL, flags);
    if(errno){
        die("fcntl error");
    }
}

bool cmp(H_node* lhn, H_node* rhn){
    Entry* le = container_of(lhn, Entry, node);
    Entry* re = container_of(rhn, Entry, node);
    return lhn->code == rhn->code && strcmp(le->key.c_str(), re->key.c_str()) == 0;
}

uint32_t get_cmds(uint8_t* data, uint32_t len, std::vector<std::string> &cmds){
    char a[k_max_msg + 1]; uint32_t cmd_len, nstr;
    if(len < 4) return -1;
    memcpy(&nstr, data, 4);
    len -= 4; data += 4;
    for(int i = 0; i < nstr; i++){
        memcpy(&cmd_len, data, 4);
        if(len < 4 + cmd_len) return -1;
        memcpy(&a, &data[4], cmd_len);
        a[cmd_len] = '\0';
        cmds.emplace_back(a);
        len -= cmd_len + 4;
        data += cmd_len + 4;
    }
    if(len) return -1;
    return 0;
}

static void out_str(std::string &out, size_t size, const char* s){
    out.push_back(SER_STR);
    uint32_t len = size;
    out.append((char*)&len, 4);
    out.append(s, len);
}

static void out_updatearr(std::string &out, uint32_t n){
    assert(out[0] == SER_ARR);
    memcpy(&(out[0]), &n, 4);
}

static void out_str(std::string &out, const std::string &s){
    size_t size = s.size();
    out_str(out, size, (char*)s.data());
}

static void out_int(std::string &out, int64_t val){
    out.push_back(SER_INT);
    out.append((char*)&val, 8);
}

static void out_nil(std::string &out){
    out.push_back(SER_NIL);
}

static void out_dbl(std::string &out, double val){
    out.push_back(SER_DBL);
    out.append((char*)&val, 8);
}

static void out_err(std::string &out, int32_t err_type, const std::string msg){
    out.push_back(SER_ERR);
    out.append((char*)&err_type, 4);
    uint32_t len = msg.size();
    out.append((char*) &len, 4);
    out.append(msg);
}

static void out_arr(std::string &out, u_int32_t len){
    out.push_back(SER_ARR);
    out.append((char*)&len, 4);
}

static int32_t do_set(std::vector<std::string> &cmds, std::string &out){
    uint32_t cd = str_hash((uint8_t*)((cmds[1]).data()), (cmds[1]).size());
    Entry key; key.key = cmds[1], key.node.code = cd;
    
    H_node* ans = hm_lookup(&(g_data.hmap), &(key.node), cmp);
    if(ans == NULL){
        Entry *now = new Entry;
        now->key = cmds[1]; now->val = cmds[2]; now->node.code = cd; now->type = 0;
        hm_insert(&(g_data.hmap), &(now->node));
    }
    else  {
        Entry* now = (container_of(ans, Entry, node));
        if(now->type != T_STR){
            out_err(out, ERR_TYPE, "expect a string type");
            return -1;
        }
        now->val = cmds[2];
    }
    out_nil(out);
    return 0;    
}

static int32_t do_get(std::vector<std::string> &cmds, std::string &out){
    uint32_t cd = str_hash((uint8_t*)((cmds[1]).data()), (cmds[1].size()));
    Entry now(cd, cmds[1]);
    H_node* ans = hm_lookup(&(g_data.hmap), &(now.node), cmp);
    if(ans == NULL){
        std::string res = "the \"" + cmds[1] + "\" is not found";
        out_str(out, res);
        return -1;
    }
    std::string res = (container_of(ans, Entry, node))->val;
    out_str(out, res);
    return 0;
}

static void entry_del_async(void* args){
    Entry* ent = (Entry *)args;

    zset_dispose(ent->zset);
    delete(ent->zset);
    delete(ent);
    return ;
}

static void entry_del(Entry* entry){
    const size_t k_large_container_size = 10000;
    bool big_size = false;
    switch (entry->type)
    {
    case T_ZSET:
        big_size = k_large_container_size < hm_size(&(entry->zset->hmap));
        break;
    }
    if(big_size){
        thread_pool_queue(&(g_data.tp), entry_del_async, (void *)entry);
        return ;
    }

    if(entry->type == T_ZSET){
        zset_dispose(entry->zset);
        delete(entry->zset);
    }
    delete(entry);
}

static int32_t do_del(std::vector<std::string> &cmds, std::string &out){
    uint32_t cd = str_hash((uint8_t*)((cmds[1]).data()), (cmds[1]).size());
    Entry entry(cd, cmds[1]);

    H_node* now = hm_pop(&(g_data.hmap), &(entry.node), cmp);    
    if(now != NULL){
        std::string res = "ok!";
        out_str(out, res);
        return 0;
    }
    else{
        std::string res = "not found!";
        out_str(out, res);
        return 0;
    }
    entry_del(container_of(now, Entry, node));
    out_int(out, 1);
    return 0;
}

static bool str2dbl(const std::string &s, double &out){
    char* epbl;
    out = strtod(s.c_str(), &epbl);
    return epbl == s.c_str() + s.size();
}

static bool str2int(const std::string &s, int64_t &out){
    char* epbl;
    out = strtoll(s.c_str(), &epbl, 10);
    return epbl == s.c_str() + s.size();
}

static void h_scan(std::string &out, void(*f)(H_node*, void*), H_table* ht){
    if(ht->tb_size == 0) return;
    for(int i = 0; i < ht->code_mask + 1; i++){
        H_node* node = ht->h_tb[i];
        while(node){
            f(node, &out);
            node = node->next;
        }
    }
}

static void cb_scan(H_node* node, void* args){
    std::string &out = *(std::string *)args;
    out_str(out, (container_of(node, Entry, node)->key));
}

static void do_keys(std::string &out){
    out_arr(out, (uint32_t)(g_data.hmap.tb_1->tb_size + g_data.hmap.tb_2->tb_size));
    h_scan(out, cb_scan, g_data.hmap.tb_1);
    if(g_data.hmap.tb_2) h_scan(out, cb_scan, g_data.hmap.tb_2);
}

static bool expect_zset(std::string &s, Entry** entry, std::string &out){
    Entry key(str_hash((uint8_t*)s.data(), s.size()), s);

    H_node* node = hm_lookup(&(g_data.hmap), &(key.node), cmp);
    if(node == NULL){
        out_err(out, ERR_ARG, "not found!");
        return false;
    }
    *entry = container_of(node, Entry, node);
    if((*entry)->type != T_ZSET){
        out_err(out, ERR_TYPE, "expect zset!");
        return false;
    }
    return true;
}

static void do_zscore(std::vector<std::string> &cmds, std::string &out){
    Entry *entry;
    if(!expect_zset(cmds[1], &entry, out)){
        return out_nil(out);
    }
    Z_node* znode = zset_find(entry->zset, (char*)cmds[2].data(), cmds[2].size());
    return znode == NULL ? out_nil(out) : out_dbl(out, znode->score);
}

static int32_t do_zadd(std::vector<std::string> &cmds, std::string &out){
    double score;
    Entry *entry;
    if(!str2dbl(cmds[3], score)){
        out_err(out, ERR_ARG, "expect a double score");
        return -1;
    }
    uint32_t code = str_hash((uint8_t*)cmds[1].data(), cmds[1].size());
    Entry key(code, cmds[1]); 
    H_node* node = hm_lookup(&(g_data.hmap), &(key.node), cmp);
    if(!node){
        entry = new Entry;
        entry->key = cmds[1];
        entry->type = T_ZSET;
        entry->node.code = code;
        entry->zset = new Z_set;
        hm_init(4, &(entry->zset->hmap));
        hm_insert(&(g_data.hmap), &(entry->node));
    }
    else{
        entry = container_of(node, Entry, node);
        if(entry->type != T_ZSET){
            out_err(out, ERR_ARG, "expect zset");
            return -1;
        }
    }
    zset_add(entry->zset, (char*)cmds[2].data(), cmds[2].size(), score);
    out_str(out, "ok");
    return 0;
}

static int32_t do_zquery(std::vector<std::string> &cmds, std::string &out){
    double score;
    Entry *entry;
    if(!str2dbl(cmds[3], score)){
        out_err(out, ERR_ARG, "expect double score");
        return -1;
    }

    int64_t offset, limmit;
    if(!str2int(cmds[4], offset)){
        out_err(out, ERR_ARG, "expect int");
        return -1;
    }
    if(!str2int(cmds[5], limmit)){
        out_err(out, ERR_ARG, "expect int");
        return -1;
    }

    if(!expect_zset(cmds[1], &entry, out)){
        out_err(out, ERR_TYPE, "expect zset");
        return -1;
    }
    Z_node* znode = zset_query(entry->zset, score, cmds[2].data(), cmds[2].size(), offset);
    uint32_t n = 0;
    out_arr(out, 0);
    for(int i = 0; i < limmit && znode; i++){
        out_str(out, znode->name);
        out_dbl(out, znode->score);
        n += 2;
        AVL_node* avlnode = avl_offset(&(znode->tree), 1);
        znode = container_of(avlnode, Z_node, tree);
    }
    out_updatearr(out, n);
    return 0;
}

static void do_zrem(std::string &out, std::vector<std::string> &cmds){
    Entry *entry;

    if(!expect_zset(cmds[1], &entry, out)){
        return out_int(out, 0);
    }

    Z_node* znode = zset_pop(entry->zset, (char*)cmds[2].data(), cmds[2].size());
    free(znode);
    return out_int(out, 1);
}

static bool cmd_is(std::string &s, const char* cmd){
    return strcasecmp((char*)s.c_str(), cmd) == 0;
}

static int32_t do_one_request(uint8_t* data, uint32_t len, std::string &out){
    std::vector<std::string> cmds;
    if(get_cmds(data, len, cmds)){
        std::string res = "cmd error please try again!";
        out_str(out, res);
        return -1;
    }
    if(cmd_is(cmds[0], "set") && cmds.size() == 3){
        if(do_set(cmds, out)) return -1;
    }
    else if(cmd_is(cmds[0], "get") && cmds.size() == 2){
        if(do_get(cmds, out)) return -1;
    }
    else if(cmd_is(cmds[0], "del") && cmds.size() == 2){
        if(do_del(cmds, out)) return -1;
    }
    else if(cmd_is(cmds[0], "zrem") && cmds.size() == 3){
        do_zrem(out, cmds);
    }
    else if(cmd_is(cmds[0], "zquery") && cmds.size() == 6){
        if(do_zquery(cmds, out)) return -1;
    }
    else if(cmd_is(cmds[0], "zadd") && cmds.size() == 4){
        if(do_zadd(cmds, out)) return -1;
    }
    else if(cmd_is(cmds[0], "zscore") && cmds.size() == 3){
        do_zscore(cmds, out);
    }
    else if(cmd_is(cmds[0], "keys") && cmds.size() == 1){
        do_keys(out);
    }
    else{
        std::string res = "cmd error please try again.";
        out_str(out, res);
        return -1;
    }
    return 0;
}

void* get_in_addr(struct sockaddr *sa){
    if(sa->sa_family == AF_INET){
        return &(((struct sockaddr_in *)sa)->sin_addr);
    }
    else{
        return &(((struct sockaddr_in6 *)sa)->sin6_addr);
    }
}
static void conn_put(std::vector<Conn*> &fd2conn, Conn* conn){
    if((size_t)conn->connfd >= fd2conn.size()){
        fd2conn.resize(conn->connfd + 1);
    }
    fd2conn[conn->connfd] = conn;
}

static int32_t accept_new_fd(std::vector<Conn*> &fd2conn, int sockfd){
    struct sockaddr_storage client_addr;
    socklen_t sin_size; char s[1000];

    sin_size = sizeof(client_addr);
    int new_fd = accept(sockfd, (struct sockaddr *)&client_addr, &sin_size);

    if(new_fd == -1){
        perror("accept");
        return -1;
    }

    inet_ntop(client_addr.ss_family, get_in_addr((struct sockaddr *)&client_addr), s, sizeof(s));
    printf("sever : get connection from %s\n", s);

    Conn* conn = new Conn;
    conn->connfd = new_fd;
    conn->state = STATE_REQ;
    conn->rbuf_sz = conn->wbuf_sent = conn->wbuf_sz = 0;
    conn->idle_start = get_monotonic_us();
    dlist_insert_before(&(g_data.timer_list), &(conn->idle_list));
    conn_put(fd2conn, conn);
    return 0;
}

static void state_res(Conn* conn);
static bool try_flush_buffer(Conn* conn);

static bool try_one_request(Conn* conn){
    if(conn->rbuf_sz < 4){
        return false;
    }
    uint32_t len = 0, wlen, rescode;
    memcpy(&len, conn->rbuf, 4);
    if(len > k_max_msg){
        msg("too long");
        conn->state = STATE_END;
        return false;
    }
    if(4 + len > conn->rbuf_sz){
        return false;
    }

    std::string out;
    do_one_request((uint8_t*)&conn->rbuf[4], len, out);
    wlen = out.size();
    if(wlen + 4 > k_max_msg){
        out.clear();
        out_err(out, ERR_2BIG, "too long to get");
        wlen = out.size();
    }
    memcpy(conn->wbuf, &wlen, 4);
    memcpy(&conn->wbuf[4], out.data(), (size_t)wlen);
    conn->wbuf_sz = 4+wlen;

    size_t remain = conn->rbuf_sz - len - 4;
    if(remain){
        memmove(conn->rbuf, &conn->rbuf[4+len], remain);
    }
    conn->rbuf_sz = remain;

    conn->state = STATE_RES;
    state_res(conn);

    return (conn->state == STATE_REQ);
}

static bool try_flush_buffer(Conn* conn){
    ssize_t rv = 0;
    do{
        size_t remain = conn->wbuf_sz - conn->wbuf_sent;
        rv = write(conn->connfd, &conn->wbuf[conn->wbuf_sent], remain);
    }while(rv < 0 && errno == EINTR);
    if(rv < 0 && errno == EAGAIN){
        return false;
    }
    if(rv < 0){
        msg("write() error");
        conn->state = STATE_END;
        return false;
    }

    conn->wbuf_sent += (size_t)rv;
    assert(conn->wbuf_sent <= conn->wbuf_sz);
    if(conn->wbuf_sz == conn->wbuf_sent){
        conn->wbuf_sent = conn->wbuf_sz = 0;
        conn->state = STATE_REQ;
        return false;
    }
    return true;
}

static void state_res(Conn* conn){
    while(try_flush_buffer(conn)) ;
}

static bool try_fill_buffer(Conn* conn){
    assert(conn->rbuf_sz < sizeof(conn->rbuf));
    ssize_t rv;
    do{
        size_t cap = sizeof(conn->rbuf) - conn->rbuf_sz;
        rv = read(conn->connfd, conn->rbuf + conn->rbuf_sz, cap);
    }while(rv < 0 && errno == EINTR);
    if(rv < 0 && errno == EAGAIN){
        return false;                              
    }
    if(rv < 0){
        msg("read() error");
        conn->state = STATE_END;
        return false;
    }
    if(rv == 0){
        if(conn->rbuf_sz > 0){
            msg("unexpected error");
        }
        else{
            msg("EOF");
        }
        conn->state = STATE_END;
        return false;
    }
    conn->rbuf_sz += (size_t)rv;

    while(try_one_request(conn)) ;
    return conn->state == STATE_REQ;
}

void state_req(Conn* conn){
    while(try_fill_buffer(conn)) ;
}

void connection_io(Conn* conn){
    conn->idle_start = get_monotonic_us();
    dlist_detach(&(conn->idle_list));
    dlist_insert_before(&(g_data.timer_list), &(conn->idle_list));
    if(conn->state == STATE_REQ){
        state_req(conn);
    }
    else if(conn->state == STATE_RES){
        state_res(conn);
    }
}

void conn_done(Conn* conn){
    g_data.fd2conn[conn->connfd] = NULL;
    close(conn->connfd);
    dlist_detach(&(conn->idle_list));
    free(conn);
}

void process_timer(DList *node){
    uint64_t now_us = get_monotonic_us();
    Conn *conn;
    while(!dlist_empty(node)){
        if(next_timer_ms(node->next, &conn)) break;
        printf("removing idle connection: %d\n", conn->connfd);
        conn_done(conn);
    }
}

int main(){
    if(hm_init((size_t)4, &(g_data.hmap))) die("hash table init error!!");
    int status, sockfd, yes = 1, rv;
    struct addrinfo *serveinfo, *info, hints;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_flags = AI_PASSIVE;
    hints.ai_socktype = SOCK_STREAM;

    std::vector<struct pollfd> poll_args;
    g_data.timer_list.next = g_data.timer_list.prev = &(g_data.timer_list);
    thread_pool_init(&g_data.tp, 4);

    if((status = getaddrinfo(NULL, PORT, &hints, &serveinfo)) != 0){
        fprintf(stderr, "getaddrinfo error: %s\n", gai_strerror(status));
        exit(1);
    }        
    
    for(info = serveinfo; info != NULL; info = info->ai_next){
        void *addr; char *ipver;

        if((sockfd = socket(info->ai_family, info->ai_socktype, info->ai_protocol)) == -1){
            perror("server: socket");
            continue;
        }

        if(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1){
            perror("setsockopt");
            exit(1);
        }

        if(bind(sockfd, info->ai_addr, info->ai_addrlen) == -1){
            close(sockfd);
            perror("server: bind");
            continue;
        }

        break;
    }
    set_fd_nb(sockfd);

    if(info == NULL){
        fprintf(stderr, "server: failed to bind\n");
        return 2;
    }

    if (listen(sockfd, BACKLOG) == -1){
        perror("listen");
        exit(1);
    }

    while(1){
        poll_args.clear();

        struct pollfd pfd = {sockfd, POLL_IN, 0};
        poll_args.push_back(pfd);
        for(Conn* conn : g_data.fd2conn){
            if(!conn) continue;
            struct pollfd pfd = {};
            pfd.fd = conn->connfd;
            pfd.events = (conn->state == STATE_REQ) ? POLL_IN : POLL_OUT;
            pfd.events |= POLL_ERR;
            poll_args.push_back(pfd);
        }

        rv = poll(poll_args.data(), (unsigned int)poll_args.size(), 1000);
        if(rv < 0){
            die("poll");
        }
        
        for(size_t i = 1; i < poll_args.size(); i++){
            if(poll_args[i].revents) {
                Conn *conn = g_data.fd2conn[poll_args[i].fd];
                connection_io(conn);
                if(conn->state == STATE_END){
                    conn_done(conn);
                }
            }
        }

        process_timer(&(g_data.timer_list));

        if(poll_args[0].revents){
            (void)accept_new_fd(g_data.fd2conn, sockfd);
        }
    }
    return 0;
}
