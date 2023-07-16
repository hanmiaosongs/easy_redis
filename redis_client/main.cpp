#include<poll.h>
#include<fcntl.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<errno.h>
#include<netdb.h>
#include<sys/types.h>
#include<netinet/in.h>
#include<string.h>
#include<sys/socket.h>
#include<arpa/inet.h>
#include<fcntl.h>
#include<vector>
#include<assert.h>
#include<iostream>
#include"common.h"

#define PORT "3490"
#define MAXDATASIZE 100

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
    int flag = fcntl(connfd, F_GETFL, 0);
    if(errno){
        die("fcntl error");
    }
    flag |= O_NONBLOCK;
    (void)fcntl(connfd, F_SETFL, flag);
    if(errno){
        die("fcntl error");
    }
}



void *get_in_addr(struct sockaddr* sa){
    if(sa->sa_family = AF_INET){
        return &(((struct sockaddr_in *)sa)->sin_addr);
    }
    else{
        return &(((struct sockaddr_in6 *)sa)->sin6_addr);
    }
}

static ssize_t send_full(int connfd, char* wbuf, size_t len){
    while(len > 0){
        ssize_t sended = send(connfd, wbuf, len, 0);
        if(sended < 0) return -1;
        assert((size_t)sended <= len);
        wbuf += sended;
        len -= (size_t)sended;
    }
    return 0;
}

int read_full(int connfd, char* rbuf, size_t n){
    while(n > 0){
        ssize_t numbytes = read(connfd, rbuf, n);
        if(numbytes < 0) return -1;
        assert((size_t)numbytes <= n);
        n -= (size_t)numbytes;
        rbuf += numbytes;
    }
    return 0;
}

static ssize_t send_req(int fd, const uint8_t* s, uint32_t len){
    if(len > k_max_msg){
        msg("too long");
        return -1;
    }
    char wbuf[k_max_msg+4];
    memcpy(wbuf, &len, 4);
    memcpy(&wbuf[4], s, len);
    return send_full(fd, wbuf, 4+len);
}

static int32_t one_response(size_t len, const uint8_t* data){
    if(len < 1){
        msg("bad response!");
        return -1;
    }
    switch(data[0]){
    case SER_NIL:
        msg("nil");
        return 1;
    case SER_INT:
        int64_t num;
        if(len < 9) {
            msg("bad response");
            return -1;
        }
        memcpy(&num, &data[1], (size_t)8);
        printf("(int) %ld \n", num);
        return 9;
    case SER_DBL:
        double dbl;
        if(len < 9) {
            msg("bad response");
            return -1;
        }
        memcpy(&dbl, &data[1], 8);
        printf("(dbl) %lf\n", dbl);
        return 9;
    case SER_STR:
        uint32_t slen;
        if(len < 5){
            msg("bad response");
            return -1;
        }
        memcpy(&slen, &data[1], 4);
        if(len < 5 + slen) {
            msg("bad response");
            return -1;
        }
        printf("(str) %.*s\n", slen, &data[5]);
        return 5+slen;
    case SER_ERR:
        int32_t code; uint32_t msglen;
        if(len < 9){
            msg("bad response");
            return -1;
        }
        memcpy(&code, &data[1], 4);
        memcpy(&msglen, &data[5], 4);
        if(len < 9+msglen){
            msg("bad response");
            return -1;
        }
        printf("(err) %d %.*s\n", code, msglen, &data[9]);
        return 9+msglen;
    case SER_ARR:
        if(len < 5){
            msg("bad response");
            return -1;
        }
        uint32_t nitem, arry_bytes = 5;
        memcpy(&nitem, &data[1], 4);
        printf("(arr) item_num=%d\n", nitem);
        for(int i = 0; i < nitem; i++){
            int32_t rv = one_response(len - arry_bytes, &data[arry_bytes]);
            if(rv < 0) return rv;
            arry_bytes += rv;
        }
        printf("(arr) end\n");
        return (int32_t)arry_bytes;
    }
    msg("bad response");
    return -1;
}

static ssize_t read_res(int fd){
    char rbuf[k_max_msg+5] = {0};
    if(read_full(fd, rbuf, 4)) {
        if(errno == 0){
            msg("EOF");
            return -1;
        }
        else {
            msg("read error");
            return -1;
        }
    }
    uint32_t len, nstr, tt = 8;
    memcpy(&len, rbuf, 4);

    if(len > k_max_msg) {
        msg("too long");
        return -1;
    }

    if(read_full(fd, &rbuf[4], (size_t)len)) {
        msg("read error");
        return -1;
    }

    if(one_response(len, (uint8_t*)&rbuf[4]) < 0) return -1;
    
    return 0;
}

static int32_t put_cmd(uint8_t* buf, const uint8_t* cmd, const uint32_t len, uint32_t &res){
    if(4 + len > res) return -1;
    res -= 4 + len;
    memcpy(buf, &len, 4);
    memcpy(&buf[4], cmd, len);
    return 0;
}

static int32_t do_set(int connfd, const char* ke, const char* content){
    uint32_t wlen = 3, tt = 4, res = k_max_msg;
    uint8_t wbuf[k_max_msg+4] = {0};
    memcpy(wbuf, &wlen, 4);

    if(put_cmd(&wbuf[tt], (uint8_t*)"set", 3, res)) return -1;
    tt += 7;
    if(put_cmd(&wbuf[tt], (uint8_t*)ke, strlen(ke), res)) return -1;
    tt += strlen(ke) + 4;
    if(put_cmd(&wbuf[tt], (uint8_t*)content, strlen(content), res)) return -1;
    tt += strlen(content) + 4;

    if(send_req(connfd, wbuf, tt)) return -1;
    if(read_res(connfd)) return -1;
    return 0;
}

static int32_t do_get(int connfd, const char* ke){
    uint32_t wlen = 2, tt = 4, res = k_max_msg;
    uint8_t wbuf[k_max_msg+4] = {0};  
    memcpy(wbuf, &wlen, 4);

    if(put_cmd(&wbuf[tt], (uint8_t*)"get", 3, res)) return -1;
    tt += 7;
    if(put_cmd(&wbuf[tt], (uint8_t*)ke, strlen(ke), res)) return -1;
    tt += strlen(ke) + 4;

    if(send_req(connfd, wbuf, tt)) return -1;
    if(read_res(connfd)) return -1;
    return 0;
}

static int32_t do_del(int connfd, const char* ke){
    uint32_t wlen = 2, tt = 4, res = k_max_msg;
    uint8_t wbuf[k_max_msg + 4] = {0};
    memcpy(wbuf, &wlen, 4);

    if(put_cmd(&wbuf[tt], (uint8_t*)"del", (uint32_t)3, res)) return -1;
    tt += 7;
    if(put_cmd(&wbuf[tt], (uint8_t*)ke, strlen(ke), res)) return -1;
    tt += strlen(ke) + 4;
    
    if(send_req(connfd, wbuf, tt)) return -1;
    if(read_res(connfd)) return -1;
    return 0;
}

int32_t do_keys(int connfd){
    uint32_t tt = 4, wlen = 1, res = k_max_msg;
    uint8_t wbuf[k_max_msg+4] = {0};
    memcpy(wbuf, &wlen, 4);
    if(put_cmd(&wbuf[tt], (uint8_t*)"keys", (uint32_t)3, res)) return -1;
    tt += 7;
    
    if(send_req(connfd, wbuf, tt)) return -1;
    if(read_res(connfd)) return -1;
    return 0;
}

int32_t do_zscore(int connfd, const char* ke, const char* name){
    uint32_t tt = 4, wlen = 3, res = k_max_msg;
    uint8_t wbuf[k_max_msg+4] = {0};
    memcpy(wbuf, &wlen, 4);
    if(put_cmd(&wbuf[tt], (uint8_t*)"zscore", (uint32_t)6, res)) return -1;
    tt += 10;
    if(put_cmd(&wbuf[tt], (uint8_t*)ke, strlen(ke), res)) return -1;
    tt += strlen(ke) + 4;
    if(put_cmd(&wbuf[tt], (uint8_t*)name, strlen(name), res)) return -1;
    tt += strlen(name) + 4;

    if(send_req(connfd, wbuf, tt)) return -1;
    if(read_res(connfd)) return -1;
    return 0;
}

int32_t do_zadd(int connfd, const char* ke, const char* name, double sc){
    uint32_t tt = 4, wlen = 4, res = k_max_msg;
    uint8_t wbuf[k_max_msg+4] = {0}; char dd[30] = {0};
    memcpy(wbuf, &wlen, 4);
    sprintf(dd, "%.6lf", sc); printf("%s\n", dd);
    if(put_cmd(&wbuf[tt], (uint8_t*)"zadd", (uint32_t)4, res)) return -1;
    tt += 8;
    if(put_cmd(&wbuf[tt], (uint8_t*)ke, strlen(ke), res)) return -1;
    tt += strlen(ke) + 4;
    if(put_cmd(&wbuf[tt], (uint8_t*)name, strlen(name), res)) return -1;
    tt += strlen(name) + 4;
    if(put_cmd(&wbuf[tt], (uint8_t*)dd, strlen(dd), res));
    tt += strlen(dd) + 4;

    if(send_req(connfd, wbuf, tt)) return -1;
    if(read_res(connfd)) return -1;
    return 0;
}

int32_t do_zrem(int connfd, const char* ke, const char* name){
    uint32_t tt = 4, wlen = 3, res = k_max_msg;
    uint8_t wbuf[k_max_msg+4] = {0};
    memcpy(wbuf, &wlen, 4);
    if(put_cmd(&wbuf[tt], (uint8_t*)"zrem", 6, res)) return -1;
    tt += 10;
    if(put_cmd(&wbuf[tt], (uint8_t*)ke, strlen(ke), res)) return -1;
    tt += strlen(ke) + 4;
    if(put_cmd(&wbuf[tt], (uint8_t*)name, strlen(name), res)) return -1;
    tt += strlen(name) + 4;

    if(send_req(connfd, wbuf, tt)) return -1;
    if(read_res(connfd)) return -1;
    return 0;
}

int32_t do_zquery(int connfd, const char* ke, const char* name, double sc, int64_t offset, int64_t limit){
    uint32_t tt = 4, wlen = 6, res = k_max_msg;
    uint8_t wbuf[k_max_msg+4] = {0}; char dd[30] = {0};
    memcpy(wbuf, &wlen, 4);
    if(put_cmd(&wbuf[tt], (uint8_t*)"zquery", 6, res)) return -1;
    tt += 10;
    if(put_cmd(&wbuf[tt], (uint8_t*)ke, strlen(ke), res)) return -1;
    tt += strlen(ke) + 4;
    if(put_cmd(&wbuf[tt], (uint8_t*)name, strlen(name), res)) return -1;
    tt += strlen(name) + 4;
    sprintf(dd, "%.6lf", sc);
    if(put_cmd(&wbuf[tt], (uint8_t*)dd, strlen(dd), res)) return -1;
    tt += strlen(dd) + 4;
    memset(dd, 0, sizeof(dd)); sprintf(dd, "%ld", offset);
    if(put_cmd(&wbuf[tt], (uint8_t*)dd, strlen(dd), res)) return -1;
    tt += strlen(dd) + 4;
    memset(dd, 0, sizeof(dd)); sprintf(dd, "%ld", limit);
    if(put_cmd(&wbuf[tt], (uint8_t*)dd, strlen(dd), res)) return -1;
    tt += strlen(dd) + 4;

    if(send_req(connfd, wbuf, tt)) return -1;
    if(read_res(connfd)) return -1;
    return 0;
}

int main(){
    struct addrinfo hints, *res, *p;
    char s[INET6_ADDRSTRLEN];
    int sockfd, numbytes, rv;
    
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if((rv = getaddrinfo("127.0.0.1", PORT, &hints, &res)) != 0){
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return 1;
    }

    for(p = res; p != NULL; p = p->ai_next){
        if((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1){
            perror("client: socket");
            continue;
        }

        if(connect(sockfd, p->ai_addr, p->ai_addrlen) == -1){
            close(sockfd);
            perror("client: connect");
            continue;
        }

        break;
    }

    if(p == NULL){
        fprintf(stderr, "client: failed to connect\n");
        return 2;
    }
    inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr), s, sizeof(s));

    std::cout << "client: connecting to " << s << "\n";
    freeaddrinfo(res);

    if(do_set(sockfd, "name", "miao han")) goto GONE;
    if(do_zadd(sockfd, "nn", "first", 1.0));
    if(do_zadd(sockfd, "nn", "second", 2.0));
    if(do_zadd(sockfd, "nn", "third", 3.0));
    if(do_zadd(sockfd, "nn", "4th", 4.0));
    if(do_zadd(sockfd, "nn", "5fs", 5.0));
    if(do_get(sockfd, "name")) goto GONE;
    if(do_get(sockfd, "names")) goto GONE;
    if(do_del(sockfd, "names")) goto GONE;


GONE:
    close(sockfd);
    return 0;
}





