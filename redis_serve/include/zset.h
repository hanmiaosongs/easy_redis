#include "common.h"
#include "AVL.h"
#include "hashtable.h"

struct Z_set{
    AVL_node* tree;
    H_map hmap;
};

struct Z_node{
    AVL_node tree;
    H_node hmap;
    double score;
    size_t len = 0;
    char name[0];
};

bool zset_add(Z_set* zset, const char* name, size_t len, double score);
Z_node* zset_find(Z_set* zset, const char* name, size_t len);
Z_node* zset_pop(Z_set* zset, const char* name, size_t len);
Z_node* zset_query(Z_set* zset, double score, const char* name, size_t len, int64_t offset);
void zset_dispose(Z_set* zset);