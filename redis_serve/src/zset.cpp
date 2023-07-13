#include<string.h>
#include<assert.h>
#include<stdlib.h>

#include "zset.h"

static Z_node* Znode_new(const char* name, size_t len, double score){
    Z_node* node = (Z_node*)malloc(sizeof(Z_node) + len);
    assert(node);
    uint32_t hcode = str_hash((uint8_t*)name, len);
    node->hmap.code = hcode; node->hmap.next = NULL;
    node->len = len;
    node->score = score;
    memcpy(&(node->name[0]), name, len);
    avl_init(&(node->tree));
    return node;
}

static size_t min(size_t a, size_t b){
    return a < b ? a : b;
}

bool h_cmp(H_node* lhn, H_node* rhn){
    if(lhn->code != rhn->code) return false;

    Z_node* le = container_of(lhn, Z_node, hmap);
    Z_node* re = container_of(rhn, Z_node, hmap);
    if(le->len != re->len) return false;
    return 0 == memcmp(le->name, re->name, le->len);
}

static bool zless(AVL_node* lnode, const char* name, size_t len, double score){
    Z_node* pointer = container_of(lnode, Z_node, tree);
    if(pointer->score != score) return pointer->score < score;
    int rv = memcmp(pointer->name, name, min(len, pointer->len));
    if(rv) return rv < 0;
    return pointer->len < len;
}

static bool zless(AVL_node* lnode, AVL_node* rnode){
    Z_node* pointer = container_of(rnode, Z_node, tree);
    return zless(lnode, pointer->name, pointer->len, pointer->score);
}

static void tree_add(Z_set* zset, Z_node* node){
    if(zset->tree == NULL){
        zset->tree = &(node->tree);
        return ;
    }
    AVL_node *avl_node = &(node->tree), *root = zset->tree, **from;
    while(true){
        if(zless(avl_node, root)) from = &(root->left);
        else from = &(root->right);
        
        if((*from) == NULL){
            *from = avl_node;
            avl_node->parent = root;
            zset->tree = avl_fix(avl_node);
            break;
        }
        root = *from;
    }
}

static void zset_update(Z_set* zset, Z_node* node, double score){
    if(node->score == score) return ;
    zset->tree = avl_del(&(node->tree));
    avl_init(&(node->tree));
    node->score = score;
    tree_add(zset, node);
}

bool zset_add(Z_set* zset, const char* name, size_t len, double score){
    Z_node* znode = Znode_new(name, len, score);
    H_node* node = hm_lookup(&(zset->hmap), &(znode->hmap), h_cmp);
    if(node){
        zset_update(zset, znode, score);
        free(znode);
        return false;
    }
    else{
        hm_insert(&(zset->hmap), &(znode->hmap));
        AVL_node* root = zset->tree;
        tree_add(zset, znode);
    }
    return true;
}

Z_node* zset_pop(Z_set* zset, const char* name, size_t len){
    uint32_t hcode = str_hash((uint8_t*)name, len);
    H_node pre; pre.next = NULL; pre.code = hcode;
    H_node* hnode = hm_pop(&(zset->hmap), &pre, h_cmp);
    if(!hnode) return NULL;
   
    Z_node* res = container_of(hnode, Z_node, hmap);
    zset->tree = avl_del(&(res->tree));
    return res;
}

Z_node* zset_find(Z_set* zset, const char* name, size_t len){
    uint32_t code = str_hash((uint8_t*)name, len);
    Z_node* znode = (Z_node*)malloc(sizeof(Z_node)+len);
    znode->hmap.code = code; znode->len = len; memcpy(znode->name, name, len);
    H_node* node = hm_lookup(&(zset->hmap), &(znode->hmap), h_cmp);
    if(!node) return NULL;
    Z_node* znode_1 = container_of(node, Z_node, hmap);
    return znode_1;
}

Z_node* zset_query(Z_set* zset, double score, const char* name, size_t len, int64_t offset){
    AVL_node* cur = zset->tree, *found;
    while(cur){
        if(zless(cur, name, len, score)){
            cur = cur->right;
        }else{
            found = cur;
            cur = cur->left;
        }
    }
    if(found) found = avl_offset(found, offset);
    return found ? container_of(found, Z_node, tree) : NULL;
}

static void tree_distroy(AVL_node* root){
    if(root->left) tree_distroy(root->left);
    if(root->right) tree_distroy(root->right);
    free((Z_node*)container_of(root, Z_node, tree));
}

void zset_dispose(Z_set* zset){
    tree_distroy(zset->tree);
    hm_distroy(&(zset->hmap));
    free(zset);
}