#include<stdlib.h>
#include<stdint.h>
#include<stddef.h>

struct AVL_node{
    uint32_t depth, cnt;
    struct AVL_node *left, *right, *parent;
};

inline void avl_init(AVL_node* node){
    node->depth = node->cnt = 1;
    node->left = node->right = node->parent = NULL;
}

AVL_node* avl_fix(AVL_node* node);
AVL_node* avl_del(AVL_node* node);
AVL_node* avl_offset(AVL_node* node, int64_t offset);
