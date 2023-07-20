#include"AVL.h"
#include<string.h>

static uint32_t avl_depth(AVL_node* node){
    return node ? node->depth : 0;
}

static uint32_t avl_cnt(AVL_node* node){
    return node ? node->cnt : 0;
}

static void avl_update(AVL_node* node){
    node->cnt = 1 + avl_cnt(node->left) + avl_cnt(node->right);
    if(avl_depth(node->left) <= avl_depth(node->right)) node->depth = avl_depth(node->right) + 1;
    else node->depth = avl_depth(node->left) + 1;
}

static AVL_node* rol_left(AVL_node* root){
    AVL_node* new_node = root->right;
    if(new_node->left != NULL){
        new_node->left->parent = root;
        root->right = new_node->left;
    }
    new_node->parent = root->parent;
    new_node->left = root;
    root->parent = new_node;
    return new_node;
}

static AVL_node* rol_right(AVL_node* root){
    AVL_node* new_node = root->left;
    if(new_node->right != NULL){
        new_node->right->parent = root;
        root->left = new_node->right;
    }
    new_node->parent = root->parent;
    new_node->right = root;
    root->parent = new_node;
    return new_node;
}

static AVL_node* fix_left(AVL_node* root){
    if(avl_depth(root->right->left) > avl_depth(root->right->right)){
        root->right = rol_right(root->right);
    }
    return rol_left(root);
}

static AVL_node* fix_right(AVL_node* root){
    if(avl_depth(root->left->left) < avl_depth(root->left->right)){
        root->left = rol_left(root->left);
    }
    return rol_right(root);
}

AVL_node* avl_fix(AVL_node* node){
    while(true){
        avl_update(node);
        uint32_t l = avl_depth(node->left), r = avl_depth(node->right);
        AVL_node **from = NULL;
        if(node->parent) from = (node->parent->left == node) ? &node->parent->left : &node->parent->right;
        if(l == r + 2) node = fix_right(node);
        else if(r == l+2) node = fix_left(node);
        if(!from) return node;
        *from = node;
        node = node->parent;
    }
}

AVL_node* avl_del(AVL_node* node){
    if(node->left == NULL && node->right == NULL){
        if(node->parent){
            if(node == node->parent->left) node->parent->left = NULL;
            else node->parent->right = NULL;
            return avl_fix(node->parent);
        }
        else return NULL;
    }    
    else if(node->left == NULL){
        if(node->parent){
            if(node == node->parent->left) node->parent->left = node->right;
            else node->parent->right = node->right;
            node->right->parent = node->parent;
            return avl_fix(node->parent);
        }
        else{
            node->right->parent = NULL;
            return node->right;
        }
    }
    else if(node->right == NULL){
        if(node->parent){
            if(node == node->parent->left) node->parent->left = node->left;
            else node->parent->right = node->left;
            node->left->parent = node->parent;
            return avl_fix(node->parent);
        }
        else {
            node->left->parent = NULL;
            return node->left;
        }
    }
    else{
        AVL_node* victom = node->right;
        while(victom->left) victom = victom->left;
        AVL_node* root = avl_del(victom);
        memcpy(victom, node, sizeof(AVL_node));
        node->left->parent = victom;
        if(node->right) node->right->parent = victom;
        if(node->parent){
            (node->parent->left == node ? node->parent->left : node->parent->right) = victom;
            return root;
        }
        else return victom;
    }
}

AVL_node* avl_offset(AVL_node* node, int64_t offset){
    int64_t pos = 0;
    while(offset != pos){
        if(pos > offset && avl_cnt(node->left) >= (pos - offset)){
            node = node->left;
            pos -= avl_cnt(node->right) + 1; 
        }
        else if(pos < offset && avl_cnt(node->right) >= (offset - pos)){
            node = node->right;
            pos += avl_cnt(node->left) + 1;
        }
        else{
            if(node->parent == NULL) return NULL;
            AVL_node* parent = node->parent;
            if(node == parent->right){
                pos -= avl_cnt(node->left) + 1;
                node = node->parent;
            }
            else{
                pos += avl_cnt(node->right) + 1;
                node = node->parent;
            }
        }
    }
    return node;
}