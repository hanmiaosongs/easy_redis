#include<stdio.h>
#include<stdlib.h>
#include"hashtable.h"

const uint32_t one_resize = 200;

static void msg(const char* msg){
    fprintf(stderr, "hashtable error :%s\n", msg);
}

static int32_t ht_init(H_table* tb, size_t n){
    if(n == 0 || (n & (n-1))) return -1;
    tb->code_mask = n-1;
    
    tb->h_tb = (H_node**)calloc(n, sizeof(H_node*));
    if(tb->h_tb == NULL) return -1;
    
    tb->tb_size = 0;
    return 0;
}

size_t hm_size(H_map* map){
    return map->tb_1->tb_size + map->tb_2->tb_size;
}

int32_t hm_init(size_t n, H_map* hmap){
    hmap->resizing_pos = 0;
    if(ht_init(hmap->tb_1, n)) return -1;
    return 0;
}

void ht_insert(H_table* tb, H_node* key){
    size_t lc = key->code & tb->code_mask;
    key->next = tb->h_tb[lc];
    tb->h_tb[lc] = key;
    tb->tb_size += 1;
}

static H_node* ht_detect(H_table* ht, H_node** from){
    H_node *node = *from;
    (*from) = (*from)->next;
    ht->tb_size -= 1;
    return node;
}

static int32_t do_resize(H_map* hmap){
    size_t lc = hmap->resizing_pos;
    H_node** from = &(hmap->tb_2->h_tb[lc]), *now;
    for(int i = 0; (i < one_resize) && hmap->tb_2->tb_size > 0; i++){
        while(!(*from)){
            lc += 1;
            from = &(hmap->tb_2->h_tb[lc]);
        }
        now = ht_detect(hmap->tb_2, from);
        ht_insert(hmap->tb_1, now);
    }

    if(hmap->tb_2->tb_size == 0){
        hmap->resizing_pos = 0;
        free(hmap->tb_2->h_tb);
        hmap->tb_2->code_mask = 0;
        hmap->tb_2->tb_size = 0;
    }
    else hmap->resizing_pos = lc;

    return 0;
}

static int32_t start_resizing(H_map* hmap){
    hmap->tb_2->h_tb = hmap->tb_1->h_tb;
    hmap->tb_2->code_mask = hmap->tb_1->code_mask;
    hmap->tb_2->tb_size = hmap->tb_1->tb_size;
    size_t nx_size = (hmap->tb_2->code_mask + 1) * 2;
    if(ht_init(hmap->tb_1, nx_size)) return -1;
    hmap->resizing_pos = 0;
    if(do_resize(hmap)) return -1;
    return 0;    
}

void hm_insert(H_map* hmap, H_node* key){
    ht_insert(hmap->tb_1, key);
    if(hmap->tb_2->code_mask) do_resize(hmap);
    else if(hmap->tb_1->code_mask < hmap->tb_1->tb_size){
        if(start_resizing(hmap)){
            msg("calloc error!");
            exit(1);
        }
    }
}

static H_node* ht_find(H_table* ht, H_node* key, bool (*cmp)(H_node*, H_node*)){
    size_t lc = (key->code) & (ht->code_mask);
    H_node* now = ht->h_tb[lc];
    while(now){
        if(cmp(now, key)){
            return now;
        }
        now = now->next;
    }
    return NULL;
}

H_node* hm_lookup(H_map* hmap, H_node* key, bool(*cmp)(H_node*, H_node*)){
    if(hmap->tb_2->code_mask) do_resize(hmap);

    H_node* now = ht_find(hmap->tb_1, key, cmp);
    if(now) return now;
    else if(hmap->tb_2->code_mask){
        now = ht_find(hmap->tb_2, key, cmp);
    }
    return now;
}

static H_node* ht_del(H_table* ht, H_node* key, bool(*cmp)(H_node*, H_node*)){
    size_t lc = ht->code_mask & key->code;
    H_node** now = &(ht->h_tb[lc]), *ans;
    if((*now) == NULL) return NULL;
    while((*now)){
        if(cmp((*now), key)){
            ans = (*now);
            (*now) = (*now)->next;
            ht->tb_size -= 1;
            return ans;
        }
        now = &((*now)->next);
    }
    return NULL;
}

H_node* hm_pop(H_map* hmap, H_node* key, bool(*cmp)(H_node*, H_node*)){
    if(hmap->tb_2->code_mask) do_resize(hmap);
    
    H_node* res;
    if((res = ht_del(hmap->tb_1, key, cmp)) != NULL) return res;
    if(hmap->tb_2->code_mask && (res = ht_del(hmap->tb_2, key, cmp)) != NULL) return res;
    return NULL;
}

void hm_distroy(H_map* hmap){
    free(hmap->tb_1);
    free(hmap->tb_2);
    free(hmap);
}