#include<stdint.h>
#include<stddef.h>

struct H_node{
    struct H_node* next = NULL;
    uint32_t code = 0;
};

struct H_table{
    size_t tb_size = 0, code_mask = 0;
    struct H_node** h_tb = NULL;
};

struct H_map{
    uint32_t resizing_pos = 0;
    struct H_table *tb_1 = new(H_table), *tb_2 = new(H_table);
};

H_node* hm_lookup(H_map* hmap, H_node* key, bool (*cmp)(H_node*, H_node*));
H_node* hm_pop(H_map* hmap, H_node* key, bool (*cmp)(H_node*, H_node*));
void hm_insert(H_map *hmap, H_node* key);
int32_t hm_init(size_t n, H_map* hmap);
void hm_distroy(H_map* hmap);