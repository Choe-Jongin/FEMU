#ifndef LATENCY_LOG_H
#define LATENCY_LOG_H

#define BUCKET_SIZE 16384
#define INITIAL_HEAP_SIZE 16384  // 힙의 초기 크기 설정

struct lat_node {
    int lat;
    int count;
};

struct lat_list {
    int buckets[BUCKET_SIZE];  // 1us 단위로 저장
    struct lat_node *heap;     // 동적으로 할당될 힙 배열
    int heap_size;             // 힙에 저장된 요소의 수
    int heap_capacity;         // 힙의 현재 용량
    int normal_io;
    int tail_io;
};

// 힙 관련 함수 선언
void insert_heap(struct lat_list *list, int lat);
void heapify_up(struct lat_list *list, int index);
void heapify_down(struct lat_list *list, int index);
void delete_min(struct lat_list *list);
void resize_heap(struct lat_list *list); 

//조작 관련 함수
struct lat_list *new_lat_list(void);
void delete_lat_list(struct lat_list *list);
void add_lat(struct lat_list *list, unsigned long lat_ns);
void lat_to_file(struct lat_list *list, int num, int tail);

#endif // LATENCY_LOG_H
