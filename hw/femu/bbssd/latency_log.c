#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "latency_log.h"

// 새로운 레이턴시 리스트 생성
struct lat_list *new_lat_list(void)
{
    struct lat_list *ret = malloc(sizeof(struct lat_list));
    memset(ret, 0, sizeof(struct lat_list));
    ret->heap_capacity = INITIAL_HEAP_SIZE;  // 초기 힙 용량 설정
    ret->heap = malloc(ret->heap_capacity * sizeof(struct lat_node));  // 초기 힙 할당
    ret->heap_size = 0;
    ret->normal_io = 0;
    ret->tail_io = 0;
    return ret;
}

// 레이턴시 리스트 삭제
void delete_lat_list(struct lat_list *list)
{
    memset(list->buckets, 0, sizeof(int) * BUCKET_SIZE);
    free(list->heap);  // 힙 메모리 해제
    free(list);
}

// 힙에 새로운 요소 삽입
void insert_heap(struct lat_list *list, int lat)
{
    // 기존에 동일한 lat 값이 있는지 확인
    for (int i = 0; i < list->heap_size; i++) {
        if (list->heap[i].lat == lat) {
            list->heap[i].count++;  // 동일한 값이 있으면 count 증가
            return;
        }
    }

    if (list->heap_size >= list->heap_capacity) {
        // 힙이 가득 찬 경우 크기를 늘림
        resize_heap(list);
    }
    // 새 요소를 힙의 마지막에 추가
    list->heap[list->heap_size].lat = lat;
    list->heap[list->heap_size].count = 1;
    list->heap_size++;
    heapify_up(list, list->heap_size - 1);
}

// 힙 크기를 동적으로 늘리는 함수
void resize_heap(struct lat_list *list)
{
    list->heap_capacity *= 2;  // 힙 용량을 두 배로 늘림
    list->heap = realloc(list->heap, list->heap_capacity * sizeof(struct lat_node));
    if (list->heap == NULL) {
        fprintf(stderr, "Error reallocating memory for heap\n");
        exit(EXIT_FAILURE);
    }
}

// 힙 위로 재정렬 (삽입 시)
void heapify_up(struct lat_list *list, int index)
{
    if (index == 0) return;  // 루트 노드인 경우 종료
    int parent = (index - 1) / 2;
    if (list->heap[parent].lat > list->heap[index].lat) {
        // 부모보다 작으면 교체
        struct lat_node temp = list->heap[parent];
        list->heap[parent] = list->heap[index];
        list->heap[index] = temp;
        heapify_up(list, parent);
    }
}

// 힙 아래로 재정렬 (삭제 시)
void heapify_down(struct lat_list *list, int index)
{
    int left = 2 * index + 1;
    int right = 2 * index + 2;
    int smallest = index;

    if (left < list->heap_size && list->heap[left].lat < list->heap[smallest].lat) {
        smallest = left;
    }
    if (right < list->heap_size && list->heap[right].lat < list->heap[smallest].lat) {
        smallest = right;
    }
    if (smallest != index) {
        struct lat_node temp = list->heap[smallest];
        list->heap[smallest] = list->heap[index];
        list->heap[index] = temp;
        heapify_down(list, smallest);
    }
}

// 힙에서 가장 작은 요소 제거
void delete_min(struct lat_list *list)
{
    if (list->heap_size == 0) return;
    list->heap[0] = list->heap[list->heap_size - 1];  // 마지막 요소를 루트로 이동
    list->heap_size--;
    heapify_down(list, 0);
}

// 레이턴시 추가 함수
void add_lat(struct lat_list *list, unsigned long lat_ns)
{
    int lat = (int)(lat_ns / 1000);

    if (lat != 0 && lat < BUCKET_SIZE) { 
        list->normal_io++;
        list->buckets[lat]++;
    } else {
        list->tail_io++;
        // 힙에 lat 값 삽입
        insert_heap(list, lat);
    }
}

// 로그 파일로 레이턴시 기록
void lat_to_file(struct lat_list *list, int num, int tail)
{
    FILE *fp;
    char filename[32];
    char buff[1024*1024];
    int len;

    memset(buff, 0, sizeof(buff));
    len = 0;
    if( tail ){
        sprintf(filename, "ns%d.taillat", num + 1);
    }else{
        sprintf(filename, "ns%d.lat", num + 1);
    }
    fp = fopen(filename, "w");

    /* normal lat io */
    for (int i = 0; i < BUCKET_SIZE; i++) {
        if (list->buckets[i] == 0) {
            continue;
        }
        char lat_str[32];
        sprintf(lat_str, "%d %d\n", i, list->buckets[i]);
        sprintf(buff + len, "%s", lat_str);
        len += strlen(lat_str);
        if (len + 32 > 1024*1024) {
            fprintf(fp, "%s", buff);
            memset(buff, 0, sizeof(buff));
            len = 0;
        }
    }

    /* tail lat io (힙 순회) */
    for (int i = 1; i < list->heap_size; i++) {
        char lat_str[32];
        sprintf(lat_str, "%d %d\n", list->heap[i].lat, list->heap[i].count);
        sprintf(buff + len, "%s", lat_str);
        len += strlen(lat_str);
        if (len + 32 > 1024*1024) {
            fprintf(fp, "%s", buff);
            memset(buff, 0, sizeof(buff));
            len = 0;
        }
    }
    fprintf(fp, "%s", buff);

    // fprintf(fp, "heap_size %d\n", list->heap_size);
    // fprintf(fp, "normal_io %d\n", list->normal_io);
    // fprintf(fp, "tail_io %d\n", list->tail_io);
    fclose(fp);
}
