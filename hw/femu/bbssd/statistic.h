#ifndef __FEMU_STATISITC_H
#define __FEMU_STATISITC_H

struct time_unit{
    unsigned long iops;
    unsigned long gc;
    unsigned long chip_gc;
    unsigned long us_read;
    unsigned long us_write;
    unsigned long gc_read;
    unsigned long gc_write;
    unsigned long wl_read;
    unsigned long wl_write;
};

struct statistic{
    struct time_unit *tot;
    struct time_unit *pre;
    struct time_unit *cur;
    int sec;
    int size;
};

void statistic_init(struct statistic *statistic);
void statistic_delete(struct statistic *statistic);
void user_write(struct statistic *statistic);
void user_read(struct statistic *statistic);
void gc_write(struct statistic *statistic);
void wl_read(struct statistic *statistic);
void wl_write(struct statistic *statistic);
void inc_iops(struct statistic *statistic);
void inc_gc(struct statistic *statistic);
void inc_chip_gc(struct statistic *statistic);
struct time_unit get_previous_statistic(struct statistic *statistic, int back_time, int period, int avg);
void one_clock(struct statistic *statistics, int num);
void flush_to_file(struct statistic *statistics, int num);

#endif