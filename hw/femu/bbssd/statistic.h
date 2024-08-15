#ifndef __FEMU_STATISITC_H
#define __FEMU_STATISITC_H

struct time_unit{
    unsigned long iops;
    unsigned long gcps;
    unsigned long us_write;
    unsigned long gc_write;
    unsigned long wl_write;
    unsigned long us_read;
    unsigned long gc_read;
};

struct statistic{
    struct time_unit *tot;
    
    struct time_unit *pre;
    struct time_unit *cur;
    int sec;
};

void statistic_init(struct statistic *statistic);
void one_clock(struct statistic *statistic);
void user_write(struct statistic *statistic);
void user_read(struct statistic *statistic);
void gc_write(struct statistic *statistic);
void wl_write(struct statistic *statistic);
void inc_iops(struct statistic *statistic);
void inc_gc(struct statistic *statistic);
void flush_to_file(struct statistic *statistics, int num, int sec);
#endif