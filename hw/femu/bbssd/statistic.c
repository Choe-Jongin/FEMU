#include <glib.h>
#include <stdio.h>
#include "statistic.h"

void statistic_init(struct statistic *statistic)
{
    statistic->tot = g_malloc0(sizeof(struct time_unit));
    statistic->pre = g_malloc0(sizeof(struct time_unit));
    statistic->cur = g_malloc0(sizeof(struct time_unit));
    statistic->sec = 1;
}
void one_clock(struct statistic *statistic)
{
    struct time_unit *temp;
    temp = statistic->pre;
    statistic->pre = statistic->cur;
    statistic->cur = temp;

    statistic->cur->iops = 0;
    statistic->cur->gcps = 0;
    statistic->cur->us_write = 0;
    statistic->cur->gc_write = 0;
    statistic->cur->wl_write = 0;
    statistic->cur->us_read = 0;
    statistic->cur->gc_read = 0;
}
void user_write(struct statistic *statistic)
{
    statistic->tot->us_write++;
    statistic->cur->us_write++;
}
void user_read(struct statistic *statistic)
{
    statistic->tot->us_read++;
    statistic->cur->us_read++;
}
void gc_write(struct statistic *statistic)
{
    statistic->tot->gc_write++;
    statistic->cur->gc_write++;
}
void wl_write(struct statistic *statistic)
{
    statistic->tot->wl_write++;
    statistic->cur->wl_write++;
}
void inc_iops(struct statistic *statistic)
{
    statistic->tot->iops++;
    statistic->cur->iops++;
}
void inc_gc(struct statistic *statistic)
{
    statistic->tot->gcps++;
    statistic->cur->gcps++;
}
void flush_to_file(struct statistic *statistics, int num, int sec)
{
    char buff[4096];
    unsigned long tot_us_read = 0;
    unsigned long tot_us_write = 0;
    unsigned long tot_gc_write = 0;
    unsigned long tot_wl_write = 0;
    FILE *fp;
    if( sec == 1){
        fp = fopen("data.txt", "w");
        fprintf(fp, "[ time] ");
        for( int i = 0; i < num; i++ ){
            fprintf(fp, "read  write gcwrt wlwrt | ");
        }
        fprintf(fp, "tot_r tot_w totgw\n");
    }else{
        fp = fopen("data.txt", "a");
    }

    memset(buff, 0, sizeof(buff));
    sprintf(buff+strlen(buff), "[%5d] ", sec);
    for( int i = 0; i < num; i++ ){
        struct time_unit *u = statistics[i].cur;
        statistics[i].sec = sec;
        tot_us_read+=u->us_read;
        tot_us_write+=u->us_write;
        tot_gc_write+=u->gc_write;
        tot_wl_write+=u->wl_write;
        sprintf(buff+strlen(buff),"%5ld %5ld %5ld %5ld | ", u->us_read, u->us_write, u->gc_write, u->wl_write);
    }
    sprintf(buff+strlen(buff),"%5ld %5ld %5ld %5ld", tot_us_read, tot_us_write, tot_gc_write, tot_wl_write);
    
    fprintf(fp, "%s\n", buff);
    fclose(fp);
}