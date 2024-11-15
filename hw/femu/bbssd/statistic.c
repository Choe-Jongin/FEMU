#include <glib.h>
#include <stdio.h>
#include "statistic.h"

void statistic_init(struct statistic *statistic)
{
    statistic->size = 1024; // initial size   
    statistic->sec = 0;
    statistic->tot = g_malloc0(sizeof(struct time_unit));
    statistic->pre = g_malloc0(sizeof(struct time_unit)*statistic->size);
    statistic->cur = &statistic->pre[statistic->sec];
}
void statistic_delete(struct statistic *statistic)
{
    g_free(statistic->tot);
    g_free(statistic->pre);
}
void user_read(struct statistic *statistic)
{
    statistic->tot->us_read++;
    statistic->cur->us_read++;
}
void user_write(struct statistic *statistic)
{
    statistic->tot->us_write++;
    statistic->cur->us_write++;
}
void gc_write(struct statistic *statistic)
{
    statistic->tot->gc_write++;
    statistic->cur->gc_write++;
}
void wl_read(struct statistic *statistic)
{
    statistic->tot->wl_read++;
    statistic->cur->wl_read++;
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
    statistic->tot->gc++;
    statistic->cur->gc++;
}
void inc_chip_gc(struct statistic *statistic)
{
    statistic->tot->chip_gc++;
    statistic->cur->chip_gc++;
}
struct time_unit get_previous_statistic(struct statistic *statistic, int back_time, int period, int avg){
    struct time_unit ret = {0};
    int count = 0;
    int start = statistic->sec >= back_time ? statistic->sec - back_time : 0;
    for( int i = start; i < start+period ; i++ ){
        if(i > statistic->sec){
            break;
        }
        struct time_unit *it = &statistic->pre[i];
        ret.iops        += it->iops;     
        ret.gc          += it->gc;       
        ret.chip_gc     += it->chip_gc;
        ret.us_read     += it->us_read;
        ret.us_write    += it->us_write;
        ret.gc_read     += it->gc_read;
        ret.gc_write    += it->gc_write;
        ret.wl_read     += it->wl_read;
        ret.wl_write    += it->wl_write;
        count++;
    }

    if( avg ){
        ret.iops        /= count;  
        ret.gc          /= count;  
        ret.chip_gc     /= count;
        ret.us_read     /= count;
        ret.us_write    /= count;
        ret.gc_read     /= count;
        ret.gc_write    /= count;
        ret.wl_read     /= count;
        ret.wl_write    /= count;
    }

    return ret;
}

void one_clock(struct statistic *statistics, int num)
{
    for( int i = 0; i < num; i++ ){
        struct statistic *statistic = &statistics[i];
        if(statistic->sec == statistic->size - 1){
            statistic->size *= 2;
            statistic->pre = g_realloc(statistic->pre, sizeof(struct time_unit)*statistic->size);
        }
        statistic->sec++;
        statistic->cur = &statistic->pre[statistic->sec];
        memset(statistic->cur, 0, sizeof(struct time_unit));
    }
}
void flush_to_file(struct statistic *statistics, int num)
{
    char buff[4096];
    unsigned long tot_us_read = 0;
    unsigned long tot_us_write = 0;
    unsigned long tot_gc_write = 0;
    unsigned long tot_wl_read = 0;
    unsigned long tot_wl_write = 0;
    unsigned long tot_chip_gc = 0;
    FILE *fp;
    if( statistics[0].sec == 0){
        fp = fopen("data.txt", "w");

        fprintf(fp, "[time ]");
        for( int i = 0; i < num + 1; i++ ){
            fprintf(fp, " read  write gcwrt wlred wlwrt lungc |");
        }
        fprintf(fp, "\n");
    }else{
        fp = fopen("data.txt", "a");
    }
    memset(buff, 0, sizeof(buff));
    sprintf(buff+strlen(buff), "[%5d] ", statistics[0].sec+1);
    for( int i = 0; i < num; i++ ){
        struct time_unit *u = statistics[i].cur;
        tot_us_read+=u->us_read;
        tot_us_write+=u->us_write;
        tot_gc_write+=u->gc_write;
        tot_wl_read+=u->wl_read;
        tot_wl_write+=u->wl_write;
        sprintf(buff+strlen(buff),"%5ld %5ld %5ld %5ld %5ld %5ld |", 
            u->us_read, u->us_write, u->gc_write, u->wl_read, u->wl_write, u->chip_gc);
    }
    sprintf(    buff+strlen(buff),"%5ld %5ld %5ld %5ld %5ld %5ld", 
        tot_us_read, tot_us_write, tot_gc_write, tot_wl_read, tot_wl_write, tot_chip_gc);
    
    fprintf(fp, "%s\n", buff);
    fclose(fp);
}