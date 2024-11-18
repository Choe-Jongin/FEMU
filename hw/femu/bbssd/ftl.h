#ifndef __FEMU_FTL_H
#define __FEMU_FTL_H

#include "../nvme.h"

#define INVALID_PPA     (~(0ULL))
#define INVALID_LPN     (~(0ULL))
#define UNMAPPED_PPA    (~(0ULL))

enum {
    NAND_READ =  0,
    NAND_WRITE = 1,
    NAND_ERASE = 2,

    NAND_READ_LATENCY = 40000,
    NAND_PROG_LATENCY = 200000,
    NAND_ERASE_LATENCY = 2000000,
};

enum {
    USER_IO = 0,
    GC_IO = 1,
    WL_IO = 2,
};

enum {
    SEC_FREE = 0,
    SEC_INVALID = 1,
    SEC_VALID = 2,

    PG_FREE = 0,
    PG_INVALID = 1,
    PG_VALID = 2,
};

enum {
    BLOCK_FREE = 0,
    BLOCK_OPEN = 1,
    BLOCK_FULL = 2,
};

enum {
    OFFSET = 0,
    SORTED = 1,
};

enum {
    FEMU_ENABLE_GC_DELAY = 1,
    FEMU_DISABLE_GC_DELAY = 2,

    FEMU_ENABLE_DELAY_EMU = 3,
    FEMU_DISABLE_DELAY_EMU = 4,

    FEMU_RESET_ACCT = 5,
    FEMU_ENABLE_LOG = 6,
    FEMU_DISABLE_LOG = 7,
};

enum {
    MODE_NORMAL = 0,
    MODE_ADAPTIVE = 1,
    MODE_SEAMLESS = 2,
};

#define BLK_BITS    (16)
#define PG_BITS     (16)
#define SEC_BITS    (8)
#define PL_BITS     (8)
#define LUN_BITS    (8)
#define CH_BITS     (7)

#define NS_TO_SEC(x) ((x)/(uint64_t)1000000000)
#define PAGE_TO_GB(x) (((x)*16)/1024/1024)

#define MAX_PE 600

/* describe a physical page addr */
struct ppa {
    union {
        struct {
            uint64_t blk : BLK_BITS;
            uint64_t pg  : PG_BITS;
            uint64_t sec : SEC_BITS;
            uint64_t pl  : PL_BITS;
            uint64_t lun : LUN_BITS;
            uint64_t ch  : CH_BITS;
            uint64_t rsv : 1;
        } g;

        uint64_t ppa;
    };
};

typedef int nand_sec_status_t;

struct nand_page {
    nand_sec_status_t *sec;
    int nsecs;
    int status;
};

typedef struct nand_block {
    struct nand_page *pg;
    int npgs;
    int ipc; /* invalid page count */
    int vpc; /* valid page count */
    int erase_cnt;
    int wp; /* current write pointer */
    int state;
    bool swapped; /* for swapping */

    QTAILQ_ENTRY(nand_block) entry;
    size_t pos;

    struct ppa ppa;
}nand_block;

struct nand_plane {
    struct nand_block *blk;
    int nblks;

    QTAILQ_HEAD(free_block_list, nand_block) free_block_list;
    int free_block_cnt;

    struct ppa ppa;
};

struct nand_lun {
    struct nand_plane *pl;
    int npls;
    uint64_t next_lun_avail_time;
    bool busy;
    uint64_t gc_endtime;

    int wp; /* current write block pointer */
    bool chip_gc_now;

    /* for multi namespace */
    pqueue_t *victim_block_pq;
    int victim_block_cnt;

    /* for swapping */
    int seamless_stage;
    int erase_count;
    int erase_count_after_swap;

    struct ppa ppa;
};

struct ssd_channel {
    struct nand_lun *lun;
    int nluns;
    uint64_t next_ch_avail_time;
    bool busy;
    uint64_t gc_endtime;
    struct ppa ppa;
};

struct ssdparams {
    int secsz;        /* sector size in bytes */
    int secs_per_pg;  /* # of sectors per page */
    int pgs_per_blk;  /* # of NAND pages per block */
    int blks_per_pl;  /* # of blocks per plane */
    int pls_per_lun;  /* # of planes per LUN (Die) */
    int luns_per_ch;  /* # of LUNs per channel */
    int nchs;         /* # of channels in the SSD */

    int pg_rd_lat;    /* NAND page read latency in nanoseconds */
    int pg_wr_lat;    /* NAND page program latency in nanoseconds */
    int blk_er_lat;   /* NAND block erase latency in nanoseconds */
    int ch_xfer_lat;  /* channel transfer latency for one page in nanoseconds
                       * this defines the channel bandwith
                       */

    double gc_thres_pcent;
    double gc_thres_pcent_high;
    bool enable_gc_delay;

    /* below are all calculated values */
    int secs_per_blk; /* # of sectors per block */
    int secs_per_pl;  /* # of sectors per plane */
    int secs_per_lun; /* # of sectors per LUN */
    int secs_per_ch;  /* # of sectors per channel */
    int tt_secs;      /* # of sectors in the SSD */

    int pgs_per_pl;   /* # of pages per plane */
    int pgs_per_lun;  /* # of pages per LUN (Die) */
    int pgs_per_ch;   /* # of pages per channel */
    int tt_pgs;       /* total # of pages in the SSD */

    int blks_per_lun; /* # of blocks per LUN */
    int blks_per_ch;  /* # of blocks per channel */
    int tt_blks;      /* total # of blocks in the SSD */

    int pls_per_ch;   /* # of planes per channel */
    int tt_pls;       /* total # of planes in the SSD */

    int tt_luns;      /* total # of LUNs in the SSD */

    uint64_t max_swap_time;
};

typedef struct swap_task{
    uint64_t swap_timer;
    struct ssd *ssd;
    struct NvmeNamespace *ns1, *ns2;
    struct nand_lun *lun1, *lun2;
    QTAILQ_ENTRY(swap_task) entry;

    uint64_t swap_start_time;
    uint64_t block_start_time;
}swap_task;

struct swap_mgmt{
    QTAILQ_HEAD(task_list, swap_task) *task_list;
    struct NvmeNamespace *ns1, *ns2;
    uint64_t swap_start_time;
    uint64_t block_start_time;

    /* for seamless*/
    uint64_t swap_delay;
    uint64_t original_iops;

    int initial_monitoring;
    uint64_t swap_frequency;
    uint64_t next_swap_time;
    uint64_t swap_status;

    int now_swapping;
    int mode;
    int waiting_seamless;
};

struct nand_cmd {
    int type;
    int cmd;
    int64_t stime; /* Coperd: request arrival time */
};

struct ssd {
    char *ssdname;
    struct ssdparams sp;
    struct ssd_channel *ch;
    struct ppa *maptbl; /* page level mapping table */
    uint64_t *rmap;     /* reverse mapptbl, assume it's stored in OOB */

    /* lockless ring for communication with NVMe IO thread */
    struct rte_ring **to_ftl;
    struct rte_ring **to_poller;
    bool *dataplane_started_ptr;
    QemuThread ftl_thread;

    /* CAST LAB */
    uint64_t start_log_time;
    uint64_t next_log_time;
    struct statistic *statistics;   // statistic list
    struct swap_mgmt swap_mgmt;     // for swap management
    int mode; //

    FILE *logfile;
};
void ns_init(FemuCtrl *n, NvmeNamespace *ns);
void ssd_init(FemuCtrl *n);
void swap_channel(struct NvmeNamespace *ns1, int ch1, struct NvmeNamespace *ns2, int ch2);
void start_swap(struct ssd *ssd, struct NvmeNamespace *namespaces, int num_namespaces);
void ssd_dsm(struct NvmeNamespace *ns, uint64_t slba, uint64_t nlb);
#define FEMU_DEBUG_FTL
#ifdef FEMU_DEBUG_FTL
#define ftl_debug(fmt, ...) \
    do { printf("[FEMU] FTL-Dbg: " fmt, ## __VA_ARGS__); } while (0)
#else
#define ftl_debug(fmt, ...) \
    do { } while (0)
#endif

#define ftl_err(fmt, ...) \
    do { fprintf(stderr, "[FEMU] FTL-Err: " fmt, ## __VA_ARGS__); } while (0)

#define ftl_log(fmt, ...) \
    do { printf("[FEMU] FTL-Log: " fmt, ## __VA_ARGS__); } while (0)


/* FEMU assert() */
#ifdef FEMU_DEBUG_FTL
#define ftl_assert(expression) assert(expression)
#else
#define ftl_assert(expression)
#endif

#endif
