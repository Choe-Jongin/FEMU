#include "ftl.h"

//#define FEMU_DEBUG_FTL

static void *ftl_thread(void *arg);
void monitoring_to_file(struct ssd *ssd);
void analyze(struct ssd *ssd, struct NvmeNamespace *namespaces, int num_namespaces);
static void free_block(struct NvmeNamespace *ns, struct ppa *ppa);

uint64_t ftl_start = 0;
QemuMutex swap_mutex;

/* Must be dependent on Namespace policy */ 
static void set_ns_start_index(struct NvmeNamespace *ns)
{
    int start_lun = 0;
    int start_lpn = 0;
    for( int i = 0 ; i < ns->id-1 ; i++){
        uint64_t luns = ns->ctrl->namespaces[i].np.tt_luns;
        uint64_t pgs = ns->ssd->sp.pgs_per_lun;
        start_lun += luns;
        start_lpn += pgs*luns;
    }
    ns->start_lpn = start_lpn;
}

static void assign_luns_to_ns(FemuCtrl *n)
{   
    struct ssdparams *spp = &n->ssd->sp;
    NvmeNamespace *ns = NULL;
    int nsid = 1;
    int lun_index;

    ns = &n->namespaces[nsid-1];
    lun_index = 0;

    for( int i = 0 ; i < spp->nchs ; i++){
        for( int j = 0 ; j < spp->luns_per_ch ; j++){
            struct nand_lun *lun = &n->ssd->ch[i].lun[j];
            /* assign to next ns */
            if( lun_index == ns->nluns ){
                if( nsid++ == n->num_namespaces ){
                    return;
                }
                ns = &n->namespaces[nsid-1];
                lun_index = 0;
            }

            ns->lun_list[lun_index++] = lun;
        }
    }
}

/* Maping Table Functions */
static inline struct ppa get_maptbl_ent(struct NvmeNamespace *ns, uint64_t lpn)
{
    struct ssd *ssd = (struct ssd*)ns->ssd;
    uint64_t lpn_margin = ns->start_lpn;
    return ssd->maptbl[lpn+lpn_margin];
}

static inline void set_maptbl_ent(struct NvmeNamespace *ns, uint64_t lpn, struct ppa *ppa)
{
    struct ssd *ssd = (struct ssd*)ns->ssd;
    uint64_t lpn_margin = ns->start_lpn;
    ssd->maptbl[lpn+lpn_margin] = *ppa;
}

static uint64_t ppa2pgidx(struct ssd *ssd, struct ppa *ppa)
{
    struct ssdparams *spp = &ssd->sp;
    uint64_t pgidx;

    pgidx = ppa->g.ch  * spp->pgs_per_ch  + \
            ppa->g.lun * spp->pgs_per_lun + \
            ppa->g.pl  * spp->pgs_per_pl  + \
            ppa->g.blk * spp->pgs_per_blk + \
            ppa->g.pg;

    ftl_assert(pgidx < spp->tt_pgs);

    return pgidx;
}

static inline uint64_t get_rmap_ent(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct ssd *ssd = (struct ssd*)ns->ssd;
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    return ssd->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct NvmeNamespace *ns, uint64_t lpn, struct ppa *ppa)
{
    struct ssd *ssd = (struct ssd*)ns->ssd;
    uint64_t pgidx = ppa2pgidx(ssd, ppa);

    ssd->rmap[pgidx] = lpn;
}

static inline int victim_block_cmp_pri(pqueue_pri_t next, pqueue_pri_t curr)
{
    return (next > curr);
}

static inline pqueue_pri_t victim_block_get_pri(void *a)
{
    return ((struct nand_block *)a)->vpc;
}

static inline void victim_block_set_pri(void *a, pqueue_pri_t pri)
{
    ((struct nand_block *)a)->vpc = pri;
}

static inline size_t victim_block_get_pos(void *a)
{
    return ((struct nand_block *)a)->pos;
}

static inline void victim_block_set_pos(void *a, size_t pos)
{
    ((struct nand_block *)a)->pos = pos;
}

static inline void check_addr(int a, int max)
{
    ftl_assert(a >= 0 && a < max);
}

static void check_params(struct ssdparams *spp)
{
    /*
     * we are using a general write pointer increment method now, no need to
     * force luns_per_ch and nchs to be power of 2
     */

    //ftl_assert(is_power_of_2(spp->luns_per_ch));
    //ftl_assert(is_power_of_2(spp->nchs));
}


static void ssd_init_params(struct ssdparams *spp, FemuCtrl *n)
{
    spp->secsz = n->bb_params.secsz;
    spp->secs_per_pg = n->bb_params.secs_per_pg;
    spp->pgs_per_blk = n->bb_params.pgs_per_blk;
    spp->blks_per_pl = n->bb_params.blks_per_pl;
    spp->pls_per_lun = n->bb_params.pls_per_lun;
    spp->luns_per_ch = n->bb_params.luns_per_ch;
    spp->nchs = n->bb_params.nchs;

    spp->pg_rd_lat = n->bb_params.pg_rd_lat;
    spp->pg_wr_lat = n->bb_params.pg_wr_lat;
    spp->blk_er_lat = n->bb_params.blk_er_lat;
    spp->ch_xfer_lat = n->bb_params.ch_xfer_lat;
    spp->gc_thres_pcent         = n->bb_params.gc_thres_pcent/100.0f;
    spp->gc_thres_pcent_high    = n->bb_params.gc_thres_pcent_high/100.0f;

    /* calculated values */
    spp->secs_per_blk = spp->secs_per_pg * spp->pgs_per_blk;
    spp->secs_per_pl = spp->secs_per_blk * spp->blks_per_pl;
    spp->secs_per_lun = spp->secs_per_pl * spp->pls_per_lun;
    spp->secs_per_ch = spp->secs_per_lun * spp->luns_per_ch;
    spp->tt_secs = spp->secs_per_ch * spp->nchs;

    spp->pgs_per_pl = spp->pgs_per_blk * spp->blks_per_pl;
    spp->pgs_per_lun = spp->pgs_per_pl * spp->pls_per_lun;
    spp->pgs_per_ch = spp->pgs_per_lun * spp->luns_per_ch;
    spp->tt_pgs = spp->pgs_per_ch * spp->nchs;

    spp->blks_per_lun = spp->blks_per_pl * spp->pls_per_lun;
    spp->blks_per_ch = spp->blks_per_lun * spp->luns_per_ch;
    spp->tt_blks = spp->blks_per_ch * spp->nchs;

    spp->pls_per_ch =  spp->pls_per_lun * spp->luns_per_ch;
    spp->tt_pls = spp->pls_per_ch * spp->nchs;

    spp->tt_luns = spp->luns_per_ch * spp->nchs;

    spp->enable_gc_delay        = true;

    spp->max_swap_time = ((uint64_t)spp->pgs_per_blk*(spp->pg_rd_lat + spp->pg_wr_lat) + spp->blk_er_lat)*spp->blks_per_lun;

    check_params(spp);
}
 
static void namespace_init_params(struct namespace_params *npp, struct ssdparams *spp, int nluns)
{
    npp->tt_secs        = spp->secs_per_lun * nluns;
    npp->tt_pgs         = spp->pgs_per_lun  * nluns;
    npp->tt_blks        = spp->blks_per_lun * nluns;
    npp->tt_pls         = spp->pls_per_ch   * nluns;
    npp->tt_luns        = nluns;

    npp->gc_thres_blocks        = (int)((1 - spp->gc_thres_pcent)       * npp->tt_blks);
    npp->chip_gc_thres_blocks   = (int)((1 - spp->gc_thres_pcent_high)  * spp->blks_per_lun);
}

static struct nand_block *get_next_free_block(struct nand_lun *lun)
{
    struct nand_plane *pl = &lun->pl[0];
    struct nand_block *curr_block = NULL;

    curr_block = QTAILQ_FIRST(&pl->free_block_list);
    if (!curr_block) {
        femu_log("No free block here!! ch%d chip%d \r\n", lun->ppa.g.ch, lun->ppa.g.lun);
        return NULL;
    }

    QTAILQ_REMOVE(&pl->free_block_list, curr_block, entry);
    pl->free_block_cnt--;

    curr_block->state = BLOCK_OPEN;
    return curr_block;
}

static void ssd_init_nand_page(struct nand_page *pg, struct ssdparams *spp)
{
    pg->nsecs = spp->secs_per_pg;
    pg->sec = g_malloc0(sizeof(nand_sec_status_t) * pg->nsecs);
    for (int i = 0; i < pg->nsecs; i++) {
        pg->sec[i] = SEC_FREE;
    }
    pg->status = PG_FREE;
}

static void ssd_init_nand_blk(struct nand_block *blk, struct ssdparams *spp)
{
    blk->npgs = spp->pgs_per_blk;
    blk->pg = g_malloc0(sizeof(struct nand_page) * blk->npgs);
    for (int i = 0; i < blk->npgs; i++) {
        ssd_init_nand_page(&blk->pg[i], spp);
    }
    blk->ipc = 0;
    blk->vpc = 0;
    blk->erase_cnt = 0;
    blk->wp = 0;
    blk->state = BLOCK_FREE;
    blk->swapped = false;
}

static void ssd_init_nand_plane(struct nand_plane *pl, struct ssdparams *spp)
{
    pl->nblks = spp->blks_per_pl;
    pl->blk = g_malloc0(sizeof(struct nand_block) * pl->nblks);

    QTAILQ_INIT(&pl->free_block_list);
    
    for (int i = 0; i < pl->nblks; i++) {
        pl->blk[i].ppa.ppa = pl->ppa.ppa;
        pl->blk[i].ppa.g.blk = i;
        ssd_init_nand_blk(&pl->blk[i], spp);
        QTAILQ_INSERT_TAIL(&pl->free_block_list, &pl->blk[i], entry);
        pl->free_block_cnt++;
    }
}

static void ssd_init_nand_lun(struct nand_lun *lun, struct ssdparams *spp)
{
    lun->npls = spp->pls_per_lun;
    lun->pl = g_malloc0(sizeof(struct nand_plane) * lun->npls);
    for (int i = 0; i < lun->npls; i++) {
        lun->pl[i].ppa.ppa = lun->ppa.ppa;
        lun->pl[i].ppa.g.pl = i;
        ssd_init_nand_plane(&lun->pl[i], spp);
    }
    lun->next_lun_avail_time = 0;
    lun->busy = false;

    lun->victim_block_pq = pqueue_init(spp->pgs_per_blk, victim_block_cmp_pri,
            victim_block_get_pri, victim_block_set_pri,
            victim_block_get_pos, victim_block_set_pos);

    lun->wp = get_next_free_block(lun)->ppa.g.blk;
}

static void ssd_init_ch(struct ssd_channel *ch, struct ssdparams *spp)
{
    ch->nluns = spp->luns_per_ch;
    ch->lun = g_malloc0(sizeof(struct nand_lun) * ch->nluns);
    for (int i = 0; i < ch->nluns; i++) {
        ch->lun[i].ppa.ppa = ch->ppa.ppa;
        ch->lun[i].ppa.g.lun = i;
        ssd_init_nand_lun(&ch->lun[i], spp);
    }
    ch->next_ch_avail_time = 0;
    ch->busy = 0;
}

static void ssd_init_maptbl(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->maptbl = g_malloc0(sizeof(struct ppa) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++) {
        ssd->maptbl[i].ppa = UNMAPPED_PPA;
    }
}

static void ssd_init_rmap(struct ssd *ssd)
{
    struct ssdparams *spp = &ssd->sp;

    ssd->rmap = g_malloc0(sizeof(uint64_t) * spp->tt_pgs);
    for (int i = 0; i < spp->tt_pgs; i++) {
        ssd->rmap[i] = INVALID_LPN;
    }
}

void ns_init(FemuCtrl *n, NvmeNamespace *ns)
{
    struct ssd *ssd = n->ssd;
    struct ssdparams *spp = &ssd->sp;

    ns->ssd = ssd;
    namespace_init_params(&ns->np, spp, ns->nluns);
    ns->lun_list = g_malloc0(sizeof(struct nand_lun*) * ns->nluns);

    set_ns_start_index(ns);
}

static void ssd_init_swap_mgmt(struct ssd *ssd){
    ssd->swap_mgmt.task_list = g_malloc0(sizeof(QTAILQ_HEAD(task_list, swap_task)));
    QTAILQ_INIT(ssd->swap_mgmt.task_list);

    ssd->swap_mgmt.now_swapping = false;
    ssd->swap_mgmt.swap_status = 0;
    ssd->swap_mgmt.swap_frequency = 0;
    ssd->swap_mgmt.next_swap_time = 0;
}

void ssd_init(FemuCtrl *n)
{
    struct ssd *ssd = n->ssd;
    struct ssdparams *spp = &ssd->sp;

    ftl_assert(ssd);

    /* init statistic module */
    ssd->statistics = g_malloc0(sizeof(struct statistic)*n->num_namespaces);

    ssd_init_params(spp, n);
    for( int  i = 0; i < n->num_namespaces ; i ++){
        ns_init(n, &n->namespaces[i]);
        statistic_init(&ssd->statistics[i]);
        n->namespaces[i].statistic = &ssd->statistics[i];
    }

    /* initialize ssd internal layout architecture */
    ssd->ch = g_malloc0(sizeof(struct ssd_channel) * spp->nchs);
    for (int i = 0; i < spp->nchs; i++) {
        ssd->ch[i].ppa.ppa = 0;
        ssd->ch[i].ppa.g.ch = i;
        ssd_init_ch(&ssd->ch[i], spp);
    }

    /* assign chips to each chip */
    assign_luns_to_ns(n);

    /* print lun infomation */
    for( int nsid = 1 ; nsid <= n->num_namespaces ; nsid++){
        NvmeNamespace *ns = &n->namespaces[nsid-1];
        printf("physical:%ldByte, nluns:%d", (long)ns->np.tt_secs*(long)ssd->sp.secsz, ns->nluns);
        for( int i = 0 ; i < ns->nluns ; i++){
            printf(" | ch%2d,lun%d", ns->lun_list[i]->ppa.g.ch, ns->lun_list[i]->ppa.g.lun);
        }
        printf("\r\n");
    }

    ssd->mode = MODE_NORMAL;

    ssd->logfile = fopen("log.txt","w");
    fclose(ssd->logfile);

    usleep(1000000);

    /* initialize maptbl */
    ssd_init_maptbl(ssd);

    /* initialize rmap */
    ssd_init_rmap(ssd);

    /* initialize swap_mgmt */
    ssd_init_swap_mgmt(ssd);

    qemu_thread_create(&ssd->ftl_thread, "FEMU-FTL-Thread", ftl_thread, n,
                       QEMU_THREAD_JOINABLE);
}

static inline bool valid_ppa(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct ssd *ssd = (struct ssd *)ns->ssd;
    struct ssdparams *spp = &ssd->sp;
    int ch = ppa->g.ch;
    int lun = ppa->g.lun;
    int pl = ppa->g.pl;
    int blk = ppa->g.blk;
    int pg = ppa->g.pg;
    int sec = ppa->g.sec;

    if (ch >= 0 && ch < spp->nchs && lun >= 0 && lun < spp->luns_per_ch && pl >=
        0 && pl < spp->pls_per_lun && blk >= 0 && blk < spp->blks_per_pl && pg
        >= 0 && pg < spp->pgs_per_blk && sec >= 0 && sec < spp->secs_per_pg)
        return true;

    return false;
}

static inline bool valid_lpn(struct NvmeNamespace *ns, uint64_t lpn)
{
    return (lpn < ns->np.tt_pgs);
}

static inline bool mapped_ppa(struct ppa *ppa)
{
    return !(ppa->ppa == UNMAPPED_PPA);
}

static inline struct ssd_channel *get_ch(struct ssd *ssd, struct ppa *ppa)
{
    return &(ssd->ch[ppa->g.ch]);
}

static inline struct nand_lun *get_lun(struct ssd *ssd, struct ppa *ppa)
{
    struct ssd_channel *ch = get_ch(ssd, ppa);
    return &(ch->lun[ppa->g.lun]);
}

static inline struct nand_plane *get_pl(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_lun *lun = get_lun(ssd, ppa);
    return &(lun->pl[ppa->g.pl]);
}

static inline struct nand_block *get_blk(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_plane *pl = get_pl(ssd, ppa);
    return &(pl->blk[ppa->g.blk]);
}

static inline struct nand_page *get_pg(struct ssd *ssd, struct ppa *ppa)
{
    struct nand_block *blk = get_blk(ssd, ppa);
    return &(blk->pg[ppa->g.pg]);
}

static uint64_t ssd_advance_status(struct ssd *ssd, struct ppa *ppa, struct
        nand_cmd *ncmd)
{
    int c = ncmd->cmd;
    uint64_t cmd_stime = (ncmd->stime == 0) ? \
        qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;
    uint64_t nand_stime;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun *lun = get_lun(ssd, ppa);
    uint64_t lat = 0;

    switch (c) {
    case NAND_READ:
        /* read: perform NAND cmd first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->pg_rd_lat;
        lat = lun->next_lun_avail_time - cmd_stime;
        break;

    case NAND_WRITE:
        /* write: transfer data through channel first */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        if (ncmd->type == USER_IO) {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;
        } else {
            lun->next_lun_avail_time = nand_stime + spp->pg_wr_lat;
        }
        lat = lun->next_lun_avail_time - cmd_stime;

        break;

    case NAND_ERASE:
        /* erase: only need to advance NAND status */
        nand_stime = (lun->next_lun_avail_time < cmd_stime) ? cmd_stime : \
                     lun->next_lun_avail_time;
        lun->next_lun_avail_time = nand_stime + spp->blk_er_lat;
        lat = lun->next_lun_avail_time - cmd_stime;
        break;

    default:
        ftl_err("Unsupported NAND command: 0x%x\n", c);
    }

    return lat;
}

static struct ppa get_new_page(struct NvmeNamespace *ns)
{
    struct nand_lun *cur_lun = ns->lun_list[ns->write_lun];
    struct ppa ppa;
    ppa.ppa     = cur_lun->ppa.ppa;
    ppa.g.pl    = 0;
    ppa.g.blk   = cur_lun->wp;
    ppa.g.pg    = cur_lun->pl[ppa.g.pl].blk[ppa.g.blk].wp;
    return ppa;
}

static int do_chip_gc(struct NvmeNamespace *ns, struct nand_lun *lun)
{
    struct nand_block *victim_block = NULL;

    /* select victim block in chip (no in namespace) */
    victim_block = pqueue_peek(lun->victim_block_pq);
    if (!victim_block) {
        ftl_err("failed victim block select in intra chip gc\r\n");
        return -1;
    }

    if (victim_block->pos) {
        victim_block = pqueue_pop(lun->victim_block_pq);
        victim_block->pos = 0;
        lun->victim_block_cnt--;
    }

    inc_chip_gc(ns->statistic);
    /* make free block */
    free_block(ns, &victim_block->ppa);
    return 0;
}

static void ns_advance_write_lun(struct NvmeNamespace *ns)
{
    /* increase wp by channel-first, lun-second */
    while(1){
        if( ++ns->write_lun == ns->nluns){
            ns->write_lun = 0;
        }

        // condition check
        if( ns->lun_list[ns->write_lun]->wp == -1 ){
            continue;
        }

        break;
    }
}

static void lun_advance_write_pointer(struct NvmeNamespace *ns, struct nand_lun *curr_lun)
{
    struct nand_block *curr_block = NULL;
    struct nand_block *next_block = NULL;

    curr_block = &curr_lun->pl[0].blk[curr_lun->wp];
    curr_block->wp++;

    /* page over in block */
    if (curr_block->wp == ns->ssd->sp.pgs_per_blk) {
        curr_block->state = BLOCK_FULL;
        pqueue_insert(curr_lun->victim_block_pq, curr_block);
        curr_lun->victim_block_cnt++;

        next_block = get_next_free_block(curr_lun);
        if (next_block == NULL) {           // no free block in chip
            femu_log("[ CAST ] ERR! no free block in ch%d lun%d\r\n", curr_lun->ppa.g.ch, curr_lun->ppa.g.lun);
            curr_lun->wp = -1;              // no write pointer
            ns_advance_write_lun(ns);       // 재귀참조 방지
            if( do_chip_gc(ns, curr_lun) != -1 ){ // so make free block
                next_block = get_next_free_block(curr_lun);
            }       
        }

        // swap 도중이면 새로 할당 받은 블럭이 swap된 블럭으로 표시
        if (curr_lun->seamless_stage != 0 && next_block != NULL) {
            next_block->swapped = true;
        }

        if (next_block != NULL) {
            curr_lun->wp = next_block->ppa.g.blk;
        }

        for(int i=0; i < ns->ssd->sp.pgs_per_blk ; i++)
            ftl_assert(curr_lun->pl[0].blk[curr_lun->wp].pg[i].status == PG_FREE);
    }
}

static void ssd_advance_write_pointer(struct NvmeNamespace *ns)
{
    lun_advance_write_pointer(ns, ns->lun_list[ns->write_lun]);
    ns_advance_write_lun(ns);
}

/* update SSD status about one page */
static void mark_page_invalid(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct nand_lun *lun = NULL;
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;

    /* update corresponding page status */
    pg = get_pg(ns->ssd, ppa);
    ftl_assert(pg->status == PG_VALID);
    pg->status = PG_INVALID;

    /* update corresponding block status */
    lun = get_lun(ns->ssd, ppa);
    blk = get_blk(ns->ssd, ppa);
    blk->ipc++;
    if ( blk->pos ) {
        pqueue_change_priority(lun->victim_block_pq, blk->vpc - 1, blk);
    }else{
        blk->vpc--;
    }
}

static void mark_page_valid(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;

    /* update page status */
    pg = get_pg(ns->ssd, ppa);
    ftl_assert(pg->status == PG_FREE);
    pg->status = PG_VALID;

    /* update corresponding block status */
    blk = get_blk(ns->ssd, ppa);
    ftl_assert(blk->vpc >= 0 && blk->vpc < ns->ssd->sp.pgs_per_blk);
    blk->vpc++;
}

static void mark_block_free(struct ssd *ssd, struct ppa *ppa)
{
    // struct nand_lun *lun = get_lun(ssd, ppa);
    struct nand_plane *pl = get_pl(ssd, ppa);
    struct nand_block *blk = get_blk(ssd, ppa);
    struct nand_page *pg = NULL;

    for (int i = 0; i < ssd->sp.pgs_per_blk; i++) {
        /* reset page status */
        pg = &blk->pg[i];
        ftl_assert(pg->nsecs == ssd->sp.secs_per_pg);
        pg->status = PG_FREE;
    }

    QTAILQ_INSERT_TAIL(&pl->free_block_list, blk, entry);
    pl->free_block_cnt++;

    /* reset block status */
    ftl_assert(blk->npgs == ssd->sp.pgs_per_blk);
    blk->ipc = 0;
    blk->vpc = 0;
    blk->wp = 0;
    blk->erase_cnt++;
    blk->state = BLOCK_FREE;
}

static void gc_read_page(NvmeNamespace *ns, struct ppa *ppa)
{
    /* advance ssd status, we don't care about how long it takes */
    if (ns->ssd->sp.enable_gc_delay) {
        struct nand_cmd gcr;
        gcr.type = GC_IO;
        gcr.cmd = NAND_READ;
        gcr.stime = 0;
        ssd_advance_status(ns->ssd, ppa, &gcr);
    }
}

/* move valid page data (already in DRAM) from victim block to a new page */
static uint64_t gc_write_page(struct NvmeNamespace *ns, struct ppa *old_ppa)
{
    struct ppa new_ppa;
    struct nand_lun *new_lun;
    uint64_t lpn = get_rmap_ent(ns, old_ppa);

    ftl_assert(valid_lpn(ns, lpn));
    new_ppa = get_new_page(ns);
    /* update maptbl */
    set_maptbl_ent(ns, lpn, &new_ppa);
    /* update rmap */
    set_rmap_ent(ns, lpn, &new_ppa);

    mark_page_valid(ns, &new_ppa);

    /* need to advance the write pointer here */
    ssd_advance_write_pointer(ns);

    if (ns->ssd->sp.enable_gc_delay) {
        struct nand_cmd gcw;
        gcw.type = GC_IO;
        gcw.cmd = NAND_WRITE;
        gcw.stime = 0;
        ssd_advance_status(ns->ssd, &new_ppa, &gcw);
    }

    new_lun = get_lun(ns->ssd, &new_ppa);
    new_lun->gc_endtime = new_lun->next_lun_avail_time;

    gc_write(ns->statistic);

    return 0;
}

static struct nand_block *select_victim_block(struct NvmeNamespace *ns, bool force)
{
    struct nand_lun *lun = NULL;
    struct nand_block *victim_block = NULL;
    int min_vpc = ns->ssd->sp.pgs_per_blk;
    int lun_index = -1;
    
    /* search minimum vpc block across all luns in ns */
    for( int i = 0; i < ns->nluns; i++ ){
        lun = ns->lun_list[i];
        victim_block = pqueue_peek(lun->victim_block_pq);
        if( victim_block != NULL && victim_block->vpc < min_vpc){
            min_vpc = victim_block->vpc;
            lun_index = i;
        }
    }

    if (lun_index == -1) {
        femu_log("[ CAST ] victim_block_pq is empty\n");
        return NULL;
    }

    lun = ns->lun_list[lun_index];
    victim_block = pqueue_peek(lun->victim_block_pq);
    if (!force && victim_block->ipc < ns->ssd->sp.pgs_per_blk / 8) { 
        femu_log("[ CAST ] Failed to select victim block ns%d vpc%d ipc%d \r\n",ns->id, victim_block->vpc, victim_block->ipc);
        return NULL;
    }  

    victim_block = pqueue_pop(lun->victim_block_pq);
    victim_block->pos = 0;
    lun->victim_block_cnt--;
    return victim_block;
}

/* here ppa identifies the block we want to clean */
static void clean_one_block(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct ssd *ssd = ns->ssd;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun *lun = NULL;
    struct nand_block *block = NULL;
    struct nand_page *pg_iter = NULL;
    int cnt = 0;

    lun = get_lun(ssd, ppa);
    block = get_blk(ssd, ppa);
    if(lun->seamless_stage != 0 && block->swapped == false) {
        if( ns == ssd->swap_mgmt.ns1){
            ns = ssd->swap_mgmt.ns2;
        }else if( ns == ssd->swap_mgmt.ns2){
            ns = ssd->swap_mgmt.ns1;
        }
    }
    for (int pg = 0; pg < spp->pgs_per_blk; pg++) {
        ppa->g.pg = pg;
        pg_iter = get_pg(ssd, ppa);
        /* there shouldn't be any free page in victim blocks */
        if (pg_iter->status == PG_VALID) {
            gc_read_page(ns, ppa);
            /* delay the maptbl update until "write" happens */
            gc_write_page(ns, ppa);
            cnt++;
        }
    }

    ftl_assert(get_blk(ns->ssd, ppa)->vpc == cnt);
}

static void free_block(struct NvmeNamespace *ns, struct ppa *ppa)
{   
    struct ssd *ssd = ns->ssd;
    struct ssdparams *spp = &ns->ssd->sp;
    struct nand_lun *lunp;

    lunp = get_lun(ssd, ppa);
    clean_one_block(ns, ppa);
    mark_block_free(ssd, ppa);

    if (spp->enable_gc_delay) {
        struct nand_cmd gce;
        gce.type = GC_IO;
        gce.cmd = NAND_ERASE;
        gce.stime = 0;
        ssd_advance_status(ssd, ppa, &gce);
    }

    lunp->erase_count++;
    lunp->erase_count_after_swap++;
    lunp->gc_endtime = lunp->next_lun_avail_time;
}

static int do_gc(struct NvmeNamespace *ns, bool force)
{
    struct nand_block *victim_block = NULL;

    victim_block = select_victim_block(ns, force);
    if (!victim_block) {
        return -1;
    }

    free_block(ns, &victim_block->ppa);
    return 0;
}

static inline bool should_gc(struct NvmeNamespace *ns)
{
    struct nand_lun *lun;
    int free_block_cnt = 0;

    for (int i = 0; i < ns->nluns; i++){
        lun = ns->lun_list[i];
        free_block_cnt += lun->pl[0].free_block_cnt;
    }
    return (free_block_cnt <= ns->np.gc_thres_blocks);
}

static inline int should_chip_gc(struct NvmeNamespace *ns)
{
    for (int i = 0; i < ns->nluns; i++){
        if( ns->lun_list[i]->pl[0].free_block_cnt < ns->np.chip_gc_thres_blocks )
            return i;
    }
    return -1;
}

static uint64_t ssd_read(struct ssd *ssd, NvmeRequest *req)
{
    struct NvmeNamespace * ns = req->ns;
    struct ssdparams *spp = &ns->ssd->sp;
    struct namespace_params *npp = &ns->np;
    uint64_t lba = req->slba;
    int nsecs = req->nlb;
    struct ppa ppa;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + nsecs - 1) / spp->secs_per_pg;
    uint64_t lpn;
    uint64_t sublat, maxlat = 0;

    if (end_lpn >= npp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\r\n", start_lpn, ns->np.tt_pgs);
    }

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        ppa = get_maptbl_ent(ns, lpn);
        if (!mapped_ppa(&ppa) || !valid_ppa(ns, &ppa)) {
            // printf("%s,lpn(%" PRId64 ") not mapped to valid ppa\n", ssd->ssdname, lpn);
            // printf("Invalid ppa,ch:%d,lun:%d,blk:%d,pl:%d,pg:%d,sec:%d\n",
            // ppa.g.ch, ppa.g.lun, ppa.g.blk, ppa.g.pl, ppa.g.pg, ppa.g.sec);
            continue;
        }        
        struct nand_cmd srd;
        srd.type = USER_IO;
        srd.cmd = NAND_READ;
        srd.stime = req->stime;
        sublat = ssd_advance_status(ns->ssd, &ppa, &srd);
        maxlat = (sublat > maxlat) ? sublat : maxlat;

        user_read(ns->statistic);
        inc_iops(ns->statistic);
    }

    return maxlat;
}

static uint64_t ssd_write(struct ssd *ssd, NvmeRequest *req)
{
    uint64_t lba = req->slba;
    struct NvmeNamespace * ns = req->ns;
    struct ssdparams *spp = &ns->ssd->sp;
    struct namespace_params *npp = &ns->np;
    int len = req->nlb;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + len - 1) / spp->secs_per_pg;
    struct ppa ppa;
    uint64_t lpn;
    uint64_t curlat = 0, maxlat = 0;
    int r;

    if (end_lpn >= npp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\r\n", start_lpn, ns->np.tt_pgs);
    }

    while ((r = should_chip_gc(ns)) >=0 ) {
        /* perform GC here until !should_gc(ssd) */
        r = do_chip_gc(ns, ns->lun_list[r]);
        if (r == -1)
            break;
    }

    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        ppa = get_maptbl_ent(ns, lpn);

        if (mapped_ppa(&ppa)) {
            /* update old page information first */
            mark_page_invalid(ns, &ppa);
            set_rmap_ent(ns, INVALID_LPN, &ppa);
        }

        /* new write */
        ppa = get_new_page(ns);
        /* update maptbl */
        set_maptbl_ent(ns, lpn, &ppa);
        /* update rmap */
        set_rmap_ent(ns, lpn, &ppa);

        mark_page_valid(ns, &ppa);

        /* need to advance the write pointer here */
        ssd_advance_write_pointer(ns);

        struct nand_cmd swr;
        swr.type = USER_IO;
        swr.cmd = NAND_WRITE;
        swr.stime = req->stime;
        /* get latency statistics */
        curlat = ssd_advance_status(ns->ssd, &ppa, &swr);
        maxlat = (curlat > maxlat) ? curlat : maxlat;

        user_write(ns->statistic);
        inc_iops(ns->statistic);
    }

    return maxlat;
}

static uint64_t ssd_dsm(struct ssd *ssd, NvmeRequest *req)
{
    struct NvmeNamespace *ns = req->ns;
    struct ssdparams *spp = &ssd->sp;
    struct ppa ppa;
    uint64_t lpn;
    int i;
    uint64_t curlat = 0, maxlat = 0;

    for (i = 0; i < req->nr; i++) {
        uint64_t slba = le64_to_cpu(req->range[i].slba);
        uint32_t nlb = le32_to_cpu(req->range[i].nlb);

        /* CAST Lab */
        uint64_t start_lpn = slba / spp->secs_per_pg;
        uint64_t end_lpn = (slba + nlb - 1) / spp->secs_per_pg;

        for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
            ppa = get_maptbl_ent(ns, lpn);
            if (mapped_ppa(&ppa)) {
                mark_page_invalid(ns, &ppa);
                set_rmap_ent(ns, INVALID_LPN, &ppa);
                ppa.ppa = UNMAPPED_PPA;
                set_maptbl_ent(ns, lpn, &ppa);
                curlat = 0; /* page invalidation latency */
                maxlat = (curlat > maxlat) ? curlat : maxlat;
            }
        }
    }
    g_free(req->range);

    return maxlat;
}

static void wl_read_page(NvmeNamespace *ns, struct ppa *ppa)
{
    struct nand_cmd wlr;
    wlr.type = WL_IO;
    wlr.cmd = NAND_READ;
    wlr.stime = 0;
    ssd_advance_status(ns->ssd, ppa, &wlr);
    wl_read(ns->statistic);
}

static void wp_block_close(struct NvmeNamespace *ns, struct nand_lun *lun)
{
    struct ssd *ssd = ns->ssd;
    struct nand_block * blk;
    struct nand_page *pg_iter;
    struct ppa ppa;

    ppa.ppa = lun->ppa.ppa;
    ppa.g.blk = lun->wp;
    blk = get_blk(ssd, &ppa);
    for (int pg = 0; pg < ssd->sp.pgs_per_blk; pg++)
    {
        ppa.g.pg = pg;
        pg_iter = get_pg(ssd, &ppa);
        if (pg_iter->status == PG_FREE)
        {
            pg_iter->status = PG_INVALID;
            blk->ipc++;
        }
    }
    blk->wp = ssd->sp.pgs_per_blk-1;
    lun_advance_write_pointer(ns, lun);
}

static struct nand_block *get_next_full_block(struct ssd *ssd, struct nand_lun *lun, int last_index)
{
    struct nand_block *next_full_block = NULL;
    for (int blk = last_index; blk < ssd->sp.blks_per_lun; blk++) {
        if( lun->pl[0].blk[blk].swapped == false 
         && lun->pl[0].blk[blk].state == BLOCK_FULL 
         && lun->pl[0].blk[blk].vpc > 0)
        {
            next_full_block = &lun->pl[0].blk[blk];

            pqueue_remove(lun->victim_block_pq, next_full_block);
            next_full_block->pos = 0;
            lun->victim_block_cnt--;
            break;
        }
    }
    return next_full_block;
}

static void finish_swap_lun(struct swap_task *task)
{
    struct ssd *ssd = task->ssd;
    struct nand_lun *lun1 = task->lun1;
    struct nand_lun *lun2 = task->lun2;
    struct ppa ppa1, ppa2;
    ppa1.ppa = lun1->ppa.ppa;
    ppa2.ppa = lun2->ppa.ppa;

    femu_log("[ CAST ]  %lu sec [ch%d, lun%d] <-> [ch%d, lun%d]  Done. \r\n", 
        NS_TO_SEC(qemu_clock_get_ns(QEMU_CLOCK_REALTIME)-task->block_start_time),
        task->lun1->ppa.g.ch, task->lun1->ppa.g.lun, task->lun2->ppa.g.ch, task->lun2->ppa.g.lun);

    lun1->seamless_stage = 0;
    lun2->seamless_stage = 0;
    for( int blk = 0 ; blk < ssd->sp.blks_per_lun ; blk++ ){
        ppa1.g.blk = blk;
        ppa2.g.blk = blk;
        get_blk(ssd, &ppa1)->swapped = false;
        get_blk(ssd, &ppa2)->swapped = false;
    }

    lun1->erase_count_after_swap = 0;
    lun2->erase_count_after_swap = 0;
}

static int read_one_block(struct NvmeNamespace *ns, struct nand_block *blk, uint64_t *lpn_buffer, int start, int len)
{
    struct ppa ppa;
    struct ssd *ssd = ns->ssd;
    struct ssdparams *spp = &ssd->sp;
    int count = 0;

    if( blk == NULL ){
        return 0;
    }

    ppa.ppa = blk->ppa.ppa;
    for (int pg = 0; pg < spp->pgs_per_blk; pg++) {
        ppa.g.pg = pg;
        if (get_pg(ssd, &ppa)->status == PG_VALID) {
            lpn_buffer[start+count++] = get_rmap_ent(ns, &ppa);
            wl_read_page(ns, &ppa);
            mark_page_invalid(ns, &ppa);
        }
        if( start+count == len )    // if len <= -1, read all pages
            break;
    }
    return count;
}

/* move valid page data (already in DRAM) from victim block to a new page */
static void wl_write_page(struct NvmeNamespace *ns, uint64_t lpn, struct nand_lun *write_lun)
{
    struct ppa ppa;

    ppa.ppa     = write_lun->ppa.ppa;
    ppa.g.blk   = write_lun->wp;
    ppa.g.pg    = write_lun->pl[0].blk[write_lun->wp].wp;

    ftl_assert(get_pg(ns->ssd, &ppa)->status == PG_FREE);
    
    ftl_assert(valid_lpn(ns, lpn));
    /* update maptbl */
    set_maptbl_ent(ns, lpn, &ppa);
    /* update rmap */
    set_rmap_ent(ns, lpn, &ppa);

    mark_page_valid(ns, &ppa);

    /* flash write cmd */
    struct nand_cmd wlw;
    wlw.type = WL_IO;
    wlw.cmd = NAND_WRITE;
    wlw.stime = 0;
    ssd_advance_status(ns->ssd, &ppa, &wlw);

    wl_write(ns->statistic);
    
    /* advance the write pointer */
    lun_advance_write_pointer(ns, write_lun);
}

/* adaptive block swap */
static void swap_block_adaptive( struct NvmeNamespace *ns1, struct nand_lun *lun1, struct nand_block *blk1, 
                                 struct NvmeNamespace *ns2, struct nand_lun *lun2, struct nand_block *blk2)
{
    struct ssd *ssd = ns1->ssd;
    struct nand_block *er_blk1, *er_blk2;
    uint64_t *lpn_buffer1, *lpn_buffer2;
    int lpn_idx1 = 0, lpn_idx2 = 0;
    int count = 0;
    lpn_buffer1 = g_malloc0(sizeof(uint64_t) * ssd->sp.pgs_per_blk);
    lpn_buffer2 = g_malloc0(sizeof(uint64_t) * ssd->sp.pgs_per_blk);

    er_blk1 = blk1;
    er_blk2 = blk2;

    lpn_idx1 = read_one_block(ns1, blk1, lpn_buffer1, 0, -1);
    lpn_idx2 = read_one_block(ns2, blk2, lpn_buffer2, 0, -1);

    while( lpn_idx1 != lpn_idx2 && blk1 != NULL && blk2 != NULL)
    {
        if(lpn_idx1 < lpn_idx2){
            count = lpn_idx2;
            blk1 = get_next_full_block(ssd, lun1, 0);
            lpn_idx1 += read_one_block(ns1, blk1, lpn_buffer1, lpn_idx1, count);
            if(blk1 != NULL && blk1 != er_blk1 ){
                pqueue_insert(lun1->victim_block_pq, blk1);
                lun1->victim_block_cnt++;
            }
        }else{
            count = lpn_idx1;
            blk2 = get_next_full_block(ssd, lun2, 0);
            lpn_idx2 += read_one_block(ns2, blk2, lpn_buffer2, lpn_idx2, count);
            if(blk2 != NULL && blk2 != er_blk2 ){
                pqueue_insert(lun2->victim_block_pq, blk2);
                lun2->victim_block_cnt++;
            }
        }
    }

    // wait for all page read
    if(blk1 != NULL && blk2 != NULL){
        if (lun1->next_lun_avail_time > lun2->next_lun_avail_time){
            lun2->next_lun_avail_time = lun1->next_lun_avail_time;
        }else{
            lun1->next_lun_avail_time = lun2->next_lun_avail_time;
        }    
    }

    if (er_blk1 != NULL) {
        free_block(ns2, &er_blk1->ppa);
    }
    if (er_blk2 != NULL) {
        free_block(ns1, &er_blk2->ppa);
    }

    /* move counterpart chip */
    for (int i = 0; i < lpn_idx1; i++){
        wl_write_page(ns1, lpn_buffer1[i], lun2);
    }
    for (int i = 0; i < lpn_idx2; i++){
        wl_write_page(ns2, lpn_buffer2[i], lun1);
    }

    g_free(lpn_buffer1);
    g_free(lpn_buffer2);
}

/* CAST : Swap two lun */
static uint64_t swap_lun_adaptive(struct swap_task *task)
{
    struct ssd *ssd = task->ssd;
    struct NvmeNamespace *ns1 = task->ns1;
    struct NvmeNamespace *ns2 = task->ns2;
    struct nand_lun *lun1 = task->lun1;
    struct nand_lun *lun2 = task->lun2;
    struct nand_block *blk1 = NULL;
    struct nand_block *blk2 = NULL;

    uint64_t later_avail = lun1->next_lun_avail_time > lun2->next_lun_avail_time ? lun1->next_lun_avail_time : lun2->next_lun_avail_time;
    lun1->next_lun_avail_time = lun2->next_lun_avail_time = later_avail;
    lun1->seamless_stage = 2;       // block swap stage
    lun2->seamless_stage = 2;       // block swap stage

    blk1 = get_next_full_block(ssd, lun1, 0);
    blk2 = get_next_full_block(ssd, lun2, 0);

    if( blk1 == NULL && blk2 == NULL ){
        return 0;
    }

    swap_block_adaptive(ns1, lun1, blk1, ns2, lun2, blk2);
    
    // wait block IO 
    later_avail = lun1->next_lun_avail_time > lun2->next_lun_avail_time ? lun1->next_lun_avail_time : lun2->next_lun_avail_time;
    if( blk1 != NULL && blk2 != NULL ){
        lun1->next_lun_avail_time = lun2->next_lun_avail_time = later_avail;
    }
    uint64_t next_swap_time = later_avail + (uint64_t)1*1000*1000;
    return next_swap_time;
}

/* normal block swap */
static void swap_block( struct NvmeNamespace *ns1, struct nand_lun *lun1, struct nand_block *blk1, 
                        struct NvmeNamespace *ns2, struct nand_lun *lun2, struct nand_block *blk2)
{
    struct ssd *ssd = ns1->ssd;
    uint64_t *lpn_buffer1, *lpn_buffer2;
    int lpn_idx1 = 0, lpn_idx2 = 0;
    lpn_buffer1 = g_malloc0(sizeof(uint64_t) * ssd->sp.pgs_per_blk);
    lpn_buffer2 = g_malloc0(sizeof(uint64_t) * ssd->sp.pgs_per_blk);

    lpn_idx1 = read_one_block(ns1, blk1, lpn_buffer1, 0, -1);
    lpn_idx2 = read_one_block(ns2, blk2, lpn_buffer2, 0, -1);

    // wait for all page read
    if( blk1 != NULL && blk2 != NULL){
        if (lun1->next_lun_avail_time > lun2->next_lun_avail_time){
            lun2->next_lun_avail_time = lun1->next_lun_avail_time;
        }else{
            lun1->next_lun_avail_time = lun2->next_lun_avail_time;
        }    
    }

    if (blk1 != NULL) {
        free_block(ns2, &blk1->ppa);
    }
    if (blk2 != NULL) {
        free_block(ns1, &blk2->ppa);
    }

    /* move counterpart chip */
    for (int i = 0; i < lpn_idx1; i++){
        wl_write_page(ns1, lpn_buffer1[i], lun2);
    }
    for (int i = 0; i < lpn_idx2; i++){
        wl_write_page(ns2, lpn_buffer2[i], lun1);
    }

    g_free(lpn_buffer1);
    g_free(lpn_buffer2);
}

/* CAST : Swap two lun */
static uint64_t swap_lun(struct swap_task *task)
{
    struct ssd *ssd = task->ssd;
    struct NvmeNamespace *ns1 = task->ns1;
    struct NvmeNamespace *ns2 = task->ns2;
    struct nand_lun *lun1 = task->lun1;
    struct nand_lun *lun2 = task->lun2;
    struct nand_block *blk1 = NULL;
    struct nand_block *blk2 = NULL;
    uint64_t later_avail = lun1->next_lun_avail_time > lun2->next_lun_avail_time ? lun1->next_lun_avail_time : lun2->next_lun_avail_time;
    lun1->next_lun_avail_time = lun2->next_lun_avail_time = later_avail;
    lun1->seamless_stage = 2;       // block swap stage
    lun2->seamless_stage = 2;       // block swap stage

    blk1 = get_next_full_block(ssd, lun1, 0);
    blk2 = get_next_full_block(ssd, lun2, 0);

    if( blk1 == NULL && blk2 == NULL ){
        return 0;
    }

    swap_block(ns1, lun1, blk1, ns2, lun2, blk2);

    // wait block IO 
    later_avail = lun1->next_lun_avail_time > lun2->next_lun_avail_time ? lun1->next_lun_avail_time : lun2->next_lun_avail_time;
    // lun1->next_lun_avail_time = lun2->next_lun_avail_time = later_avail;
    uint64_t next_swap_time = later_avail + (uint64_t)1*1000*1000;
    return next_swap_time;
}

/* CAST : Swap two channel */
void swap_channel(struct NvmeNamespace *ns1, int ch1, struct NvmeNamespace *ns2, int ch2)
{
    struct ssd *ssd = ns1->ssd;
    struct ssdparams *spp = &ssd->sp;
    struct nand_lun **swap_lun_list1;
    struct nand_lun **swap_lun_list2;

    qemu_mutex_lock(&swap_mutex);

    /* TODO : 같은 채널 예외처리 */
    // do something
    
    swap_lun_list1 = g_malloc0(sizeof(struct nand_lun*) * spp->luns_per_ch);
    swap_lun_list2 = g_malloc0(sizeof(struct nand_lun*) * spp->luns_per_ch);
    for( int i = 0 ; i < spp->luns_per_ch ; i++ ){
        swap_lun_list1[i] = ns1->lun_list[ch1*spp->luns_per_ch+i];
        swap_lun_list2[i] = ns2->lun_list[ch2*spp->luns_per_ch+i];
    }

    /* TODO : sort lun_list1 by erase count */
    // do something

    femu_log("[ CAST ] time:%ld Channel swap [ns%d, ch%d] <-> [ns%d, ch%d] mode : %d\r\n", NS_TO_SEC(qemu_clock_get_ns(QEMU_CLOCK_REALTIME) - ssd->start_log_time), ns1->id, ch1, ns2->id, ch2, ssd->mode);
    for( int i = 0 ; i < spp->luns_per_ch ; i++ ){
        femu_log("[ CAST ] Swap physical chip [ch%d, lun%d] <-> [ch%d, lun%d]\r\n", 
            swap_lun_list1[i]->ppa.g.ch, swap_lun_list1[i]->ppa.g.lun, swap_lun_list2[i]->ppa.g.ch, swap_lun_list2[i]->ppa.g.lun);
    }

    for( int i = 0 ; i < spp->luns_per_ch ; i++ ){
        swap_lun_list1[i]->seamless_stage = 1;       // seamless stage
        swap_lun_list2[i]->seamless_stage = 1;       // seamless stage
        wp_block_close(ns1, swap_lun_list1[i]);
        wp_block_close(ns2, swap_lun_list2[i]);

        /* meta swap */
        struct nand_lun *temp_lun = ns1->lun_list[ch1*spp->luns_per_ch+i];
        ns1->lun_list[ch1*spp->luns_per_ch+i] = ns2->lun_list[ch2*spp->luns_per_ch+i];
        ns2->lun_list[ch2*spp->luns_per_ch+i] = temp_lun;
    }

    ssd->logfile = fopen("log.txt","a");
    fprintf(ssd->logfile, "Channel swap [ns%d, ch%d] <-> [ns%d, ch%d] mode : %d\r\n", ns1->id, ch1, ns2->id, ch2, ssd->mode);
    fclose(ssd->logfile);

    /* setting swap manager */
    ssd->swap_mgmt.now_swapping = true;
    ssd->swap_mgmt.mode = ssd->mode;
    struct time_unit unit1 = get_previous_statistic(ns1->statistic, 60, 60, TRUE);
    struct time_unit unit2 = get_previous_statistic(ns2->statistic, 60, 60, TRUE);
    ssd->swap_mgmt.original_iops = unit1.iops + unit2.iops;

    /* regist new swap task */
    ssd->swap_mgmt.ns1 = ns1;
    ssd->swap_mgmt.ns2 = ns2;
    ssd->swap_mgmt.swap_start_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    ssd->swap_mgmt.block_start_time =  ssd->swap_mgmt.swap_start_time;

    if( ssd->mode == MODE_SEAMLESS ){
        ssd->swap_mgmt.swap_delay = spp->max_swap_time;
        ssd->swap_mgmt.waiting_seamless = 1;
    }else{
        ssd->swap_mgmt.swap_delay = 0;
        ssd->swap_mgmt.waiting_seamless = 0;
    }

    for( int i = 0 ; i < spp->luns_per_ch ; i++ ){
        struct swap_task *task = g_malloc0(sizeof(struct swap_task));
        task->ssd = ssd;
        task->ns1 = ns1;
        task->ns2 = ns2;
        task->lun1 = swap_lun_list1[i];
        task->lun2 = swap_lun_list2[i];
        task->swap_timer = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
        task->swap_start_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
        task->block_start_time = task->swap_timer;

        QTAILQ_INSERT_TAIL(ssd->swap_mgmt.task_list, task, entry); // for next tick
    }

    g_free(swap_lun_list1);
    g_free(swap_lun_list2);
    qemu_mutex_unlock(&swap_mutex);
}

void start_swap(struct ssd *ssd, struct NvmeNamespace *namespaces, int num_namespaces)
{
    struct NvmeNamespace *max_ns = NULL;
    struct NvmeNamespace *min_ns = NULL;
    int ch1 = 0, ch2 = 0;
    uint64_t max_pe = 0;
    uint64_t min_pe = MAX_PE*ssd->sp.blks_per_ch;

    // fine max channel
    for( int i = 0 ; i < num_namespaces ; i++){
        int nchs = namespaces[i].nluns/ssd->sp.luns_per_ch;
        for( int j = 0 ; j < nchs ; j++){
            int pe = 0;
            for( int k = 0; k < ssd->sp.luns_per_ch ; k++ )
                pe += namespaces[i].lun_list[j*ssd->sp.luns_per_ch + k]->erase_count_after_swap;
            if( max_pe < pe ){
                max_pe = pe;
                max_ns = &namespaces[i];
                ch1 = j;
            }
        }
    }

    // fine min channel
    for( int i = 0 ; i < num_namespaces ; i++){
        int nchs = namespaces[i].nluns/ssd->sp.luns_per_ch;
        for( int j = 0 ; j < nchs ; j++){
            int pe = 0;
            for( int k = 0; k < ssd->sp.luns_per_ch ; k++ )
                pe += namespaces[i].lun_list[j*ssd->sp.luns_per_ch + k]->erase_count;
            if( (&namespaces[i] != max_ns || ch1 != j) && min_pe > pe){
                min_pe = pe;
                min_ns = &namespaces[i];
                ch2 = j;
            }
        }
    }

    swap_channel(max_ns, ch1, min_ns, ch2);
}

void monitoring_to_file(struct ssd *ssd)
{
    char buff[32*1024];
    char str[32*1024];
    int len;
    FILE *fp = fopen("monitoring.txt", "w");

    memset(buff, 0, sizeof(buff));

    for( int ch = 0; ch < ssd->sp.nchs; ch++ ){
        for( int lun = 0; lun < ssd->sp.luns_per_ch; lun++ ){
            sprintf(str, "%2dch %dchip freeblk %-4d ", ch, lun, ssd->ch[ch].lun[lun].pl[0].free_block_cnt);
            sprintf(buff + len, "%s", str);
            len+=strlen(str);
            for( int blk = 0; blk < ssd->sp.blks_per_pl; blk++ ){
                int vpc_perdec = (ssd->ch[ch].lun[lun].pl[0].blk[blk].vpc*10)/(ssd->sp.pgs_per_blk+1);
                if(ssd->ch[ch].lun[lun].pl[0].blk[blk].state == BLOCK_FREE)
                    sprintf(str, "_ ");
                else if(ssd->ch[ch].lun[lun].pl[0].blk[blk].state == BLOCK_OPEN)
                    sprintf(str, "%1d<", vpc_perdec);
                else
                    sprintf(str, "%1d ", vpc_perdec);
                sprintf(buff + len, "%s", str);
                len+=strlen(str);
            }
            fprintf(fp, "%s\n", buff);
            memset(buff, 0, sizeof(buff));
            len = 0;
        }
    }
    fclose(fp);
}

void analyze(struct ssd *ssd, struct NvmeNamespace *namespaces, int num_namespaces)
{
    struct swap_mgmt *swap_mgmt = &ssd->swap_mgmt;
    FILE *fp = fopen("analyze.txt", "w");
    int max_pe = 0;
    float avg_pe = 0.0f;
    uint64_t total_time = NS_TO_SEC(qemu_clock_get_ns(QEMU_CLOCK_REALTIME) - ssd->start_log_time);
    fprintf(fp, "time : %ld\n", total_time);

    fprintf(fp, "     ");
    for( int i = 0; i < ssd->sp.nchs ; i++ ){
        fprintf(fp, " ch%-2d", i);
    }
    fprintf(fp, "\n");
    
    fprintf(fp, "PE   ");
    for( int i = 0; i < ssd->sp.nchs ; i++ ){
        int pe = 0;
        for( int j = 0; j < ssd->sp.luns_per_ch ; j++ )
            pe += ssd->ch[i].lun[j].erase_count;
        fprintf(fp, " %4d", pe/ssd->sp.blks_per_ch);
        if( pe > max_pe )
            max_pe = pe;
        avg_pe += (float)pe;
    } 
    avg_pe /= ssd->sp.tt_blks;
    fprintf(fp, "\n");
    
    fprintf(fp, "PEAS ");
    for( int i = 0; i < ssd->sp.nchs ; i++ ){
        int pe = 0;
        for( int j = 0; j < ssd->sp.luns_per_ch ; j++ )
            pe += ssd->ch[i].lun[j].erase_count_after_swap;
        fprintf(fp, " %4d", pe/ssd->sp.blks_per_ch);
    } 
    fprintf(fp, "\n");
    uint64_t ssd_dev_write = 0;
    for( int i = 0; i < num_namespaces ; i++ ){
        struct NvmeNamespace *ns = &namespaces[i];
        struct statistic *s = ns->statistic;
        struct time_unit prev = get_previous_statistic(s, 120, 60, TRUE);
        struct time_unit curr = get_previous_statistic(s, 60, 60, TRUE);
        int iops_rate = curr.iops*100 / (prev.iops != 0 ? prev.iops : 1);
        uint64_t dev_write = s->tot->us_write + s->tot->gc_write + s->tot->wl_write;
        ssd_dev_write += dev_write;
        float WAF = (float)dev_write / (s->tot->us_write+1.0f);
        
        fprintf(fp, "ns%d User_Write %ldGB GC_Write %ldGB WL_Write %ldGB Device_Write %ldGB WAF %.2f IOPS_Drop %d%% Waiting %d\n", ns->id,
                PAGE_TO_GB(s->tot->us_write), PAGE_TO_GB(s->tot->gc_write), PAGE_TO_GB(s->tot->wl_write), PAGE_TO_GB(dev_write),
                WAF, (int)iops_rate, ns->waiting_io);
    }
    
    uint64_t dev_wearout = (ssd_dev_write*ssd->sp.secs_per_pg*ssd->sp.secsz)/total_time;
    uint64_t ssd_total_life = (uint64_t)ssd->sp.tt_secs*ssd->sp.secsz*MAX_PE;

    /* 초기 모니터링이 끝남 */
    if( swap_mgmt->swap_status == 0 && avg_pe >= 0.5f){
        swap_mgmt->swap_frequency = ((ssd_total_life/(dev_wearout?dev_wearout:1))/160)*1000000000;
        swap_mgmt->next_swap_time = ssd->start_log_time + swap_mgmt->swap_frequency;
        swap_mgmt->swap_status = 1;
    }

    fprintf(fp,"Max PE %d AVG PE %.2f Imbalance %.2f \n", max_pe/ssd->sp.blks_per_ch, avg_pe, avg_pe!=0?(float)max_pe/avg_pe:1.0f);
    fprintf(fp,"Device Wearout %ldMB/s  Lifetime %ldmin  Swap Frequency %ld  Next Swap Time %ld \n", 
        dev_wearout/1024/1024, 
        ssd_total_life/(dev_wearout?dev_wearout:1)/60,
        NS_TO_SEC(swap_mgmt->swap_frequency),
        NS_TO_SEC(swap_mgmt->next_swap_time - ssd->start_log_time));
    fprintf(fp,"MODE :%d\n", ssd->mode);

    if(swap_mgmt->now_swapping){
        uint64_t curr_iops = get_previous_statistic(swap_mgmt->ns1->statistic, 60, 60, TRUE).iops + get_previous_statistic(swap_mgmt->ns2->statistic, 60, 60, TRUE).iops;
        float iops_drop = (float)curr_iops/(float)(swap_mgmt->original_iops+0.1f);
        fprintf(fp,"Original IOPS %ld\t Current IOPS %ld\t Drop%.2f%%\t swap_delay %ldsec\n", swap_mgmt->original_iops, curr_iops, iops_drop*100, NS_TO_SEC(swap_mgmt->swap_delay));
    }
    fclose(fp);
}

static void *ftl_thread(void *arg)
{
    FemuCtrl *n = (FemuCtrl *)arg;
    struct ssd *ssd = n->ssd;
    struct swap_mgmt *swap_mgmt = &ssd->swap_mgmt; 
    struct swap_task *task, *temp;
    NvmeRequest *req = NULL;
    uint64_t lat = 0;
    int rc;
    int i;

    ftl_start = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    ssd->start_log_time = ftl_start;
    ssd->next_log_time = ssd->start_log_time + 1*1000*1000*1000;
    while (!*(ssd->dataplane_started_ptr)) {
        usleep(100000);
    }

    qemu_mutex_init(&swap_mutex);

    /* FIXME: not safe, to handle ->to_ftl and ->to_poller gracefully */
    ssd->to_ftl = n->to_ftl;
    ssd->to_poller = n->to_poller;
    while (1) {

        qemu_mutex_lock(&swap_mutex);
        for (i = 1; i <= n->nr_pollers; i++) {
            if (!ssd->to_ftl[i] || !femu_ring_count(ssd->to_ftl[i]))
                continue;

            rc = femu_ring_dequeue(ssd->to_ftl[i], (void *)&req, 1);
            if (rc != 1) {
                printf("FEMU: FTL to_ftl dequeue failed\n");
            }

            ftl_assert(req);

            switch (req->cmd.opcode) {
            case NVME_CMD_WRITE:
                lat = ssd_write(ssd, req);
                break;
            case NVME_CMD_READ:
                lat = ssd_read(ssd, req);
                break;
            case NVME_CMD_DSM:
                lat = ssd_dsm(ssd, req);
                break;
            default:
                ftl_err("FTL received unkown request type, ERROR\n");
                break;
            }

            req->reqlat = lat;
            req->expire_time += lat;
            rc = femu_ring_enqueue(ssd->to_poller[i], (void *)&req, 1);
            if (rc != 1) {
                ftl_err("FTL to_poller enqueue failed\n");
            }

            if (should_gc(req->ns)) {
                do_gc(req->ns, false);
            }
        }
        qemu_mutex_unlock(&swap_mutex);

        /* 반복작업 */
        uint64_t now = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);

        if(swap_mgmt->swap_status == 1 && now > swap_mgmt->next_swap_time){
            // femu_log("now %ld next_swap_time %ld  logtime %ld next_swap_time %ld\n", 
            //     now, swap_mgmt->next_swap_time, 
            //     NS_TO_SEC(ssd->start_log_time),
            //     NS_TO_SEC(swap_mgmt->next_swap_time - ssd->start_log_time));
            start_swap(ssd, n->namespaces, n->num_namespaces);
            swap_mgmt->next_swap_time += swap_mgmt->swap_frequency;
        }

        /* swap_task 처리 */
        if( swap_mgmt->waiting_seamless ){
            uint64_t curr_iops = get_previous_statistic(swap_mgmt->ns1->statistic, 60, 60, TRUE).iops + get_previous_statistic(swap_mgmt->ns2->statistic, 60, 60, TRUE).iops;
            double iops_drop = (double)curr_iops/(double)(swap_mgmt->original_iops+0.1f);
            if(iops_drop < 1){
                iops_drop = iops_drop > 0.01f ? iops_drop : 0.01f;
                swap_mgmt->swap_delay = (uint64_t)(iops_drop*(double)ssd->sp.max_swap_time);
            }
            if(swap_mgmt->swap_start_time + swap_mgmt->swap_delay <= now){
                ssd->logfile = fopen("log.txt","a");
                fprintf(ssd->logfile, "ORIGIN IOPS %ld CURRENT IOPS %ld drop %.2f %ld %ld\n", swap_mgmt->original_iops, curr_iops, iops_drop, swap_mgmt->swap_delay, ssd->sp.max_swap_time);
                fclose(ssd->logfile);
                swap_mgmt->waiting_seamless = 0;
                swap_mgmt->swap_delay = 0;
            }
        }else{
            QTAILQ_FOREACH_SAFE(task, swap_mgmt->task_list, entry, temp) {
                if (task->swap_timer <= now) {
                    switch(ssd->mode) {
                        case  MODE_NORMAL:
                            task->swap_timer = swap_lun(task);
                            break;
                        case  MODE_ADAPTIVE:
                            task->swap_timer = swap_lun_adaptive(task);
                            break;
                        case  MODE_SEAMLESS:
                            task->swap_timer = swap_lun_adaptive(task);
                            break;
                    }
                }
                /* some task is fisished */
                if (task->swap_timer == 0) {
                    finish_swap_lun(task);
                    QTAILQ_REMOVE(swap_mgmt->task_list, task, entry);
                    free(task);

                    /* all tasks are finished */
                    if(QTAILQ_EMPTY(swap_mgmt->task_list)){
                        ssd->logfile = fopen("log.txt","a");
                        fprintf(ssd->logfile, "ns%d ns%d swap_start %ld block_swap_start %ld swap_time %ld block_swap_time %ld mode %d\n",
                            ssd->swap_mgmt.ns1->id, ssd->swap_mgmt.ns2->id, 
                            NS_TO_SEC(ssd->swap_mgmt.swap_start_time - ssd->start_log_time), 
                            NS_TO_SEC(ssd->swap_mgmt.block_start_time - ssd->start_log_time),
                            NS_TO_SEC(now - ssd->swap_mgmt.swap_start_time),
                            NS_TO_SEC(now - ssd->swap_mgmt.block_start_time),
                            ssd->mode);
                        fclose(ssd->logfile);

                        ssd->swap_mgmt.now_swapping = false;
                        ssd->swap_mgmt.ns1 = ssd->swap_mgmt.ns2 = NULL;
                    }
                }
            }
        }
        
        /* 0.1sec timer */
        if( now >= ssd->next_log_time ){
            switch ((now/(100*1000*1000))%10)
            {
            case 0:     // every x.0 sec
                /* flush to file */
                flush_to_file(ssd->statistics, n->num_namespaces);
                /* increse one second */
                one_clock(ssd->statistics, n->num_namespaces);
                break;
            case 1: // every x.1 sec
                    // lat
                for (int j = 0; j < n->num_namespaces; j++) {
                    lat_to_file(n->namespaces[j].lat_list, j, FALSE);
                }
                break;
            case 2: // every x.2 sec
                    // tail lat
                for (int j = 0; j < n->num_namespaces; j++) {
                    lat_to_file(n->namespaces[j].swap_lat_list, j, TRUE);
                }
                break;
            }
            // every 0.1 sec
            monitoring_to_file(ssd);

            analyze(ssd, n->namespaces, n->num_namespaces);

            /* timer setting */
            ssd->next_log_time += 100*1000*1000;
        }

    }
    return NULL;
}
