#include "ftl.h"

//#define FEMU_DEBUG_FTL

static void *ftl_thread(void *arg);

/* Must be dependent on Namespace policy */ 
static void set_ns_start_index(struct NvmeNamespace *ns)
{
    int start_lpn = 0;
    int start_ch = 0;
    for( int i = 0 ; i < ns->id-1 ; i++){
        uint64_t ch = ns->ctrl->namespaces[i].sp.nchs;
        uint64_t pgs = ns->ctrl->namespaces[i].sp.pgs_per_ch;
        start_lpn += ch*pgs;
        start_ch += ch;
    }
    ns->start_lpn = start_lpn;

    for( int i = 0 ; i < ns->sp.nchs ; i++){
        ns->ch_list[i] = start_ch + i;
    }
}

static uint64_t get_ns_start_lpn(struct NvmeNamespace *ns)
{
    return ns->start_lpn;
}
 
/* Maping Table Functions */
static inline struct ppa get_maptbl_ent(struct NvmeNamespace *ns, uint64_t lpn)
{
    struct ssd *ssd = (struct ssd*)ns->ssd;
    uint64_t lpn_margin = get_ns_start_lpn(ns);
    return ssd->maptbl[lpn+lpn_margin];
}

static inline void set_maptbl_ent(struct NvmeNamespace *ns, uint64_t lpn, struct ppa *ppa)
{
    struct ssd *ssd = (struct ssd*)ns->ssd;
    uint64_t lpn_margin = get_ns_start_lpn(ns);
    ftl_assert(lpn+lpn_margin < ssd->sp.tt_pgs);
    ssd->maptbl[lpn+lpn_margin] = *ppa;
}

static uint64_t ppa2pgidx(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct ssd *ssd = (struct ssd*)ns->ssd;
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
    uint64_t pgidx = ppa2pgidx(ns, ppa);
    // uint64_t lpn_margin = get_ns_start_lpn(ns);

    return ssd->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct NvmeNamespace *ns, uint64_t lpn, struct ppa *ppa)
{
    struct ssd *ssd = (struct ssd*)ns->ssd;
    uint64_t pgidx = ppa2pgidx(ns, ppa);
    // uint64_t lpn_margin = get_ns_start_lpn(ns);

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
    spp->secsz = n->bb_params.secsz; // 512
    spp->secs_per_pg = n->bb_params.secs_per_pg; // 8
    spp->pgs_per_blk = n->bb_params.pgs_per_blk; //256
    spp->blks_per_pl = n->bb_params.blks_per_pl; /* 256 16GB */
    spp->pls_per_lun = n->bb_params.pls_per_lun; // 1
    spp->luns_per_ch = n->bb_params.luns_per_ch; // 8
    spp->nchs = n->bb_params.nchs; // 8

    spp->pg_rd_lat = n->bb_params.pg_rd_lat;
    spp->pg_wr_lat = n->bb_params.pg_wr_lat;
    spp->blk_er_lat = n->bb_params.blk_er_lat;
    spp->ch_xfer_lat = n->bb_params.ch_xfer_lat;

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

    spp->gc_thres_pcent         = 0.75;
    spp->gc_thres_blocks         = (int)((1 - spp->gc_thres_pcent) * spp->tt_blks);
    spp->gc_thres_pcent_high    = 0.95;
    spp->gc_thres_blocks_high    = (int)((1 - spp->gc_thres_pcent_high) * spp->tt_blks);
    spp->enable_gc_delay        = true;

    check_params(spp);
}

static void namespace_init_params(struct namespace_params *spp, struct ssdparams *ssdp, uint64_t phy_size)
{
    spp->secsz = ssdp->secsz;
    spp->secs_per_pg = ssdp->secs_per_pg;
    spp->pgs_per_blk = ssdp->pgs_per_blk;
    spp->blks_per_pl = ssdp->blks_per_pl;
    spp->pls_per_lun = ssdp->pls_per_lun;
    spp->luns_per_ch = ssdp->luns_per_ch;
    spp->nchs = (phy_size/ssdp->secs_per_ch)/spp->secsz;    
    // same as phy_size/(ssdp->secs_per_ch*spp->secsz)

    spp->pg_rd_lat   = ssdp->pg_rd_lat;
    spp->pg_wr_lat   = ssdp->pg_wr_lat;
    spp->blk_er_lat  = ssdp->blk_er_lat;
    spp->ch_xfer_lat = ssdp->ch_xfer_lat;

    /* calculated values */
    spp->secs_per_blk   = ssdp->secs_per_blk;
    spp->secs_per_pl    = ssdp->secs_per_pl;
    spp->secs_per_lun   = ssdp->secs_per_lun;
    spp->secs_per_ch    = ssdp->secs_per_ch;

    spp->pgs_per_pl     = ssdp->pgs_per_pl;
    spp->pgs_per_lun    = ssdp->pgs_per_lun;
    spp->pgs_per_ch     = ssdp->pgs_per_ch;

    spp->blks_per_lun   = ssdp->blks_per_lun;
    spp->blks_per_ch    = ssdp->blks_per_ch;

    spp->pls_per_ch     = ssdp->pls_per_ch;

    spp->tt_secs    = spp->secs_per_ch  * spp->nchs;
    spp->tt_pgs     = spp->pgs_per_ch   * spp->nchs;
    spp->tt_blks    = spp->blks_per_ch  * spp->nchs;
    spp->tt_pls     = spp->pls_per_ch   * spp->nchs;
    spp->tt_luns    = spp->luns_per_ch  * spp->nchs;

    spp->gc_thres_pcent         = 0.75;
    spp->gc_thres_blocks         = (int)((1 - spp->gc_thres_pcent) * spp->tt_blks);
    spp->gc_thres_pcent_high    = 0.95;
    spp->gc_thres_blocks_high    = (int)((1 - spp->gc_thres_pcent_high) * spp->tt_blks);
    spp->enable_gc_delay        = true;
}

static struct nand_block *get_next_free_block(struct nand_lun *lun)
{
    /* If multi plane is supported, this algorithm must be modified */
    struct nand_plane *pl = &lun->pl[0];
    struct nand_block *cur_block = NULL;

    cur_block = QTAILQ_FIRST(&pl->free_block_list);
    if (!cur_block) {
        return NULL;
    }

    QTAILQ_REMOVE(&pl->free_block_list, cur_block, entry);
    pl->free_block_cnt--;
    return cur_block;
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

    // phy_size = ns->size(MB) * (1 + OP)
    uint64_t phy_size;
    phy_size = ns->size/(1024*1024) * ((uint64_t)spp->tt_secs * spp->secsz) / n->memsz;
    ns->ssd = ssd;
    ns->bm = g_malloc0(sizeof(struct block_mgmt));

    namespace_init_params(&ns->sp, spp, phy_size);
    ns->ch_list = g_malloc0(sizeof(int) * spp->nchs);

    struct block_mgmt *bm = ns->bm;
    bm->victim_block_pq = pqueue_init(ns->sp.tt_blks, victim_block_cmp_pri,
            victim_block_get_pri, victim_block_set_pri,
            victim_block_get_pos, victim_block_set_pos);
    bm->ch = bm->lun = 0;

    set_ns_start_index(ns);


    printf("physical:%ldByte, nch:%d( ", phy_size, ns->sp.nchs);

    for( int i = 0 ; i < ns->sp.nchs ; i++){
        printf("%2dch ", ns->ch_list[i]);
    }
    printf(")\r\n");
}

void ssd_init(FemuCtrl *n)
{
    struct ssd *ssd = n->ssd;
    struct ssdparams *spp = &ssd->sp;

    ftl_assert(ssd);

    ssd_init_params(spp, n);
    for( int  i = 0; i < n->num_namespaces ; i ++){
        ns_init(n, &n->namespaces[i]);
    }

    /* initialize ssd internal layout architecture */
    ssd->ch = g_malloc0(sizeof(struct ssd_channel) * spp->nchs);
    for (int i = 0; i < spp->nchs; i++) {
        ssd->ch[i].ppa.ppa = 0;
        ssd->ch[i].ppa.g.ch = i;
        ssd_init_ch(&ssd->ch[i], spp);
    }

    /* initialize maptbl */
    ssd_init_maptbl(ssd);

    /* initialize rmap */
    ssd_init_rmap(ssd);

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
    return (lpn < ns->sp.tt_pgs);
}

static inline bool mapped_ppa(struct ppa *ppa)
{
    return !(ppa->ppa == UNMAPPED_PPA);
}

static inline struct ssd_channel *get_ch(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct ssd *ssd = (struct ssd*)ns->ssd;
    return &(ssd->ch[ppa->g.ch]);
}

static inline struct nand_lun *get_lun(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct ssd_channel *ch = get_ch(ns, ppa);
    return &(ch->lun[ppa->g.lun]);
}

static inline struct nand_plane *get_pl(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct nand_lun *lun = get_lun(ns, ppa);
    return &(lun->pl[ppa->g.pl]);
}

static inline struct nand_block *get_blk(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct nand_plane *pl = get_pl(ns, ppa);
    return &(pl->blk[ppa->g.blk]);
}

static inline struct nand_page *get_pg(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct nand_block *blk = get_blk(ns, ppa);
    return &(blk->pg[ppa->g.pg]);
}

static uint64_t ssd_advance_status(struct NvmeNamespace *ns, struct ppa *ppa, struct
        nand_cmd *ncmd)
{
    int c = ncmd->cmd;
    uint64_t cmd_stime = (ncmd->stime == 0) ? \
        qemu_clock_get_ns(QEMU_CLOCK_REALTIME) : ncmd->stime;
    uint64_t nand_stime;
    struct namespace_params *spp = &ns->sp;
    struct nand_lun *lun = get_lun(ns, ppa);
    uint64_t lat = 0;

    // printf("ssd_advance_status ppa ns:%d, ch:%d, lun:%d, blk:%d, page:%d \r\n", ns->id, ppa->g.ch, ppa->g.lun, ppa->g.blk, ppa->g.pg);

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
    struct block_mgmt *bm = ns->bm;
    struct nand_lun *cur_lun = NULL;
    struct ppa ppa;
    ppa.ppa = 0;
 
    ppa.g.ch    = ns->ch_list[bm->ch];
    ppa.g.lun   = bm->lun;
    ppa.g.pl    = 0;
    cur_lun = get_lun(ns, &ppa);
    ppa.g.blk   = cur_lun->wp;
    ppa.g.pg    = cur_lun->pl[ppa.g.pl].blk[ppa.g.blk].wp;
    // printf("new  ppa ns:%d, ch:%d, lun:%d, blk:%d, page:%d \r\n", ns->id, ppa.g.ch, ppa.g.lun, ppa.g.blk, ppa.g.pg);

    return ppa;
}
static void ssd_advance_write_pointer(struct NvmeNamespace *ns)
{
    struct namespace_params *spp = &ns->sp;
    struct block_mgmt *bm = ns->bm;
    struct nand_lun *cur_lun = NULL;
    struct nand_block *curr_block = NULL;
    struct nand_block *next_block = NULL;
    struct ppa ppa;
    int repeat = 0;
    ppa.ppa = 0;

    ppa.g.ch    = ns->ch_list[bm->ch];
    ppa.g.lun   = bm->lun;
    cur_lun = get_lun(ns, &ppa);
    curr_block = &cur_lun->pl[0].blk[cur_lun->wp];
    curr_block->wp++;

    /* page over in block */
    if( curr_block->wp == spp->pgs_per_blk ){
        // printf("insert pq pqsize:%ld, free_blk_cnt:%d, ch:%d, lun:%d, blk:%d, pg_wp:%d\r\n",
        //     bm->victim_block_pq->size, cur_lun->pl[0].free_block_cnt, curr_block->ppa.g.ch, curr_block->ppa.g.lun, curr_block->ppa.g.blk, curr_block->wp);
        pqueue_insert(bm->victim_block_pq, curr_block);
        bm->victim_block_cnt++;

        next_block = get_next_free_block(cur_lun);
        if( next_block == NULL ){
            cur_lun->wp = -1;
        }else{
            cur_lun->wp = next_block->ppa.g.blk;
            curr_block = &cur_lun->pl[0].blk[cur_lun->wp];
            // curr_block->wp = 0;
        }
    }

retry:
    ftl_assert(repeat < spp->tt_luns);
    repeat++;
    bm->ch++;
    if( bm->ch == spp->nchs){
        bm->ch = 0;
        bm->lun++;
        if (bm->lun == spp->luns_per_ch) {
            bm->lun = 0;
        }
    }

    ppa.g.ch    = ns->ch_list[bm->ch];
    ppa.g.lun   = bm->lun;
    cur_lun = get_lun(ns, &ppa);

    if(cur_lun->wp == -1){
        printf("no free block ns:%d, ch:%d, lun:%d (repeat :%d)\r\n", ns->id, ppa.g.ch, ppa.g.lun, repeat);
        goto retry;
    }

    curr_block = &cur_lun->pl[0].blk[cur_lun->wp];
    ppa.g.blk = cur_lun->wp;
    ppa.g.pg = curr_block->wp;
    
    if ( !valid_ppa(ns, &ppa)) {
        printf("Invalid ppa,ch:%d, lun:%d, blk:%d, pg:%d\r\n",
        ppa.g.ch, ppa.g.lun, ppa.g.blk, ppa.g.pg);
    }
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
static void mark_page_invalid(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct block_mgmt *bm = ns->bm;
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;

    /* update corresponding page status */
    pg = get_pg(ns, ppa);
    ftl_assert(pg->status == PG_VALID);
    pg->status = PG_INVALID;

    /* update corresponding block status */
    blk = get_blk(ns, ppa);
    blk->ipc++;
    if (blk->pos) {
        pqueue_change_priority(bm->victim_block_pq, blk->vpc-1, blk);
    }else{
        blk->vpc--;
    }
}

static void mark_page_valid(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;

    /* update page status */
    pg = get_pg(ns, ppa);
    ftl_assert(pg->status == PG_FREE);
    pg->status = PG_VALID;

    /* update corresponding block status */
    blk = get_blk(ns, ppa);
    ftl_assert(blk->vpc >= 0 && blk->vpc < ns->sp.pgs_per_blk);
    blk->vpc++;
}

static void mark_block_free(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct namespace_params *spp = &ns->sp;
    struct nand_plane *pl = get_pl(ns, ppa);
    struct nand_block *blk = get_blk(ns, ppa);
    struct nand_page *pg = NULL;

    for (int i = 0; i < spp->pgs_per_blk; i++) {
        /* reset page status */
        pg = &blk->pg[i];
        ftl_assert(pg->nsecs == spp->secs_per_pg);
        pg->status = PG_FREE;
    }

    /* reset block status */
    ftl_assert(blk->npgs == spp->pgs_per_blk);
    blk->ipc = 0;
    blk->vpc = 0;
    blk->wp = 0;
    blk->erase_cnt++;

    QTAILQ_INSERT_TAIL(&pl->free_block_list, blk, entry);
    pl->free_block_cnt++;
}

static void gc_read_page(NvmeNamespace *ns, struct ppa *ppa)
{
    /* advance ssd status, we don't care about how long it takes */
    if (ns->sp.enable_gc_delay) {
        struct nand_cmd gcr;
        gcr.type = GC_IO;
        gcr.cmd = NAND_READ;
        gcr.stime = 0;
        ssd_advance_status(ns, ppa, &gcr);
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

    if (ns->sp.enable_gc_delay) {
        struct nand_cmd gcw;
        gcw.type = GC_IO;
        gcw.cmd = NAND_WRITE;
        gcw.stime = 0;
        ssd_advance_status(ns, &new_ppa, &gcw);
    }

    new_lun = get_lun(ns, &new_ppa);
    new_lun->gc_endtime = new_lun->next_lun_avail_time;

    return 0;
}

static struct nand_block *select_victim_block(struct NvmeNamespace *ns, bool force)
{
    struct block_mgmt *bm = ns->bm;
    struct nand_block *victim_block = NULL;

    victim_block = pqueue_peek(bm->victim_block_pq);
    if (!victim_block) {
        return NULL;
    }
    
    // printf("select nsid:%d pqsize: %ld, vpc:%d, ch:%d \r\n", ns->id, bm->victim_block_pq->size, victim_block->vpc, victim_block->ppa.g.ch);
    if (!force && victim_block->ipc < ns->sp.pgs_per_blk / 8) {
        return NULL;
    }

    pqueue_pop(bm->victim_block_pq);
    victim_block->pos = 0;
    bm->victim_block_cnt--;

    // printf("select ch:%d, lun:%d, blk:%d, page:%d\r\n", 
    //     victim_block->ppa.g.ch, victim_block->ppa.g.lun, victim_block->ppa.g.blk, victim_block->ppa.g.pg);

    return victim_block;
}

/* here ppa identifies the block we want to clean */
static void clean_one_block(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct namespace_params *spp = &ns->sp;
    struct nand_page *pg_iter = NULL;
    int cnt = 0;

    for (int pg = 0; pg < spp->pgs_per_blk; pg++) {
        ppa->g.pg = pg;
        pg_iter = get_pg(ns, ppa);
        /* there shouldn't be any free page in victim blocks */
        // ftl_assert(pg_iter->status != PG_FREE);  // not suitable for wear-leveling 
        if (pg_iter->status == PG_VALID) {
            gc_read_page(ns, ppa);
            /* delay the maptbl update until "write" happens */
            gc_write_page(ns, ppa);
            cnt++;
        }
    }

    ftl_assert(get_blk(ns, ppa)->vpc == cnt);
}

static void free_block(struct NvmeNamespace *ns, struct ppa *ppa)
{    
    struct namespace_params *spp = &ns->sp;
    struct nand_lun *lunp;

    // printf("GC block ch:%d,lun:%d,blk:%d,\r\n", ppa->g.ch, ppa->g.lun, ppa->g.blk);

    lunp = get_lun(ns, ppa);
    clean_one_block(ns, ppa);
    mark_block_free(ns, ppa);

    if (spp->enable_gc_delay) {
        struct nand_cmd gce;
        gce.type = GC_IO;
        gce.cmd = NAND_ERASE;
        gce.stime = 0;
        ssd_advance_status(ns, ppa, &gce);
    }

    lunp->gc_endtime = lunp->next_lun_avail_time;
}

static int do_gc(struct NvmeNamespace *ns, bool force)
{
    struct nand_block *victim_block = NULL;
    victim_block = select_victim_block(ns, force);
    if (!victim_block) {
        return -1;
    }
    printf("GC nsid:%d, ch:%d, lun:%d, blk:%d\r\n", ns->id, victim_block->ppa.g.ch, victim_block->ppa.g.lun, victim_block->ppa.g.blk);
    free_block(ns, &victim_block->ppa);
    return 0;
}

static inline bool should_gc(struct NvmeNamespace *ns)
{
    // struct block_mgmt *bm = ns->bm;
    uint64_t free_block_cnt = 0;
    struct ppa ppa;
    ppa.ppa = 0;
    for (int i = 0; i < ns->sp.nchs; i++){
        for (int j = 0; j < ns->sp.luns_per_ch; j++){
            for (int k = 0; k < ns->sp.pls_per_lun; k++){
                ppa.g.ch = ns->ch_list[i];
                ppa.g.lun = j;
                ppa.g.pl = k;
                free_block_cnt += get_pl(ns, &ppa)->free_block_cnt;
            }
        }
    }
    return (free_block_cnt <= ns->sp.gc_thres_blocks);
}

static inline bool should_gc_high(struct NvmeNamespace *ns)
{
    // struct block_mgmt *bm = ns->bm;
    uint64_t free_block_cnt = 0;
    struct ppa ppa;
    ppa.ppa = 0;
    for (int i = 0; i < ns->sp.nchs; i++){
        for (int j = 0; j < ns->sp.luns_per_ch; j++){
            for (int k = 0; k < ns->sp.pls_per_lun; k++){
                ppa.g.ch = ns->ch_list[i];
                ppa.g.lun = j;
                ppa.g.pl = k;
                free_block_cnt += get_pl(ns, &ppa)->free_block_cnt;
            }
        }
    }
    return (free_block_cnt <= ns->sp.gc_thres_blocks_high);
}

static void wl_read_page(NvmeNamespace *ns, struct ppa *ppa)
{
    // uint64_t lat = 0;
    /* advance ssd status, we don't care about how long it takes */
    struct nand_cmd wlr;
    wlr.type = WL_IO;
    wlr.cmd = NAND_READ;
    wlr.stime = 0;
    ssd_advance_status(ns, ppa, &wlr);
    
    // return lat;
}

/* move valid page data (already in DRAM) from victim block to a new page */
static uint64_t wl_write_page(struct NvmeNamespace *ns, uint64_t lpn, struct ppa *ppa)
{
    uint64_t lat = 0;

    ftl_assert(valid_lpn(ns, lpn));
    /* update maptbl */
    set_maptbl_ent(ns, lpn, ppa);
    /* update rmap */
    set_rmap_ent(ns, lpn, ppa);

    mark_page_valid(ns, ppa);

    /* need to advance the write pointer here */
    // ssd_advance_write_pointer(ns);

    struct nand_cmd wlw;
    wlw.type = WL_IO;
    wlw.cmd = NAND_WRITE;
    wlw.stime = 0;
    lat = ssd_advance_status(ns, ppa, &wlw);
    return lat;
}

static void swap_one_block(struct NvmeNamespace *ns1, struct ppa *ppa1, struct NvmeNamespace *ns2, struct ppa *ppa2)
{
    struct namespace_params *spp1 = &ns1->sp;
    struct namespace_params *spp2 = &ns2->sp;
    // struct nand_page *pg_iter = NULL;

    // allocate NPN buffer
    uint64_t *lpn_buffer1 = g_malloc0(sizeof(uint64_t) * spp1->pgs_per_blk);
    uint64_t *lpn_buffer2 = g_malloc0(sizeof(uint64_t) * spp2->pgs_per_blk);
    int lpn_idx1 = 0;
    int lpn_idx2 = 0;
    
    /* read one block from each chip */
    for (int pg = 0; pg < spp1->pgs_per_blk; pg++) {
        ppa1->g.pg = pg;
        if (get_pg(ns1, ppa1)->status == PG_VALID) {
            lpn_buffer1[lpn_idx1++] = get_rmap_ent(ns1, ppa1);
            wl_read_page(ns1, ppa1);
        }
    }
    for (int pg = 0; pg < spp2->pgs_per_blk; pg++) {
        ppa2->g.pg = pg;
        if (get_pg(ns2, ppa2)->status == PG_VALID) {
            lpn_buffer2[lpn_idx2++] = get_rmap_ent(ns2, ppa2);
            wl_read_page(ns2, ppa2);
        }
    }

    /* erase each block */
    mark_block_free(ns1, ppa1);
    mark_block_free(ns2, ppa2);

    struct nand_cmd wle;
    wle.type = WL_IO;
    wle.cmd = NAND_ERASE;
    wle.stime = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    ssd_advance_status(ns1, ppa1, &wle);
    ssd_advance_status(ns2, ppa2, &wle);

    /* move data( actually... no inter-block movement ) */
    for (int lpn_i = 0; lpn_i < lpn_idx1; lpn_i++) {
        ppa2->g.pg = lpn_i;
        wl_write_page(ns1, lpn_buffer1[lpn_i], ppa2);
        get_blk(ns1, ppa2)->wp++;
    }
    for (int lpn_i = 0; lpn_i < lpn_idx2; lpn_i++) {
        ppa1->g.pg = lpn_i;
        wl_write_page(ns2, lpn_buffer2[lpn_i], ppa1);
        get_blk(ns2, ppa1)->wp++;
    }

    /* invalidate the rest pages.. */
//     for (int lpn_i = lpn_index1; lpn_i < spp1->pgs_per_blk; lpn_i++) {
//         ppa2.g.pg = lpn_i;
//         mark_page_invalid(ns2, &ppa2);
//     }
//     for (int lpn_i = lpn_index2; lpn_i < spp2->pgs_per_blk; lpn_i++) {
//         ppa1.g.pg = lpn_i;
//         mark_page_invalid(ns1, &ppa1);
//     }
}

static void swap_one_chip(struct NvmeNamespace *ns1, struct ppa *ppa1, struct NvmeNamespace *ns2, struct ppa *ppa2)
{
    // struct namespace_params *spp1 = &ns1->sp;
    // struct namespace_params *spp2 = &ns2->sp;
    struct block_mgmt *bm1 = ns1->bm;
    struct block_mgmt *bm2 = ns2->bm;
    struct nand_block *blk1;
    struct nand_block *blk2;
    
    /* Operated in single-plane mode only */
    for (int blk = 0; blk < ns1->sp.blks_per_pl; blk++) {
        ppa1->g.blk = blk;
        ppa2->g.blk = blk;
        
        /* delete all victim block candidate*/
        blk1 = get_blk(ns1, ppa1);
        blk2 = get_blk(ns2, ppa2);
        if( blk1->pos ){
            pqueue_remove(bm1->victim_block_pq, blk1);
            blk1->pos = 0;
            bm1->victim_block_cnt--;
        }
        if( blk2->pos ){
            pqueue_remove(bm2->victim_block_pq, blk2);
            blk2->pos = 0;
            bm2->victim_block_cnt--;
        }

        /* swap block */
        swap_one_block(ns1, ppa1, ns2, ppa2);

        /* queueing by block state */
        if( blk1->vpc == ns1->sp.blks_per_pl ){
            QTAILQ_REMOVE(&get_pl(ns1, ppa1)->free_block_list, blk1, entry);
            get_pl(ns1, ppa1)->free_block_cnt--;
            pqueue_insert(bm1->victim_block_pq, blk1);
            bm1->victim_block_cnt++;
        }

        if( blk2->vpc == ns1->sp.blks_per_pl ){
            QTAILQ_REMOVE(&get_pl(ns2, ppa2)->free_block_list, blk2, entry);
            get_pl(ns2, ppa2)->free_block_cnt--;
            pqueue_insert(bm2->victim_block_pq, blk2);
            bm2->victim_block_cnt++;
        }
    }

    /* new write point */
    get_lun(ns1, ppa1)->wp = get_next_free_block(get_lun(ns1, ppa1))->ppa.g.blk;
    get_lun(ns2, ppa2)->wp = get_next_free_block(get_lun(ns2, ppa2))->ppa.g.blk;
}

int ch_swap(struct NvmeNamespace *ns1, int ch1, struct NvmeNamespace *ns2, int ch2)
{
    struct ppa ppa1, ppa2;
    ppa1.ppa = 0;
    ppa2.ppa = 0;
    int lun;
    struct nand_block *delete_blk;

    /* Swaps that do not take priority */
    for (lun = 0; lun < ns1->sp.luns_per_ch; lun++) {
        ppa1.g.ch = ns1->ch_list[ch1];
        ppa1.g.lun = lun;
        ppa2.g.ch = ns2->ch_list[ch2];
        ppa2.g.lun = lun;

        /* delete all block in free block */
        while((delete_blk = QTAILQ_FIRST(&get_pl(ns1, &ppa1)->free_block_list)) != NULL) {
            QTAILQ_REMOVE(&get_pl(ns1, &ppa1)->free_block_list, delete_blk, entry);
            get_pl(ns1, &ppa1)->free_block_cnt--;
        }
        while((delete_blk = QTAILQ_FIRST(&get_pl(ns2, &ppa2)->free_block_list)) != NULL) {
            QTAILQ_REMOVE(&get_pl(ns2, &ppa2)->free_block_list, delete_blk, entry);
            get_pl(ns2, &ppa2)->free_block_cnt--;
        }

        /* swap two chip */
        swap_one_chip(ns1, &ppa1, ns2, &ppa2);
    }

    // meta swap
    int temp_ch = ns1->ch_list[ch1];
    ns1->ch_list[ch1] = ns2->ch_list[ch2];
    ns2->ch_list[ch2] = temp_ch;

    return 0;
}

static uint64_t ssd_read(struct ssd *ssd, NvmeRequest *req)
{
    struct NvmeNamespace * ns = req->ns;        // <- get Namespace!!
    struct namespace_params *spp = &ns->sp;
    uint64_t lba = req->slba;
    int nsecs = req->nlb;
    struct ppa ppa;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + nsecs - 1) / spp->secs_per_pg;
    uint64_t lpn;
    uint64_t sublat, maxlat = 0;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\r\n", start_lpn, ns->sp.tt_pgs);
    }

    /* normal IO read path */
    for (lpn = start_lpn; lpn <= end_lpn; lpn++) {
        ppa = get_maptbl_ent(ns, lpn);
        if (!mapped_ppa(&ppa) || !valid_ppa(ns, &ppa)) {
            //printf("%s,lpn(%" PRId64 ") not mapped to valid ppa\n", ssd->ssdname, lpn);
            //printf("Invalid ppa,ch:%d,lun:%d,blk:%d,pl:%d,pg:%d,sec:%d\n",
            //ppa.g.ch, ppa.g.lun, ppa.g.blk, ppa.g.pl, ppa.g.pg, ppa.g.sec);
            continue;
        }        
        struct nand_cmd srd;
        srd.type = USER_IO;
        srd.cmd = NAND_READ;
        srd.stime = req->stime;
        sublat = ssd_advance_status(ns, &ppa, &srd);
        maxlat = (sublat > maxlat) ? sublat : maxlat;
    }

    return maxlat;
}

static uint64_t ssd_write(struct ssd *ssd, NvmeRequest *req)
{
    uint64_t lba = req->slba;
    struct NvmeNamespace * ns = req->ns;        // <- get Namespace!!
    struct namespace_params *spp = &ns->sp;
    int len = req->nlb;
    uint64_t start_lpn = lba / spp->secs_per_pg;
    uint64_t end_lpn = (lba + len - 1) / spp->secs_per_pg;
    struct ppa ppa;
    uint64_t lpn;
    uint64_t curlat = 0, maxlat = 0;
    int r;

    if (end_lpn >= spp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\r\n", start_lpn, ns->sp.tt_pgs);
    }

    while (should_gc_high(ns)) {
        /* perform GC here until !should_gc(ssd) */
        r = do_gc(ns, true);
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
        curlat = ssd_advance_status(ns, &ppa, &swr);
        maxlat = (curlat > maxlat) ? curlat : maxlat;
    }

    return maxlat;
}

static void *ftl_thread(void *arg)
{
    FemuCtrl *n = (FemuCtrl *)arg;
    struct ssd *ssd = n->ssd;
    NvmeRequest *req = NULL;
    uint64_t lat = 0;
    int rc;
    int i;

    while (!*(ssd->dataplane_started_ptr)) {
        usleep(100000);
    }

    /* FIXME: not safe, to handle ->to_ftl and ->to_poller gracefully */
    ssd->to_ftl = n->to_ftl;
    ssd->to_poller = n->to_poller;

    while (1) {
        for (i = 1; i <= n->nr_pollers; i++) {
            if (!ssd->to_ftl[i] || !femu_ring_count(ssd->to_ftl[i]))
                continue;

            rc = femu_ring_dequeue(ssd->to_ftl[i], (void *)&req, 1);
            if (rc != 1) {
                printf("FEMU: FTL to_ftl dequeue failed\n");
            }

            ftl_assert(req);

            // struct NvmeNamespace *ns = req->ns;
            switch (req->cmd.opcode) {
            case NVME_CMD_WRITE:
                lat = ssd_write(ssd, req);
                break;
            case NVME_CMD_READ:
                lat = ssd_read(ssd, req);
                break;
            case NVME_CMD_DSM:
                lat = 0;
                break;
            default:
                //ftl_err("FTL received unkown request type, ERROR\n");
                ;
            }

            req->reqlat = lat;
            req->expire_time += lat;

            rc = femu_ring_enqueue(ssd->to_poller[i], (void *)&req, 1);
            if (rc != 1) {
                ftl_err("FTL to_poller enqueue failed\n");
            }

            /* clean one block if needed */
            for (int i = 0; i < n->num_namespaces; i++)
            {
                if (should_gc(&n->namespaces[i])) {
                    do_gc(&n->namespaces[i], false);
                }
            }

            // if (should_gc(req->ns)) {
            //     do_gc(req->ns, false);
            // }
        }
    }

    return NULL;
}
