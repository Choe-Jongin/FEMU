#include "ftl.h"

//#define FEMU_DEBUG_FTL

static void *ftl_thread(void *arg);
static void free_block(struct NvmeNamespace *ns, struct ppa *ppa);

uint64_t ftl_start = 0;
int swaping = 0;

/* Must be dependent on Namespace policy */ 
static void set_ns_start_index(struct NvmeNamespace *ns)
{
    int start_ch = 0;
    int start_lpn = 0;
    for( int i = 0 ; i < ns->id-1 ; i++){
        uint64_t ch = ns->ctrl->namespaces[i].np.nchs;
        uint64_t pgs = ns->ctrl->namespaces[i].np.tt_pgs;
        start_ch += ch;
        start_lpn += pgs;
    }
    ns->start_lpn = start_lpn;

    for( int i = 0 ; i < ns->np.nchs ; i++){
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
    // uint64_t lpn_margin = get_ns_start_lpn(ns);

    return ssd->rmap[pgidx];
}

/* set rmap[page_no(ppa)] -> lpn */
static inline void set_rmap_ent(struct NvmeNamespace *ns, uint64_t lpn, struct ppa *ppa)
{
    struct ssd *ssd = (struct ssd*)ns->ssd;
    uint64_t pgidx = ppa2pgidx(ssd, ppa);
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

    //ftl_assert(is_power_of_2(npp->luns_per_ch));
    //ftl_assert(is_power_of_2(npp->nchs));
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

    spp->gc_thres_blocks         = (int)((1 - spp->gc_thres_pcent) * spp->tt_blks);
    spp->gc_thres_blocks_high    = (int)((1 - spp->gc_thres_pcent_high) * spp->tt_blks);
    spp->enable_gc_delay        = true;

    check_params(spp);
}
 
static void namespace_init_params(struct namespace_params *npp, struct ssdparams *ssdp, uint64_t phy_size)
{
    npp->secsz = ssdp->secsz;
    npp->secs_per_pg = ssdp->secs_per_pg;
    npp->pgs_per_blk = ssdp->pgs_per_blk;
    npp->blks_per_pl = ssdp->blks_per_pl;
    npp->pls_per_lun = ssdp->pls_per_lun;
    npp->luns_per_ch = ssdp->luns_per_ch;
    npp->nchs = (phy_size/ssdp->secs_per_ch)/npp->secsz;    
    // same as phy_size/(ssdp->secs_per_ch*npp->secsz)

    /* calculated values */
    npp->secs_per_blk   = ssdp->secs_per_blk;
    npp->secs_per_pl    = ssdp->secs_per_pl;
    npp->secs_per_lun   = ssdp->secs_per_lun;
    npp->secs_per_ch    = ssdp->secs_per_ch;

    npp->pgs_per_pl     = ssdp->pgs_per_pl;
    npp->pgs_per_lun    = ssdp->pgs_per_lun;
    npp->pgs_per_ch     = ssdp->pgs_per_ch;

    npp->blks_per_lun   = ssdp->blks_per_lun;
    npp->blks_per_ch    = ssdp->blks_per_ch;

    npp->pls_per_ch     = ssdp->pls_per_ch;

    npp->tt_secs    = npp->secs_per_ch  * npp->nchs;
    npp->tt_pgs     = npp->pgs_per_ch   * npp->nchs;
    npp->tt_blks    = npp->blks_per_ch  * npp->nchs;
    npp->tt_pls     = npp->pls_per_ch   * npp->nchs;
    npp->tt_luns    = npp->luns_per_ch  * npp->nchs;

    npp->gc_thres_pcent         = ssdp->gc_thres_pcent;
    npp->gc_thres_pcent_high    = ssdp->gc_thres_pcent_high;
    npp->gc_thres_blocks        = (int)((1 - npp->gc_thres_pcent) * npp->tt_blks);
    npp->gc_thres_blocks_high   = (int)((1 - npp->gc_thres_pcent_high) * npp->tt_blks);
    npp->enable_gc_delay        = true;
}

static struct nand_block *get_next_free_block(struct nand_lun *lun)
{
    /* If multi plane is supported, this algorithm must be modified */
    struct nand_plane *pl = &lun->pl[0];
    struct nand_block *curr_block = NULL;

    curr_block = QTAILQ_FIRST(&pl->free_block_list);
    if (!curr_block) {
        printf("ch %d, chip %d : No free block here!! \r\n", lun->ppa.g.ch, lun->ppa.g.lun);
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
    lun->swapping_now = false;

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

    namespace_init_params(&ns->np, spp, phy_size);
    ns->ch_list = g_malloc0(sizeof(int) * spp->nchs);

    struct block_mgmt *bm = ns->bm;
    bm->victim_block_pq = pqueue_init(ns->np.tt_blks, victim_block_cmp_pri,
            victim_block_get_pri, victim_block_set_pri,
            victim_block_get_pos, victim_block_set_pos);
    bm->wp_ch = bm->wp_lun = 0;

    set_ns_start_index(ns);

    printf("physical:%ldByte, nch:%d( ", phy_size, ns->np.nchs);

    for( int i = 0 ; i < ns->np.nchs ; i++){
        printf("%2dch ", ns->ch_list[i]);
    }
    printf(")\r\n");
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
    struct block_mgmt *bm = ns->bm;
    struct nand_lun *cur_lun = NULL;
    struct ppa ppa;
    ppa.ppa = 0;
 
    ppa.g.ch    = ns->ch_list[bm->wp_ch];
    ppa.g.lun   = bm->wp_lun;
    ppa.g.pl    = 0;

    cur_lun     = get_lun(ns->ssd, &ppa);
    ppa.g.blk   = cur_lun->wp;
    ppa.g.pg    = cur_lun->pl[ppa.g.pl].blk[ppa.g.blk].wp;
    // printf("new ppa ns:%d, ch:%d, lun:%d, blk:%d, page:%d \r\n", ns->id, ppa.g.ch, ppa.g.lun, ppa.g.blk, ppa.g.pg);
    return ppa;
}

static nand_block *get_free_block_by_inchip_gc(struct NvmeNamespace *ns, struct nand_lun *lun)
{
    struct block_mgmt *bm = ns->bm;
    struct nand_block *victim_block = NULL;
    int max_ipc;

    /* select victim block in chip (no in namepsace)*/
    max_ipc = -1;
    for( int i = 0 ; i < ns->np.blks_per_pl ; i++ ){
        if( lun->pl[0].blk[i].state == BLOCK_FULL && max_ipc < lun->pl[0].blk[i].ipc){
            max_ipc       = lun->pl[0].blk[i].ipc;
            victim_block = &lun->pl[0].blk[i];
        }
    }

    if (!victim_block) {
        ftl_err("failed victim block select in intra chip gc \r\n");
        return NULL;
    }

    pqueue_remove(bm->victim_block_pq, victim_block);
    victim_block->pos = 0;
    bm->victim_block_cnt--;

    /* make free block */
    free_block(ns, &victim_block->ppa);
    victim_block->state = BLOCK_OPEN;
    return victim_block;
}

static void ns_advance_ch(struct NvmeNamespace *ns)
{
    struct block_mgmt *bm = ns->bm;
    struct namespace_params *npp = &ns->np;
    struct nand_lun *next_lun;
    struct ppa ppa;
    ppa.ppa = 0;

    /* increase wp by channel-first, lun-second */
    while(1){
        bm->wp_ch++;
        if( bm->wp_ch == npp->nchs){
            bm->wp_ch = 0;
            bm->wp_lun++;
            if (bm->wp_lun == npp->luns_per_ch) {
                bm->wp_lun = 0;
            }
        }

        // condition check
        ppa.g.ch    = ns->ch_list[bm->wp_ch];
        ppa.g.lun   = bm->wp_lun;
        next_lun = get_lun(ns->ssd, &ppa);
        
        if( next_lun->wp == -1 ){
            continue;
        }
        if( next_lun->swapping_now == true ){
            continue;
        }

        break;
    }
}

static void lun_advance_write_pointer(struct NvmeNamespace *ns, struct nand_lun *curr_lun)
{
    struct block_mgmt *bm = ns->bm;
    struct nand_block *curr_block = NULL;
    struct nand_block *next_block = NULL;

    curr_block = &curr_lun->pl[0].blk[curr_lun->wp];
    curr_block->wp++;

    /* page over in block */
    if( curr_block->wp == ns->np.pgs_per_blk ){
        curr_block->state = BLOCK_FULL;
        pqueue_insert(bm->victim_block_pq, curr_block);
        bm->victim_block_cnt++;

        next_block = get_next_free_block(curr_lun);
        if( next_block == NULL ){           // no free block in chip         
            curr_lun->wp = -1;              // no write pointer
            ns_advance_ch(ns);              // inchip gc 도중 원래 칩을 참조하는 것을 방지
            next_block = get_free_block_by_inchip_gc(ns, curr_lun);  // so make free block
        }
        curr_lun->wp = next_block->ppa.g.blk;
    }
}

static void ssd_advance_write_pointer(struct NvmeNamespace *ns)
{
    struct block_mgmt *bm = ns->bm;
    struct nand_lun *curr_lun = NULL;
    struct ppa ppa;
    ppa.ppa = 0;

    ppa.g.ch    = ns->ch_list[bm->wp_ch];
    ppa.g.lun   = bm->wp_lun;
    curr_lun     = get_lun(ns->ssd, &ppa);

    lun_advance_write_pointer(ns, curr_lun);

    ns_advance_ch(ns);
}

/* update SSD status about one page from PG_VALID -> PG_VALID */
static void mark_page_invalid(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct block_mgmt *bm = ns->bm;
    struct nand_block *blk = NULL;
    struct nand_page *pg = NULL;

    /* update corresponding page status */
    pg = get_pg(ns->ssd, ppa);
    ftl_assert(pg->status == PG_VALID);
    pg->status = PG_INVALID;

    /* update corresponding block status */
    blk = get_blk(ns->ssd, ppa);
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
    pg = get_pg(ns->ssd, ppa);
    ftl_assert(pg->status == PG_FREE);
    pg->status = PG_VALID;

    /* update corresponding block status */
    blk = get_blk(ns->ssd, ppa);
    ftl_assert(blk->vpc >= 0 && blk->vpc < ns->np.pgs_per_blk);
    blk->vpc++;
}

static void mark_block_free(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct namespace_params *npp = &ns->np;
    struct nand_lun *lun = get_lun(ns->ssd, ppa);
    struct nand_plane *pl = get_pl(ns->ssd, ppa);
    struct nand_block *blk = get_blk(ns->ssd, ppa);
    struct nand_page *pg = NULL;

    for (int i = 0; i < npp->pgs_per_blk; i++) {
        /* reset page status */
        pg = &blk->pg[i];
        ftl_assert(pg->nsecs == npp->secs_per_pg);
        pg->status = PG_FREE;
    }

    QTAILQ_INSERT_TAIL(&pl->free_block_list, blk, entry);
    pl->free_block_cnt++;

    /* reset block status */
    ftl_assert(blk->npgs == npp->pgs_per_blk);
    blk->ipc = 0;
    blk->vpc = 0;
    blk->wp = 0;
    blk->erase_cnt++;
    blk->state = BLOCK_FREE;

    /* 만약 wp인데 free 할 경우 */
    if(lun->wp == ppa->g.blk){
        printf("wp block free ?? \n");
        lun->wp = get_next_free_block(lun)->ppa.g.blk;
    }

}

static void gc_read_page(NvmeNamespace *ns, struct ppa *ppa)
{
    /* advance ssd status, we don't care about how long it takes */
    if (ns->np.enable_gc_delay) {
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

    gc_write(ns->statistic);

    if (ns->np.enable_gc_delay) {
        struct nand_cmd gcw;
        gcw.type = GC_IO;
        gcw.cmd = NAND_WRITE;
        gcw.stime = 0;
        ssd_advance_status(ns->ssd, &new_ppa, &gcw);
    }

    new_lun = get_lun(ns->ssd, &new_ppa);
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
    
    if (!force && victim_block->ipc < ns->np.pgs_per_blk / 16) {        
        return NULL;
    }

    victim_block = pqueue_pop(bm->victim_block_pq);
    victim_block->pos = 0;
    bm->victim_block_cnt--;

    return victim_block;
}

/* here ppa identifies the block we want to clean */
static void clean_one_block(struct NvmeNamespace *ns, struct ppa *ppa)
{
    struct namespace_params *npp = &ns->np;
    struct nand_page *pg_iter = NULL;
    int cnt = 0;

    for (int pg = 0; pg < npp->pgs_per_blk; pg++) {
        ppa->g.pg = pg;
        pg_iter = get_pg(ns->ssd, ppa);
        /* there shouldn't be any free page in victim blocks */
        // ftl_assert(pg_iter->status != PG_FREE);  // not suitable for wear-leveling 
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
    struct namespace_params *npp = &ns->np;
    struct nand_lun *lunp;

    lunp = get_lun(ns->ssd, ppa);
    clean_one_block(ns, ppa);
    mark_block_free(ns, ppa);

    if (npp->enable_gc_delay) {
        struct nand_cmd gce;
        gce.type = GC_IO;
        gce.cmd = NAND_ERASE;
        gce.stime = 0;
        ssd_advance_status(ns->ssd, ppa, &gce);
    }

    get_ch(ns->ssd, ppa)->erase_count++;
    get_ch(ns->ssd, ppa)->erase_count_after_swap++;
    
    lunp->gc_endtime = lunp->next_lun_avail_time;
}

static int do_gc(struct NvmeNamespace *ns, bool force)
{
    struct nand_block *victim_block = NULL;

    if(pqueue_peek(ns->bm->victim_block_pq) == NULL){
        printf("victim_block_pq is empty\n");
        return -1;
    }

    victim_block = select_victim_block(ns, force);
    if (!victim_block) {
        printf("ns%d No victim here!! vpc:%d ipc:%d\r\n", ns->id, ((struct nand_block *)pqueue_peek(ns->bm->victim_block_pq))->vpc, ((struct nand_block *)pqueue_peek(ns->bm->victim_block_pq))->ipc);
        return -1;
    }

    // printf("GC nsid:%d, ch:%d, lun:%d, blk:%d\r\n", ns->id, victim_block->ppa.g.ch, victim_block->ppa.g.lun, victim_block->ppa.g.blk);
    free_block(ns, &victim_block->ppa);
    return 0;
}

static inline bool should_gc(struct NvmeNamespace *ns)
{
    // struct block_mgmt *bm = ns->bm;
    int free_block_cnt = 0;
    struct ppa ppa;
    ppa.ppa = 0;
    for (int i = 0; i < ns->np.nchs; i++){
        for (int j = 0; j < ns->np.luns_per_ch; j++){
            for (int k = 0; k < ns->np.pls_per_lun; k++){
                ppa.g.ch = ns->ch_list[i];
                ppa.g.lun = j;
                ppa.g.pl = k;
                struct nand_plane *pl= get_pl(ns->ssd, &ppa); 
                free_block_cnt += pl->free_block_cnt;
            }
        }
    }
    // if( free_block_cnt != ns->np.tt_blks - ns->bm->victim_block_cnt - ns->np.tt_luns)
    //     printf("free_block_cnt %d %d\n", free_block_cnt, ns->np.tt_blks - ns->bm->victim_block_cnt - ns->np.tt_luns);

    free_block_cnt = ns->np.tt_blks - ns->bm->victim_block_cnt - ns->np.tt_luns;
    return (free_block_cnt <= ns->np.gc_thres_blocks);
}

static inline bool should_gc_high(struct NvmeNamespace *ns)
{
    // struct block_mgmt *bm = ns->bm;
    uint64_t free_block_cnt = 0;
    struct ppa ppa;
    ppa.ppa = 0;
    for (int i = 0; i < ns->np.nchs; i++){
        for (int j = 0; j < ns->np.luns_per_ch; j++){
            for (int k = 0; k < ns->np.pls_per_lun; k++){
                ppa.g.ch = ns->ch_list[i];
                ppa.g.lun = j;
                ppa.g.pl = k;
                struct nand_plane *pl= get_pl(ns->ssd, &ppa); 
                free_block_cnt += pl->free_block_cnt;
            }
        }
    }
    return (free_block_cnt <= ns->np.gc_thres_blocks_high);
}
/*   Swap Functions   */
static void start_swap(struct NvmeNamespace *ns, int ch)
{
    struct nand_block *blk;
    struct ppa ppa;
    ppa.ppa = 0;

    ppa.g.ch = ns->ch_list[ch];
    for (ppa.g.lun = 0; ppa.g.lun < ns->np.luns_per_ch; ppa.g.lun++) {
        get_lun(ns->ssd, &ppa)->swapping_now = true;
        for( ppa.g.blk = 0 ; ppa.g.blk < ns->np.blks_per_lun ; ppa.g.blk++ ){
            blk = get_blk(ns->ssd, &ppa);
            if (blk->pos) {
                pqueue_remove(ns->bm->victim_block_pq, blk);
                blk->pos = 0;
                ns->bm->victim_block_cnt--;
            }
        }
    }

    if( ns->bm->wp_ch == ch ){
        ns_advance_ch(ns);
    }
}

static void finish_swap(struct NvmeNamespace *ns, int ch)
{
    struct ppa ppa;
    ppa.ppa = 0;
    
    ppa.g.ch = ns->ch_list[ch];
    for( ppa.g.lun = 0 ; ppa.g.lun < ns->np.luns_per_ch ; ppa.g.lun++){
        get_lun(ns->ssd, &ppa)->swapping_now = false;
        for( ppa.g.blk = 0 ; ppa.g.blk < ns->np.blks_per_lun ; ppa.g.blk++ ){
            get_blk(ns->ssd, &ppa)->swapped = false;
        }
    }
}

static void wl_read_page(NvmeNamespace *ns, struct ppa *ppa)
{
    // uint64_t lat = 0;
    /* advance ssd status, we don't care about how long it takes */
    struct nand_cmd wlr;
    wlr.type = WL_IO;
    wlr.cmd = NAND_READ;
    wlr.stime = 0;
    ssd_advance_status(ns->ssd, ppa, &wlr);
    
    // return lat;
}

/* move valid page data (already in DRAM) from victim block to a new page */
static uint64_t wl_write_page(struct NvmeNamespace *ns, uint64_t lpn, struct NvmeNamespace *ns2, struct nand_lun *write_lun)
{
    struct ppa ppa;     // page of write_lun
    uint64_t lat = 0;

    ppa.ppa     = 0;
    ppa.g.ch    = write_lun->ppa.g.ch;
    ppa.g.lun   = write_lun->ppa.g.lun;
    ppa.g.blk   = write_lun->wp;
    ppa.g.pg    = write_lun->pl[0].blk[write_lun->wp].wp;

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
    lat = ssd_advance_status(ns->ssd, &ppa, &wlw);
    
    /* advance the write pointer */
    write_lun->pl[0].blk[write_lun->wp].swapped = true;
    lun_advance_write_pointer(ns, write_lun);
    return lat;
}

static int read_one_block(struct NvmeNamespace *ns, struct nand_block *blk, uint64_t *lpn_buffer, int start, int len)
{
    struct ppa ppa;
    struct namespace_params *npp = &ns->np;
    int count = 0;

    if( blk == NULL ){
        return 0;
    }

    ppa.ppa = blk->ppa.ppa;
    for (int pg = 0; pg < npp->pgs_per_blk; pg++) {
        ppa.g.pg = pg;
        if (get_pg(ns->ssd, &ppa)->status == PG_VALID) {
            lpn_buffer[start+count++] = get_rmap_ent(ns, &ppa);
            wl_read_page(ns, &ppa);
            mark_page_invalid(ns, &ppa);
        }
        if( start+count == len )    // if len <= -1, read all pages
            break;
    }

    // // finish block read
    // if(ppa.g.pg >= npp->pgs_per_blk-1){
    //     blk->swapped = true;
    // }

    return count;
}

static void swap_block( struct NvmeNamespace *ns1, struct nand_lun *lun1, struct nand_block *blk1, 
                        struct NvmeNamespace *ns2, struct nand_lun *lun2, struct nand_block *blk2)
{
    struct ssd *ssd = ns1->ssd;
    uint64_t cmd_end_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    uint64_t *lpn_buffer1, *lpn_buffer2;
    int lpn_idx1 = 0, lpn_idx2 = 0;
    lpn_buffer1 = g_malloc0(sizeof(uint64_t) * ns1->np.pgs_per_blk);
    lpn_buffer2 = g_malloc0(sizeof(uint64_t) * ns2->np.pgs_per_blk);

    lpn_idx1 = read_one_block(ns1, blk1, lpn_buffer1, 0, -1);
    lpn_idx2 = read_one_block(ns2, blk2, lpn_buffer2, 0, -1);

    // wait for all page read
    cmd_end_time = cmd_end_time < lun1->next_lun_avail_time ? lun1->next_lun_avail_time : cmd_end_time;
    cmd_end_time = cmd_end_time < lun2->next_lun_avail_time ? lun2->next_lun_avail_time : cmd_end_time;

    /* erase each block */
    struct nand_cmd wle;
    wle.type = WL_IO;
    wle.cmd = NAND_ERASE;
    wle.stime = cmd_end_time;
    if (blk1 != NULL) {
        mark_block_free(ns1, &blk1->ppa);
        ssd_advance_status(ssd, &blk1->ppa, &wle);
    }
    if (blk2 != NULL) {
        mark_block_free(ns2, &blk2->ppa);
        ssd_advance_status(ssd, &blk2->ppa, &wle);
    }

    /* move counterpart chip */
    for (int i = 0; i < lpn_idx1; i++){
        wl_write_page(ns1, lpn_buffer1[i], ns2, lun2);
    }
    for (int i = 0; i < lpn_idx2; i++){
        wl_write_page(ns2, lpn_buffer2[i], ns1, lun1);
    }

    // wait for all page write
    if( blk1 != NULL && blk2 != NULL){
        if (lun1->next_lun_avail_time > lun2->next_lun_avail_time){
            lun2->next_lun_avail_time = lun1->next_lun_avail_time;
        }else{
            lun1->next_lun_avail_time = lun2->next_lun_avail_time;
        }    
    }

    g_free(lpn_buffer1);
    g_free(lpn_buffer2);
}

static struct nand_block *get_next_full_block(struct NvmeNamespace *ns, struct nand_lun *lun, int last_index, int mode)
{
    // 중간부터 찾도록 수정 필요
    nand_block * max_vpc_block = NULL;
    int max_vpc = 0;
    for (int blk = last_index; blk < ns->np.blks_per_lun; blk++) {
        if( lun->pl[0].blk[blk].state == BLOCK_FULL && lun->pl[0].blk[blk].vpc > 0 && lun->pl[0].blk[blk].swapped == false ){
            if(mode == OFFSET)
                return &lun->pl[0].blk[blk];
            if( max_vpc < lun->pl[0].blk[blk].vpc ){
                max_vpc = lun->pl[0].blk[blk].vpc;
                max_vpc_block = &lun->pl[0].blk[blk];
            }
        }
    }
    return max_vpc_block;
}

// static struct nand_block *get_next_invalid_block(struct NvmeNamespace *ns, struct nand_lun *lun, int last_index)
// {
//     // 중간부터 찾도록 수정 필요
//     for (int blk = last_index; blk < ns->np.blks_per_lun; blk++) {
//         if( lun->pl[0].blk[blk].state == BLOCK_FULL && lun->pl[0].blk[blk].vpc == 0 && lun->pl[0].blk[blk].swapped == false ){
//             return &lun->pl[0].blk[blk];
//         }
//     }
//     return NULL;
// }

static void *swap_chip(void *args)
{
    /* parsing args */
    struct chip_swap_args *swap_args = args;
    struct NvmeNamespace *ns1   = swap_args->ns1;
    struct NvmeNamespace *ns2   = swap_args->ns2;
    struct ppa *ppa1            = &swap_args->ppa1;
    struct ppa *ppa2            = &swap_args->ppa2;

    /* variables */
    struct ssd *ssd = ns1->ssd;
    struct nand_lun *lun1 = get_lun(ssd, ppa1);
    struct nand_lun *lun2 = get_lun(ssd, ppa2);
    struct nand_block *blk1;
    struct nand_block *blk2;

    /* Move write point block first */
    blk1 = &lun1->pl[0].blk[lun1->wp];
    blk2 = &lun2->pl[0].blk[lun2->wp];

    /* Move rest full block */
    while( blk1 != NULL || blk2 != NULL ) {
        
        //for debug
        uint64_t now = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);

        /* swap block */
        swap_block(ns1, lun1, blk1, ns2, lun2, blk2);

        // wait block IO 
        do {
            now = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
        }while(now < lun1->next_lun_avail_time || now < lun2->next_lun_avail_time);

        blk1 = get_next_full_block(ns1, lun1, 0, OFFSET);
        blk2 = get_next_full_block(ns2, lun2, 0, OFFSET);
    }
    return NULL;
}

static void *ch_swap(void *args)
{    
    /* parsing args */
    struct ch_swap_args *swap_args = args;
    struct NvmeNamespace *ns1   = swap_args->ns1;
    struct NvmeNamespace *ns2   = swap_args->ns2;
    int ch1                     = swap_args->ch1;
    int ch2                     = swap_args->ch2;

    /* variables */
    struct ssd *ssd = ns1->ssd;
    struct ppa ppa1, ppa2;
    ppa1.ppa = 0;
    ppa2.ppa = 0;

    QemuThread *chip_swap_threads    = g_malloc0(sizeof(QemuThread)           * ssd->sp.luns_per_ch);
    struct chip_swap_args *chip_args = g_malloc0(sizeof(struct chip_swap_args)* ssd->sp.luns_per_ch);

    // for debug
    uint64_t now = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
    uint64_t *origin_avail_time1 = g_malloc0(sizeof(uint64_t) * ssd->sp.luns_per_ch);
    uint64_t *origin_avail_time2 = g_malloc0(sizeof(uint64_t) * ssd->sp.luns_per_ch);
    for( int chip = 0 ; chip < ssd->sp.luns_per_ch ; chip++){
        origin_avail_time1[chip] = ssd->ch[ns1->ch_list[ch1]].lun[chip].next_lun_avail_time;
        origin_avail_time2[chip] = ssd->ch[ns2->ch_list[ch2]].lun[chip].next_lun_avail_time;
        if( origin_avail_time1[chip] < now )
            origin_avail_time1[chip] = now;
        if( origin_avail_time2[chip] < now )
            origin_avail_time2[chip] = now;
    }

    start_swap(ns1, ch1);
    start_swap(ns2, ch2);

    /* Swaps that do not take priority yet.. */
    for (int lun = 0 ; lun < ssd->sp.luns_per_ch ; lun++) {
        /* make ppa */
        ppa1.g.ch = ns1->ch_list[ch1];
        ppa2.g.ch = ns2->ch_list[ch2];
        ppa1.g.lun = lun;
        ppa2.g.lun = lun;

        /* make args */
        chip_args[lun].ns1 = ns1;
        chip_args[lun].ns2 = ns2;
        chip_args[lun].ppa1.ppa = ppa1.ppa;
        chip_args[lun].ppa2.ppa = ppa2.ppa;

        /* swap two chip */
        // qemu_thread_create(&chip_swap_threads[lun], "FEMU-Chip-Swap-Thread", swap_chip_adaptive, &chip_args[lun], QEMU_THREAD_JOINABLE);
        qemu_thread_create(&chip_swap_threads[lun], "FEMU-Chip-Swap-Thread", swap_chip, &chip_args[lun], QEMU_THREAD_JOINABLE);
    }

    /* wait all chip-swap finish */
    for (int lun = 0 ; lun < ssd->sp.luns_per_ch ; lun++) {
        qemu_thread_join(&chip_swap_threads[lun]);
        printf("%d chip is done\r\n", lun);
    }

    finish_swap(ns1, ch1);
    finish_swap(ns2, ch2);

    g_free(chip_swap_threads);
    g_free(chip_args);

    // meta swap
    int temp_ch = ns1->ch_list[ch1];
    ns1->ch_list[ch1] = ns2->ch_list[ch2];
    ns2->ch_list[ch2] = temp_ch;

    // for debug
    printf("now %ldms\r\n", (now - ftl_start)/1000000);
    print_chip_avail_times(ns1, ch1, origin_avail_time1);
    print_chip_avail_times(ns2, ch2, origin_avail_time2);
    
    print_ch_in_ns(ns1);
    print_ch_in_ns(ns2);

    g_free(origin_avail_time1);
    g_free(origin_avail_time2);

    swaping = 0;
    return NULL;
}

void run_swap_thread(struct NvmeNamespace *ns1, int ch1, struct NvmeNamespace *ns2, int ch2)
{
    struct ssd *ssd = ns1->ssd;

    /* make args */
    ssd->args.ns1 = ns1;
    ssd->args.ns2 = ns2;
    ssd->args.ch1 = ch1;
    ssd->args.ch2 = ch2;

    /* swap call */
    swaping = 1;
    qemu_thread_create(&ssd->ch_swap_thread, "FEMU-Ch-Swap-Thread", ch_swap, &ssd->args, QEMU_THREAD_JOINABLE);
}

static uint64_t ssd_read(struct ssd *ssd, NvmeRequest *req)
{
    struct NvmeNamespace * ns = req->ns;        // <- get Namespace!!
    struct namespace_params *npp = &ns->np;
    uint64_t lba = req->slba;
    int nsecs = req->nlb;
    struct ppa ppa;
    uint64_t start_lpn = lba / npp->secs_per_pg;
    uint64_t end_lpn = (lba + nsecs - 1) / npp->secs_per_pg;
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
    }

    return maxlat;
}

static uint64_t ssd_write(struct ssd *ssd, NvmeRequest *req)
{
    uint64_t lba = req->slba;
    struct NvmeNamespace * ns = req->ns;        // <- get Namespace!!
    struct namespace_params *npp = &ns->np;
    int len = req->nlb;
    uint64_t start_lpn = lba / npp->secs_per_pg;
    uint64_t end_lpn = (lba + len - 1) / npp->secs_per_pg;
    struct ppa ppa;
    uint64_t lpn;
    uint64_t curlat = 0, maxlat = 0;
    int r;

    if (end_lpn >= npp->tt_pgs) {
        ftl_err("start_lpn=%"PRIu64",tt_pgs=%d\r\n", start_lpn, ns->np.tt_pgs);
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
        curlat = ssd_advance_status(ns->ssd, &ppa, &swr);
        maxlat = (curlat > maxlat) ? curlat : maxlat;

        user_write(ns->statistic);
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

    uint64_t next_time = 0, sec = 0;
    ftl_start = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
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
            // for (int i = 0; i < n->num_namespaces; i++)
            // {
            //     if (should_gc(&n->namespaces[i])) {
            //         do_gc(&n->namespaces[i], false);
            //     }
            // }
        
            if (should_gc(req->ns)) {
                do_gc(req->ns, false);
            }
        }

        // monitoring
        uint64_t now = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
        if( now >= next_time ){
            sec++;
            /* flush to file */
            flush_to_file(ssd->statistics, n->num_namespaces, sec);
            monitoring_to_file(n);

            /* init time unit */
            for (int i = 0; i < n->num_namespaces; i++){
                one_clock(n->namespaces[i].statistic);
            }

            /* timer setting */
            next_time = now + 1*1000*1000*100;
        }
    }
    return NULL;
}

void monitoring_to_file(void *arg)
{
    FemuCtrl *n = (FemuCtrl *)arg;
    FILE *fp;
    fp = fopen("monitoring.txt", "w");

    for( int i = 0; i < n->num_namespaces; i++ ){
        struct NvmeNamespace *ns = &n->namespaces[i];
        int ipc = pqueue_peek(ns->bm->victim_block_pq) != NULL ? ((nand_block *)pqueue_peek(ns->bm->victim_block_pq))->ipc : 0;
        fprintf(fp, "ns%d( ", i);
        for( int i = 0 ; i < ns->np.nchs ; i++){
            fprintf(fp, "%2dch ", ns->ch_list[i]);
        }
        fprintf(fp, ") victim_block %d, max_ipc %d\r\n", ns->bm->victim_block_cnt, ipc);
    }

    for( int ch = 0; ch < n->ssd->sp.nchs; ch++ ){
        for( int lun = 0; lun < n->ssd->sp.luns_per_ch; lun++ ){
            int vpc = 0;
            for( int blk = 0; blk < n->ssd->sp.blks_per_pl; blk++ ){
                vpc += n->ssd->ch[ch].lun[lun].pl[0].blk[blk].vpc;
            }
            fprintf(fp, "%2dch %dchip free %-3d tot_vp %-5d vpc ", ch, lun, n->ssd->ch[ch].lun[lun].pl[0].free_block_cnt, vpc);

            for( int blk = 0; blk < n->ssd->sp.blks_per_pl; blk++ ){
                if(n->ssd->ch[ch].lun[lun].pl[0].blk[blk].state == BLOCK_FREE)
                    fprintf(fp, "  - ");
                else if(n->ssd->ch[ch].lun[lun].pl[0].blk[blk].state == BLOCK_OPEN)
                    fprintf(fp, "%3d<", n->ssd->ch[ch].lun[lun].pl[0].blk[blk].vpc);
                else
                    fprintf(fp, "%3d ", n->ssd->ch[ch].lun[lun].pl[0].blk[blk].vpc);
            }
            fprintf(fp, "\n");
        }
    }
    fclose(fp);
}

/******************** for debugging  ********************/


void print_ch_in_ns(struct NvmeNamespace *ns)
{
    printf("nsid:%d( ", ns->id);
    for( int i = 0 ; i < ns->np.nchs ; i++){
        printf("%2dch ", ns->ch_list[i]);
    }
    printf(")\r\n");
}

void print_chip_avail_times(struct NvmeNamespace *ns, int ch, uint64_t * origin_avail_time)
{
    struct ssd * ssd = ns->ssd;

    for( int chip = 0 ; chip < ssd->sp.luns_per_ch ; chip++){
        uint64_t curr = ssd->ch[ns->ch_list[ch]].lun[chip].next_lun_avail_time;
        uint64_t diff = curr - origin_avail_time[chip];
        printf("ns%d ch%d, chip%d lun avail time : delta(ms) %5ld\r\n", ns->id, ch, chip, diff/1000000);
    }
}
