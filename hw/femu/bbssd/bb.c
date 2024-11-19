#include "../nvme.h"
#include "./ftl.h"

static void bb_init_ctrl_str(FemuCtrl *n)
{
    static int fsid_vbb = 0;
    const char *vbbssd_mn = "FEMU BlackBox-SSD Controller";
    const char *vbbssd_sn = "vSSD";

    nvme_set_ctrl_name(n, vbbssd_mn, vbbssd_sn, &fsid_vbb);
}

/* bb <=> black-box */
static void bb_init(FemuCtrl *n, Error **errp)
{
    struct ssd *ssd = n->ssd = g_malloc0(sizeof(struct ssd));

    bb_init_ctrl_str(n);

    ssd->dataplane_started_ptr = &n->dataplane_started;
    ssd->ssdname = (char *)n->devname;
    femu_debug("Starting FEMU in Blackbox-SSD mode ...\n");
    ssd_init(n);
}

static void bb_flip(FemuCtrl *n, NvmeCmd *cmd)
{
    struct ssd *ssd = n->ssd;
    int64_t cdw10 = le64_to_cpu(cmd->cdw10);

    switch (cdw10) {
    case FEMU_ENABLE_GC_DELAY:
        ssd->sp.enable_gc_delay = true;
        femu_log("%s,FEMU GC Delay Emulation [Enabled]!\n", n->devname);
        break;
    case FEMU_DISABLE_GC_DELAY:
        ssd->sp.enable_gc_delay = false;
        femu_log("%s,FEMU GC Delay Emulation [Disabled]!\n", n->devname);
        break;
    case FEMU_ENABLE_DELAY_EMU:
        ssd->sp.pg_rd_lat = NAND_READ_LATENCY;
        ssd->sp.pg_wr_lat = NAND_PROG_LATENCY;
        ssd->sp.blk_er_lat = NAND_ERASE_LATENCY;
        ssd->sp.ch_xfer_lat = 0;
        femu_log("%s,FEMU Delay Emulation [Enabled]!\n", n->devname);
        break;
    case FEMU_DISABLE_DELAY_EMU:
        ssd->sp.pg_rd_lat = 0;
        ssd->sp.pg_wr_lat = 0;
        ssd->sp.blk_er_lat = 0;
        ssd->sp.ch_xfer_lat = 0;
        femu_log("%s,FEMU Delay Emulation [Disabled]!\n", n->devname);
        break;
    case FEMU_RESET_ACCT:
        n->nr_tt_ios = 0;
        n->nr_tt_late_ios = 0;
        femu_log("%s,Reset tt_late_ios/tt_ios,%lu/%lu\n", n->devname,
                n->nr_tt_late_ios, n->nr_tt_ios);
        break;
    case FEMU_ENABLE_LOG:
        n->print_log = true;
        femu_log("%s,Log print [Enabled]!\n", n->devname);
        break;
    case FEMU_DISABLE_LOG:
        n->print_log = false;
        femu_log("%s,Log print [Disabled]!\n", n->devname);
        break;
    default:
        printf("FEMU:%s,Not implemented flip cmd (%lu)\n", n->devname, cdw10);
    }
}

static uint16_t bb_nvme_rw(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                           NvmeRequest *req)
{
    return nvme_rw(n, ns, cmd, req);
}

static uint16_t bb_io_cmd(FemuCtrl *n, NvmeNamespace *ns, NvmeCmd *cmd,
                          NvmeRequest *req)
{
    switch (cmd->opcode) {
    case NVME_CMD_READ:
    case NVME_CMD_WRITE:
        return bb_nvme_rw(n, ns, cmd, req);
    default:
        return NVME_INVALID_OPCODE | NVME_DNR;
    }
}

static uint16_t bb_admin_cmd(FemuCtrl *n, NvmeCmd *cmd)
{
    switch (cmd->opcode) {
    case NVME_ADM_CMD_FEMU_FLIP:
        bb_flip(n, cmd);
        return NVME_SUCCESS;

    case 0x32:      // 명시적 swap
        int nsid1, ch1, nsid2, ch2;

        nsid1 = le64_to_cpu(cmd->cdw10);
        ch1   = le64_to_cpu(cmd->cdw11);
        nsid2 = le64_to_cpu(cmd->cdw12);
        ch2   = le64_to_cpu(cmd->cdw13);

        struct NvmeNamespace *ns1 = &n->namespaces[nsid1-1];
        struct NvmeNamespace *ns2 = &n->namespaces[nsid2-1];

        swap_channel(ns1, ch1, ns2, ch2);

        return NVME_SUCCESS;

    case 0x33:     // set mode
        int mode;
        mode = le64_to_cpu(cmd->cdw10);
        switch (mode){
            case 0: 
                femu_log("[ CAST ] Baseline Swap\n\r");
                n->ssd->mode = MODE_NORMAL;
                break;
            case 1: 
                femu_log("[ CAST ] Adaptive Swap\n\r");
                n->ssd->mode = MODE_ADAPTIVE;
                break;
            case 2: 
                femu_log("[ CAST ] Seamless Swap\n\r");
                n->ssd->mode = MODE_SEAMLESS;
                break;
        }
        return NVME_SUCCESS;

    case 0x34:      // start logging
        for (int j = 0; j < n->num_namespaces; j++){
            statistic_delete(n->namespaces[j].statistic);
            statistic_init(n->namespaces[j].statistic);

            // delete_lat_list(n->namespaces[j].lat_list);
            // n->namespaces[j].lat_list = new_lat_list();
        }

        n->ssd->start_log_time = qemu_clock_get_ns(QEMU_CLOCK_REALTIME);
        n->ssd->next_log_time = n->ssd->start_log_time + 1*1000*1000*1000;
        return NVME_SUCCESS;

    case 0x35:      // fast IO mode
        n->ssd->sp.pg_rd_lat = 1000;
        n->ssd->sp.pg_wr_lat = 1000;
        femu_log("[ CAST ] Fast IO mode\n\r");
        return NVME_SUCCESS;

    case 0x36:     // normal IO mode
        n->ssd->sp.pg_rd_lat = n->bb_params.pg_rd_lat;
        n->ssd->sp.pg_wr_lat = n->bb_params.pg_wr_lat;
        femu_log("[ CAST ] Normal IO mode\n\r");
        return NVME_SUCCESS;

    default:
        return NVME_INVALID_OPCODE | NVME_DNR;
    }
}

int nvme_register_bbssd(FemuCtrl *n)
{
    n->ext_ops = (FemuExtCtrlOps) {
        .state            = NULL,
        .init             = bb_init,
        .exit             = NULL,
        .rw_check_req     = NULL,
        .admin_cmd        = bb_admin_cmd,
        .io_cmd           = bb_io_cmd,
        .get_log          = NULL,
    };

    return 0;
}

