/*
 * librpma_srv I/O engine
 *
 * librpma_srv engine should be used together with librpma_clinet engine
 *
 * librpma_srv I/O engine based on the librpma PMDK library.
 * Supports both RDMA memory semantics and channel semantics
 *   for the InfiniBand, RoCE and iWARP protocols.
 * Supports both persistent and volatile memory.
 *
 * You will need the librpma library installed
 *
*
*/
#include "../fio.h"
#include "../optgroup.h"

#include <librpma.h>

#define rpma_td_verror(td, err, func) \
	td_vmsg((td), (err), rpma_err_2str(err), (func))

#define HARDCODED_RQ_SIZE 1
#define HARDCODED_CQ_SIZE 1

#define FIO_RDMA_MAX_IO_DEPTH    512


struct librpma_srvio_options {
	struct thread_data *td;
	char *listen_port;
	char *listen_ip;
	char *size;
};

static struct fio_option options[] = {
	{
		.name	= "listen_ip",
		.lname	= "librpma engine server ip",
		.type	= FIO_OPT_STR_STORE,
		.off1	= offsetof(struct librpma_srvio_options, listen_ip),
		.help	= "ip of server engine",
		.def    = "127.0.0.1",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_LIBRPMA,
	},

	{
		.name	= "listen_port",
		.lname	= "librpma_server engine listen port",
		.type	= FIO_OPT_STR_STORE,
		.off1	= offsetof(struct librpma_srvio_options, listen_port),
		.help	= "Port to use for rpma connections",
		.def    = "4040",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_LIBRPMA,
	},
	{
		.name	= "size",
		.lname	= "Size",
		.type	= FIO_OPT_STR_STORE,
		.off1	= offsetof(struct librpma_srvio_options, size),
		.help	= "Size",
		.def    = "",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_LIBRPMA,
	},

	{
		.name	= NULL,
	},
};

struct remote_u {
	uint64_t buf;
	uint32_t rkey;
	uint32_t size;
};

struct librpma_srv_info_blk {
	uint32_t mode;		/* channel semantic or memory semantic */
	uint32_t nr;		/* client: io depth
				   server: number of records for memory semantic
				 */
	uint32_t max_bs;        /* maximum block size */
	struct remote_u rmt_us[FIO_RDMA_MAX_IO_DEPTH];
};

struct librpma_srvio_data {
	/* created on init() */
	struct rpma_peer *peer;

	/* created on iomem_alloc() */
	struct rpma_mr_local *mr_local;

	/* create on post_init() */
	struct rpma_ep *ep;
	struct rpma_conn *conn;
};


static struct io_u *fio_librpma_srvio_event(struct thread_data *td, int event)
{
	struct librpma_srvio_data *rd = td->io_ops_data;

	return 0;
}

static int fio_librpma_srvio_getevents(struct thread_data *td, unsigned int min,
				unsigned int max, const struct timespec *t)
{
	struct librpma_srvio_data *rd = td->io_ops_data;

	return 0;
}

static enum fio_q_status fio_librpma_srvio_queue(struct thread_data *td,
					  struct io_u *io_u)
{
	return FIO_Q_QUEUED;
}


static int fio_librpma_srvio_commit(struct thread_data *td)
{
	struct librpma_srvio_data *rd = td->io_ops_data;
	return 0;
}

static int fio_librpma_srvio_open_file(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static int fio_librpma_srvio_close_file(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static int fio_librpma_srvio_init(struct thread_data *td)
{
	struct librpma_srvio_data *rd = td->io_ops_data;
	struct librpma_srvio_options *o = td->eo;
	struct ibv_context *dev = NULL;
	int ret;

	/* obtain an IBV context for a local IP address */
	if ((ret = rpma_utils_get_ibv_context(o->listen_ip,
			RPMA_UTIL_IBV_CONTEXT_LOCAL, &dev))) {
		rpma_td_verror(td, ret, "rpma_utils_get_ibv_context");
		return 1;
	}

	/* create a new peer object */
	if ((ret = rpma_peer_new(dev, &rd->peer))) {
		rpma_td_verror(td, ret, "rpma_peer_new");
		return 1;
	}

	return 0;
}

static int fio_librpma_srvio_post_init(struct thread_data *td)
{
	struct librpma_srvio_data *rd = td->io_ops_data;
	struct librpma_srvio_options *o = td->eo;
	struct rpma_conn_cfg *cfg;
	struct rpma_conn_req *req;
	rpma_mr_descriptor mr_desc;
	struct rpma_conn_private_data pdata;
	enum rpma_conn_event conn_event = RPMA_CONN_UNDEFINED;
	int ret;

	/* get local memory region descriptor */
	if ((ret = rpma_mr_get_descriptor(rd->mr_local, &mr_desc))) {
		rpma_td_verror(td, ret, "rpma_mr_get_descriptor");
		return 1;
	}
	pdata.ptr = (void *)&mr_desc;
	pdata.len = sizeof(rpma_mr_descriptor);

	/* create a new connection configuration object */
	if ((ret = rpma_conn_cfg_new(&cfg))) {
		rpma_td_verror(td, ret, "rpma_conn_cfg_new");
		return 1;
	}

	/* cannot fail if cfg != NULL */
	/* # of writes/reads + flush (required for writes) */
	(void) rpma_conn_cfg_set_sq_size(cfg, td->o.iodepth + 1);
	(void) rpma_conn_cfg_set_rq_size(cfg, HARDCODED_RQ_SIZE);
	(void) rpma_conn_cfg_set_cq_size(cfg, HARDCODED_CQ_SIZE);

	/* create a new endpoint object */
	if ((ret = rpma_ep_listen(rd->peer, o->listen_ip, o->listen_port,
			&rd->ep))) {
		rpma_td_verror(td, ret, "rpma_ep_listen");
		(void) rpma_conn_cfg_delete(&cfg);
		return 1;
	}

	/* obtain an incoming connection request */
	if ((ret = rpma_ep_next_conn_req(rd->ep, cfg, &req))) {
		rpma_td_verror(td, ret, "rpma_ep_next_conn_req");
		(void) rpma_conn_cfg_delete(&cfg);
		return 1;
	}

	(void) rpma_conn_cfg_delete(&cfg);

	/* accept the connection request and get the connection object */
	if ((ret = rpma_conn_req_connect(&req, &pdata, &rd->conn))) {
		rpma_td_verror(td, ret, "rpma_conn_req_connect");
		(void) rpma_conn_req_delete(&req);
		return 1;
	}

	/* wait for the connection to being establish */
	if ((ret = rpma_conn_next_event(rd->conn, &conn_event))) {
		rpma_td_verror(td, ret, "rpma_conn_next_event");
		return 1;
	} else if (conn_event != RPMA_CONN_ESTABLISHED) {
		td_vmsg(td, 1, "an unexpected event returned",
				"rpma_conn_next_event");
		return 1;
	}

	return 0;
}

static void fio_librpma_srvio_cleanup(struct thread_data *td)
{
	struct librpma_srvio_data *rd = td->io_ops_data;
	enum rpma_conn_event conn_event = RPMA_CONN_UNDEFINED;
	int ret;

	/* just in case a connection has not been established */
	if (rd->conn) {
		/* wait for the connection to being closed */
		if ((ret = rpma_conn_next_event(rd->conn, &conn_event))) {
			rpma_td_verror(td, ret, "rpma_conn_next_event");
		} else if (conn_event != RPMA_CONN_CLOSED) {
			td_vmsg(td, 1, "an unexpected event returned",
					"rpma_conn_next_event");
		}

		/* disconnect the connection */
		if ((ret = rpma_conn_disconnect(rd->conn)))
			rpma_td_verror(td, ret, "rpma_conn_disconnect");

		/* delete the connection object */
		if ((ret = rpma_conn_delete(&rd->conn)))
			rpma_td_verror(td, ret, "rpma_conn_delete");
	}

	/* just in case peer has not been created */
	if (rd->peer) {
		/* delete the peer object */
		if ((ret = rpma_peer_delete(&rd->peer)))
			rpma_td_verror(td, ret, "rpma_peer_delete");
	}

	/* free engine private data */
	if (rd)
		free(rd);
	td->io_ops_data = NULL;
}

static int fio_librpma_srvio_setup(struct thread_data *td)
{
	/* allocate engine private data */
	struct librpma_srvio_data *rd =
			malloc(sizeof(struct librpma_srvio_data));
	memset(rd, 0, sizeof(struct librpma_srvio_data));
	td->io_ops_data = rd;

	return 0;
}

FIO_STATIC struct ioengine_ops ioengine = {
	.name			= "librpma_srv",
	.version		= FIO_IOOPS_VERSION,
	.setup			= fio_librpma_srvio_setup,
	.init			= fio_librpma_srvio_init,
	.post_init		= fio_librpma_srvio_post_init,
	.queue			= fio_librpma_srvio_queue,
	.commit			= fio_librpma_srvio_commit,
	.getevents		= fio_librpma_srvio_getevents,
	.event			= fio_librpma_srvio_event,
	.cleanup		= fio_librpma_srvio_cleanup,
	.open_file		= fio_librpma_srvio_open_file,
	.close_file		= fio_librpma_srvio_close_file,
	.flags			= FIO_DISKLESSIO | FIO_UNIDIR | FIO_PIPEIO,
	.options		= options,
	.option_struct_size	= sizeof(struct librpma_srvio_options),
};

static void fio_init fio_librpma_srvio_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_librpma_srvio_unregister(void)
{
	unregister_ioengine(&ioengine);
}
