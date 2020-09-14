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
	/* required */
	struct rpma_peer *peer;
	struct rpma_conn *conn;
	struct rpma_mr_remote *mr_remote;

	struct rpma_mr_local *mr_local;

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
	int ret = 0;
	/*
	- rpma_peer_new(ip)
	- rpma_conn_cfg_set_sq_size(iodepth + 1)
	- rpma_conn_req_new(ip, port);
	- rpma_conn_req_connect()
	- rpma_conn_get_private_data(&mr_remote)
	- rpma_mr_remote_from_descriptor()
	- rpma_mr_remote_size() >= size
	*/
	return ret;
}

static void fio_librpma_srvio_cleanup(struct thread_data *td)
{
	struct librpma_srvio_data *rd = td->io_ops_data;
	/*
	- rpma_disconnect etc.
	*/
	if (rd)
		free(rd);
}

static int fio_librpma_srvio_setup(struct thread_data *td)
{

	struct librpma_srvio_data *rd = td->io_ops_data;
	/*	
	 - alloc private data (io_ops_data)
	 */
	return 0;
}

FIO_STATIC struct ioengine_ops ioengine = {
	.name			= "librpma_srv",
	.version		= FIO_IOOPS_VERSION,
	.setup			= fio_librpma_srvio_setup,
	.init			= fio_librpma_srvio_init,
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
