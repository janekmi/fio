/*
 * librpma_server I/O engine
 *
 * librpma_server engine should be used together with librpma_clinet engine
 *
 * librpma_server I/O engine based on the librpma PMDK library.
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


struct librpma_server_io_options {
	struct thread_data *td;
	char *listen_port;
	char *listen_ip;
};

static struct fio_option options[] = {
	{
		.name	= "listen_ip",
		.lname	= "librpma_server engine listen ip",
		.type	= FIO_OPT_STR_STORE,
		.off1	= offsetof(struct librpma_server_io_options, listen_ip),
		.help	= "IP to listen for RDMA connections",
		.def    = "",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_LIBRPMA,
	},

	{
		.name	= "listen_port",
		.lname	= "librpma_server engine listen port",
		.type	= FIO_OPT_STR_STORE,
		.off1	= offsetof(struct librpma_server_io_options, listen_port),
		.help	= "Port to listen for RDMA connections",
		.def    = "",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_LIBRPMA,
	},

	{
		.name	= NULL,
	},
};

struct librpma_server_io_data {
	/* required */
	struct rpma_peer *peer;
	struct rpma_conn *conn;
	struct rpma_mr_remote *mr_remote;

	struct rpma_mr_local *mr_local;

};


static struct io_u *fio_librpma_server_io_event(struct thread_data *td, int event)
{
	struct librpma_server_io_data *rd = td->io_ops_data;

	return 0;
}

static int fio_librpma_server_io_getevents(struct thread_data *td, unsigned int min,
				unsigned int max, const struct timespec *t)
{
	struct librpma_server_io_data *rd = td->io_ops_data;

	return 0;
}

static enum fio_q_status fio_librpma_server_io_queue(struct thread_data *td,
					  struct io_u *io_u)
{
	return FIO_Q_QUEUED;
}


static int fio_librpma_server_io_commit(struct thread_data *td)
{
	struct librpma_server_io_data *rd = td->io_ops_data;
	return 0;
}



static int fio_librpma_server_io_open_file(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static int fio_librpma_server_io_close_file(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static int fio_librpma_server_io_init(struct thread_data *td)
{
	struct librpma_server_io_data *rd = td->io_ops_data;
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

static void fio_librpma_server_io_cleanup(struct thread_data *td)
{
	struct librpma_server_io_data *rd = td->io_ops_data;
	/*
	- rpma_disconnect etc.
	*/
	if (rd)
		free(rd);
}

static int fio_librpma_server_io_setup(struct thread_data *td)
{

	struct librpma_server_io_data *rd = td->io_ops_data;
	/*	
	 - alloc private data (io_ops_data)
	 */
	return 0;
}

FIO_STATIC struct ioengine_ops ioengine = {
	.name			= "librpma_server",
	.version		= FIO_IOOPS_VERSION,
	.setup			= fio_librpma_server_io_setup,
	.init			= fio_librpma_server_io_init,
	.queue			= fio_librpma_server_io_queue,
	.commit			= fio_librpma_server_io_commit,
	.getevents		= fio_librpma_server_io_getevents,
	.event			= fio_librpma_server_io_event,
	.cleanup		= fio_librpma_server_io_cleanup,
	.open_file		= fio_librpma_server_io_open_file,
	.close_file		= fio_librpma_server_io_close_file,
	.flags			= FIO_DISKLESSIO | FIO_UNIDIR | FIO_PIPEIO,
	.options		= options,
	.option_struct_size	= sizeof(struct librpma_server_io_options),
};

static void fio_init fio_librpma_server_io_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_librpma_server_io_unregister(void)
{
	unregister_ioengine(&ioengine);
}
