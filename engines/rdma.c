/*
 * RDMA I/O engine
 *
 * RDMA I/O engine based on the IB verbs and RDMA/CM user space libraries.
 * Supports both RDMA memory semantics and channel semantics
 *   for the InfiniBand, RoCE and iWARP protocols.
 *
 * You will need the Linux RDMA software installed, either
 * from your Linux distributor or directly from openfabrics.org:
 *
 * http://www.openfabrics.org/downloads/OFED/
 *
 * Exchanging steps of RDMA ioengine control messages:
 *	1. client side sends test mode (RDMA_WRITE/RDMA_READ/SEND)
 *	   to server side.
 *	2. server side parses test mode, and sends back confirmation
 *	   to client side. In RDMA WRITE/READ test, this confirmation
 *	   includes memory information, such as rkey, address.
 *	3. client side initiates test loop.
 *	4. In RDMA WRITE/READ test, client side sends a completion
 *	   notification to server side. Server side updates its
 *	   td->done as true.
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <pthread.h>
#include <inttypes.h>

#include "../fio.h"
#include "../hash.h"
#include "../optgroup.h"

#include <librpma.h>

#define FIO_RDMA_MAX_IO_DEPTH    512
#define KILOBYTE 1024

#define client_peer_via_address(addr, peer_ptr) \
		common_peer_via_address(addr, RPMA_UTIL_IBV_CONTEXT_REMOTE, \
				peer_ptr)

#define server_peer_via_address(addr, peer_ptr) \
		common_peer_via_address(addr, RPMA_UTIL_IBV_CONTEXT_LOCAL, \
				peer_ptr)

struct common_data {
	rpma_mr_descriptor desc;
	size_t data_offset;
};

enum rdma_io_mode {
	FIO_RDMA_UNKNOWN = 0,
	FIO_RDMA_MEM_WRITE,
	FIO_RDMA_MEM_READ,
};

struct rdmaio_options {
	struct thread_data *td;
	unsigned int port;
	enum rdma_io_mode verb;
	char *bindname;
};

static int str_hostname_cb(void *data, const char *input)
{
	struct rdmaio_options *o = data;

	if (o->td->o.filename)
		free(o->td->o.filename);
	o->td->o.filename = strdup(input);
	return 0;
}

static struct fio_option options[] = {
	{
		.name	= "hostname",
		.lname	= "rdma engine hostname",
		.type	= FIO_OPT_STR_STORE,
		.cb	= str_hostname_cb,
		.help	= "Hostname for RDMA IO engine",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_RDMA,
	},
	{
		.name	= "bindname",
		.lname	= "rdma engine bindname",
		.type	= FIO_OPT_STR_STORE,
		.off1	= offsetof(struct rdmaio_options, bindname),
		.help	= "Bind for RDMA IO engine",
		.def    = "",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_RDMA,
	},
	{
		.name	= "port",
		.lname	= "rdma engine port",
		.type	= FIO_OPT_INT,
		.off1	= offsetof(struct rdmaio_options, port),
		.minval	= 1,
		.maxval	= 65535,
		.help	= "Port to use for RDMA connections",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_RDMA,
	},
	{
		.name	= "verb",
		.lname	= "RDMA engine verb",
		.alias	= "proto",
		.type	= FIO_OPT_STR,
		.off1	= offsetof(struct rdmaio_options, verb),
		.help	= "RDMA engine verb",
		.def	= "write",
		.posval = {
			  { .ival = "write",
			    .oval = FIO_RDMA_MEM_WRITE,
			    .help = "Memory Write",
			  },
			  { .ival = "read",
			    .oval = FIO_RDMA_MEM_READ,
			    .help = "Memory Read",
			  },
		},
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_RDMA,
	},
	{
		.name	= NULL,
	},
};



struct rdmaio_data {
	int is_client;
	enum rdma_io_mode rdma_protocol;
	char host[64];
	//struct sockaddr_in addr;
	char* addr;
	char  port[10];

	struct rpma_conn *conn;
	struct rpma_peer *peer;

	struct ibv_recv_wr rq_wr;
	struct ibv_sge recv_sgl;
	//struct rdma_info_blk recv_buf;
	size_t dst_size;
	size_t dst_offset;
	// struct ibv_mr *recv_mr;
	struct rpma_mr_remote *recv_mr;//this is dst_mr

	struct ibv_send_wr sq_wr;
	struct ibv_sge send_sgl;
	//struct rdma_info_blk send_buf;
	void *mr_ptr;
	size_t mr_size;
	// struct ibv_mr *send_mr;
	struct rpma_mr_local *send_mr;//this is src_mr

	struct io_u **io_us_queued;
	int io_u_queued_nr;
	struct io_u **io_us_flight;
	int io_u_flight_nr;
	struct io_u **io_us_completed;
	int io_u_completed_nr;

	struct frand_state rand_state;
};
static int fio_rdmaio_connect(struct thread_data *td);

int
common_peer_via_address(const char *addr, enum rpma_util_ibv_context_type type,
		struct rpma_peer **peer_ptr)
{
	struct ibv_context *dev = NULL;

	int ret = rpma_utils_get_ibv_context(addr, type, &dev);
	if (ret)
		return ret;

	/* create a new peer object */
	return rpma_peer_new(dev, peer_ptr);
}

/*
 * client_connect -- establish a new connection to a server listening at
 * addr:service
 */
int
client_connect(struct rpma_peer *peer, const char *addr, const char *service,
		struct rpma_conn_private_data *pdata,
		struct rpma_conn **conn_ptr)
{
	struct rpma_conn_req *req = NULL;
	enum rpma_conn_event conn_event = RPMA_CONN_UNDEFINED;

	/* create a connection request */
	int ret = rpma_conn_req_new(peer, addr, service, &req);
	if (ret)
		return ret;

	/* connect the connection request and obtain the connection object */
	ret = rpma_conn_req_connect(&req, pdata, conn_ptr);
	if (ret) {
		(void) rpma_conn_req_delete(&req);
		return ret;
	}

	/* wait for the connection to establish */
	ret = rpma_conn_next_event(*conn_ptr, &conn_event);
	if (ret) {
		goto err_conn_delete;
	} else if (conn_event != RPMA_CONN_ESTABLISHED) {
		fprintf(stderr,
				"rpma_conn_next_event returned an unexptected event\n");
		goto err_conn_delete;
	}

	return 0;

err_conn_delete:
	(void) rpma_conn_delete(conn_ptr);

	return ret;
}

/*
 * server_accept_connection -- wait for an incoming connection request,
 * accept it and wait for its establishment
 */
int
server_accept_connection(struct rpma_ep *ep,
		struct rpma_conn_private_data *pdata,
		struct rpma_conn **conn_ptr)
{
	struct rpma_conn_req *req = NULL;
	enum rpma_conn_event conn_event = RPMA_CONN_UNDEFINED;

	/* receive an incoming connection request */
	int ret = rpma_ep_next_conn_req(ep, &req);
	if (ret)
		return ret;

	/*
	 * connect / accept the connection request and obtain the connection
	 * object
	 */
	ret = rpma_conn_req_connect(&req, pdata, conn_ptr);
	if (ret) {
		(void) rpma_conn_req_delete(&req);
		return ret;
	}

	/* wait for the connection to be established */
	ret = rpma_conn_next_event(*conn_ptr, &conn_event);
	if (!ret && conn_event != RPMA_CONN_ESTABLISHED) {
		fprintf(stderr,
				"rpma_conn_next_event returned an unexptected event\n");
		ret = -1;
	}

	if (ret)
		(void) rpma_conn_delete(conn_ptr);

	return ret;
}

/*
 * common_wait_for_conn_close_verbose -- wait for RPMA_CONN_CLOSED and print
 * an error message on error
 */
static inline int
common_wait_for_conn_close_verbose(struct rpma_conn *conn)
{
	enum rpma_conn_event conn_event = RPMA_CONN_UNDEFINED;

	/* wait for the connection to be closed */
	int ret = rpma_conn_next_event(conn, &conn_event);
	if (!ret && conn_event != RPMA_CONN_CLOSED) {
		fprintf(stderr,
				"rpma_conn_next_event returned an unexptected event\n");
	}

	return ret;
}

/*
 * common_wait_for_conn_close_and_disconnect -- wait for RPMA_CONN_CLOSED,
 * disconnect and delete the connection structure
 */
int
common_wait_for_conn_close_and_disconnect(struct rpma_conn **conn_ptr)
{
	int ret = 0;
	ret |= common_wait_for_conn_close_verbose(*conn_ptr);
	ret |= rpma_conn_disconnect(*conn_ptr);
	ret |= rpma_conn_delete(conn_ptr);

	return ret;
}

/*
 * common_disconnect_and_wait_for_conn_close -- disconnect, wait for
 * RPMA_CONN_CLOSED and delete the connection structure
 */
int
common_disconnect_and_wait_for_conn_close(struct rpma_conn **conn_ptr)
{
	int ret = 0;

	ret |= rpma_conn_disconnect(*conn_ptr);
	if (ret == 0)
		ret |= common_wait_for_conn_close_verbose(*conn_ptr);

	ret |= rpma_conn_delete(conn_ptr);

	return ret;
}


static int fio_rdmaio_setup_connect(struct thread_data *td, const char *host,
				    unsigned short port)
{
	struct rdmaio_data *rd = td->io_ops_data;
	int is_pmem;
	int ret;
	struct rdmaio_options *o = td->eo;
	struct sockaddr_storage addrb;

	int err;
	//rd->addr.sin_port = htons(port);
    snprintf(rd->port,10,"%d",port);
	rd->addr = host;

	// rd->mr_ptr = pmem_map_file(f->file_name, 0 /* len */, 0 /* flags */,
	// 			0 /* mode */, &rd->mr_size, &is_pmem);
	rd->mr_ptr = pmem_map_file("/dev/dax0.0", 0 /* len */, 0 /* flags */,
				0 /* mode */, &rd->mr_size, &is_pmem);
	if (rd->mr_ptr == NULL)
		return -1;

	/* pmem is expected */
	if (!is_pmem) {
		(void) pmem_unmap(rd->mr_ptr, rd->mr_size);
		return -1;
	}
	 fio_rdmaio_connect(td);


   /*here we need to map file to get mr_ptr*/

	rpma_mr_reg(rd->peer, rd->mr_ptr, rd->mr_size,
			RPMA_MR_USAGE_WRITE_SRC,
			RPMA_MR_PLT_PERSISTENT, &rd->send_mr);

	if (rd->send_mr == NULL) {
		log_err("fio: send_buf reg_mr failed: %m\n");
		//ibv_dereg_mr(rd->recv_mr);
		rpma_mr_dereg(&rd->recv_mr);
		return 1;
	}

	/* obtain the remote memory description */
	struct rpma_conn_private_data pdata;
	ret = rpma_conn_get_private_data(rd->conn, &pdata);
	// if (ret != 0 || pdata.len < sizeof(struct common_data))
		// goto err_mr_dereg;

	/*
	 * Create a remote memory registration structure from the received
	 * descriptor.
	 */
	struct common_data *dst_data = pdata.ptr;
	rd->dst_offset = dst_data->data_offset;
	ret = rpma_mr_remote_from_descriptor(&dst_data->desc, &rd->recv_mr);
	// if (ret)
	// 	goto err_mr_dereg;

	/* get the remote memory region size */
	ret = rpma_mr_remote_get_size(rd->recv_mr, &rd->dst_size);
	if (ret) {
		
	} else if (rd->dst_size < KILOBYTE) {
		fprintf(stderr,
				"Remote memory region size too small for writing the data of the assumed size (%zu < %d)\n",
				rd->dst_size, KILOBYTE);
		
	}
	if (rd->recv_mr == NULL) {
		log_err("fio: recv_buf reg_mr failed: %m\n");
		return 1;
	}


	return 0;
}


static int fio_rdmaio_prep(struct thread_data *td, struct io_u *io_u)
{
	struct rdmaio_data *rd = td->io_ops_data;

	switch (rd->rdma_protocol) {
	case FIO_RDMA_MEM_WRITE:
	case FIO_RDMA_MEM_READ:
		rd->mr_ptr = (uint64_t) (unsigned long)io_u->buf;
		break;
	default:
		log_err("fio: unknown rdma protocol - %d\n", rd->rdma_protocol);
		break;
	}

	return 0;
}

static enum fio_q_status fio_rdmaio_queue(struct thread_data *td,
					  struct io_u *io_u) //write
{
	struct rdmaio_data *rd = td->io_ops_data;

	fio_ro_check(td, io_u);

	if (rd->io_u_queued_nr == (int)td->o.iodepth)
		return FIO_Q_BUSY;

	rd->io_us_queued[rd->io_u_queued_nr] = io_u; //RPMA_WRITE,need count queue number(write operations)
	rd->io_u_queued_nr++;
    
	dprint_io_u(io_u, "fio_rdmaio_queue");

	/*here we get conn*/
	//client_connect(peer, addr, port, NULL, &conn);

	/*src start point and size, right now is 0 and 1k*/
	switch(io_u->ddir){
		case DDIR_WRITE:
			rpma_write(rd->conn, rd->recv_mr, rd->dst_offset, rd->send_mr,0, KILOBYTE,RPMA_F_COMPLETION_ON_ERROR, NULL);
			break;
	}

	return FIO_Q_QUEUED;
}

static void fio_rdmaio_queued(struct thread_data *td, struct io_u **io_us,
			      unsigned int nr)
{
	struct rdmaio_data *rd = td->io_ops_data;
	struct timespec now;
	unsigned int i;

	if (!fio_fill_issue_time(td))
		return;

	fio_gettime(&now, NULL);

	for (i = 0; i < nr; i++) {
		struct io_u *io_u = io_us[i];

		/* queued -> flight */
		rd->io_us_flight[rd->io_u_flight_nr] = io_u;
		rd->io_u_flight_nr++;

		memcpy(&io_u->issue_time, &now, sizeof(now));
		io_u_queued(td, io_u);
	}
}

#define FLUSH_ID	(void *)0xF01D
static int fio_rdmaio_commit(struct thread_data *td) //flush
{
	struct rdmaio_data *rd = td->io_ops_data;
	struct io_u **io_us;
	int ret;

	if (!rd->io_us_queued)
		return 0;

	io_us = rd->io_us_queued;
	do {
		/* RDMA_WRITE or RDMA_READ */
		if (rd->is_client){
			// ret = fio_rdmaio_send(td, io_us, rd->io_u_queued_nr);
			rpma_flush(rd->conn, rd->recv_mr, rd->dst_offset, KILOBYTE,
			RPMA_FLUSH_TYPE_PERSISTENT, RPMA_F_COMPLETION_ALWAYS,
			FLUSH_ID);
			ret = 1;
		}			
		//else if (!rd->is_client)
			//ret = fio_rdmaio_recv(td, io_us, rd->io_u_queued_nr);
		else
			ret = 0;	/* must be a SYNC */

		if (ret > 0) {
			fio_rdmaio_queued(td, io_us, ret);
			io_u_mark_submit(td, ret);
			rd->io_u_queued_nr -= ret;
			io_us += ret;
			ret = 0;
		} else
			break;
	} while (rd->io_u_queued_nr);

	return ret;
}

static int fio_rdmaio_connect(struct thread_data *td)
{
	int ret;
	/* RPMA resources */
	struct rdmaio_data *rd = td->io_ops_data;
	/*
	 * lookup an ibv_context via the address and create a new peer using it
	 */
	ret = client_peer_via_address(rd->addr, &rd->peer);


	/* establish a new connection to a server listening at addr:port */
	ret = client_connect(rd->peer, rd->addr, rd->port, NULL, &rd->conn);


	/* send task request */

	/* In SEND/RECV test, it's a good practice to setup the iodepth of
	 * of the RECV side deeper than that of the SEND side to
	 * avoid RNR (receiver not ready) error. The
	 * SEND side may send so many unsolicited message before
	 * RECV side commits sufficient recv buffers into recv queue.
	 * This may lead to RNR error. Here, SEND side pauses for a while
	 * during which RECV side commits sufficient recv buffers.
	 */
	usleep(500000);

	return 0;
}


// static int fio_rdmaio_open_file(struct thread_data *td, struct fio_file *f)
// {
// 	struct rdmaio_data *rd = td->io_ops_data;
// 	int is_pmem;

// 	// rd->mr_ptr = pmem_map_file(f->file_name, 0 /* len */, 0 /* flags */,
// 	// 			0 /* mode */, &rd->mr_size, &is_pmem);
// 	rd->mr_ptr = pmem_map_file(/dev/dax0.0, 0 /* len */, 0 /* flags */,
// 				0 /* mode */, &rd->mr_size, &is_pmem);
// 	if (rd->mr_ptr == NULL)
// 		return -1;

// 	/* pmem is expected */
// 	if (!is_pmem) {
// 		(void) pmem_unmap(rd->mr_ptr, rd->mr_size);
// 		return -1;
// 	}
// 	return fio_rdmaio_connect(td, f);
// }

static int fio_rdmaio_close_file(struct thread_data *td, struct fio_file *f)
{

	struct rdmaio_data *rd = td->io_ops_data;

	/* delete the remote memory region's structure */
	(void) rpma_mr_remote_delete(&rd->recv_mr);

	/* deregister the memory region */
	(void) rpma_mr_dereg(&rd->send_mr);


	(void) common_disconnect_and_wait_for_conn_close(&rd->conn);

	/* delete the peer */
	(void) rpma_peer_delete(&rd->peer);

	
	pmem_unmap(rd->mr_ptr, rd->mr_size);
	rd->mr_ptr = NULL;
	

	return 0;
}

static int aton(struct thread_data *td, const char *host,
		     struct sockaddr_in *addr)
{
	if (inet_aton(host, &addr->sin_addr) != 1) {
		struct hostent *hent;

		hent = gethostbyname(host);
		if (!hent) {
			td_verror(td, errno, "gethostbyname");
			return 1;
		}

		memcpy(&addr->sin_addr, hent->h_addr, 4);
	}
	return 0;
}

static int check_set_rlimits(struct thread_data *td)
{
#ifdef CONFIG_RLIMIT_MEMLOCK
	struct rlimit rl;

	/* check RLIMIT_MEMLOCK */
	if (getrlimit(RLIMIT_MEMLOCK, &rl) != 0) {
		log_err("fio: getrlimit fail: %d(%s)\n",
			errno, strerror(errno));
		return 1;
	}

	/* soft limit */
	if ((rl.rlim_cur != RLIM_INFINITY)
	    && (rl.rlim_cur < td->orig_buffer_size)) {
		log_err("fio: soft RLIMIT_MEMLOCK is: %" PRId64 "\n",
			rl.rlim_cur);
		log_err("fio: total block size is:    %zd\n",
			td->orig_buffer_size);
		/* try to set larger RLIMIT_MEMLOCK */
		rl.rlim_cur = rl.rlim_max;
		if (setrlimit(RLIMIT_MEMLOCK, &rl) != 0) {
			log_err("fio: setrlimit fail: %d(%s)\n",
				errno, strerror(errno));
			log_err("fio: you may try enlarge MEMLOCK by root\n");
			log_err("# ulimit -l unlimited\n");
			return 1;
		}
	}
#endif

	return 0;
}

static int compat_options(struct thread_data *td)
{
	// The original RDMA engine had an ugly / seperator
	// on the filename for it's options. This function
	// retains backwards compatibility with it. Note we do not
	// support setting the bindname option is this legacy mode.

	struct rdmaio_options *o = td->eo;
	char *modep, *portp;
	char *filename = td->o.filename;

	if (!filename)
		return 0;

	portp = strchr(filename, '/');
	if (portp == NULL)
		return 0;

	*portp = '\0';
	portp++;

	o->port = strtol(portp, NULL, 10);
	if (!o->port || o->port > 65535)
		goto bad_host;

	modep = strchr(portp, '/');
	if (modep != NULL) {
		*modep = '\0';
		modep++;
	}

	if (modep) {
		if (!strncmp("rdma_write", modep, strlen(modep)) ||
		    !strncmp("RDMA_WRITE", modep, strlen(modep)))
			o->verb = FIO_RDMA_MEM_WRITE;
		else if (!strncmp("rdma_read", modep, strlen(modep)) ||
			 !strncmp("RDMA_READ", modep, strlen(modep)))
			o->verb = FIO_RDMA_MEM_READ;
		else
			goto bad_host;
	} else
		o->verb = FIO_RDMA_MEM_WRITE;


	return 0;

bad_host:
	log_err("fio: bad rdma host/port/protocol: %s\n", td->o.filename);
	return 1;
}

static int fio_rdmaio_init(struct thread_data *td)
{
	struct rdmaio_data *rd = td->io_ops_data;
	struct rdmaio_options *o = td->eo;
	int ret;

	if (td_rw(td)) {
		log_err("fio: rdma connections must be read OR write\n");
		return 1;
	}
	if (td_random(td)) {
		log_err("fio: RDMA network IO can't be random\n");
		return 1;
	}

	if (compat_options(td))
		return 1;

	if (!o->port) {
		log_err("fio: no port has been specified which is required "
			"for the rdma engine\n");
		return 1;
	}

	if (check_set_rlimits(td))
		return 1;

	rd->rdma_protocol = o->verb;

	// if ((rd->rdma_protocol == FIO_RDMA_MEM_WRITE) ||
	//     (rd->rdma_protocol == FIO_RDMA_MEM_READ)) {
	// 	rd->rmt_us =
	// 		malloc(FIO_RDMA_MAX_IO_DEPTH * sizeof(struct remote_u));
	// 	memset(rd->rmt_us, 0,
	// 		FIO_RDMA_MAX_IO_DEPTH * sizeof(struct remote_u));
	// 	rd->rmt_nr = 0;
	// }

	rd->io_us_queued = malloc(td->o.iodepth * sizeof(struct io_u *));
	memset(rd->io_us_queued, 0, td->o.iodepth * sizeof(struct io_u *));
	rd->io_u_queued_nr = 0;

	rd->io_us_flight = malloc(td->o.iodepth * sizeof(struct io_u *));
	memset(rd->io_us_flight, 0, td->o.iodepth * sizeof(struct io_u *));
	rd->io_u_flight_nr = 0;

	rd->io_us_completed = malloc(td->o.iodepth * sizeof(struct io_u *));
	memset(rd->io_us_completed, 0, td->o.iodepth * sizeof(struct io_u *));
	rd->io_u_completed_nr = 0;

	/* WRITE as the client */
	rd->is_client = 1;
	ret = fio_rdmaio_setup_connect(td, td->o.filename, o->port);

	return ret;
}

static void fio_rdmaio_cleanup(struct thread_data *td)
{
	struct rdmaio_data *rd = td->io_ops_data;

	if (rd)
		free(rd);
}

static int fio_rdmaio_setup(struct thread_data *td)
{
	struct rdmaio_data *rd;

	if (!td->files_index) {
		add_file(td, td->o.filename ?: "rdma", 0, 0);
		td->o.nr_files = td->o.nr_files ?: 1;
		td->o.open_files++;
	}

	if (!td->io_ops_data) {
		rd = malloc(sizeof(*rd));

		memset(rd, 0, sizeof(*rd));
		init_rand_seed(&rd->rand_state, (unsigned int) GOLDEN_RATIO_PRIME, 0);
		td->io_ops_data = rd;
	}

	return 0;
}

FIO_STATIC struct ioengine_ops ioengine = {
	.name			= "rdma",
	.version		= FIO_IOOPS_VERSION,
	.setup			= fio_rdmaio_setup,
	.init			= fio_rdmaio_init,
	.prep			= fio_rdmaio_prep,
	.queue			= fio_rdmaio_queue,
	.commit			= fio_rdmaio_commit,
	.cleanup		= fio_rdmaio_cleanup,
	.close_file		= fio_rdmaio_close_file,
	.flags			= FIO_SYNCIO|FIO_DISKLESSIO | FIO_UNIDIR | FIO_PIPEIO,
	.options		= options,
	.option_struct_size	= sizeof(struct rdmaio_options),
};

static void fio_init fio_rdmaio_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_rdmaio_unregister(void)
{
	unregister_ioengine(&ioengine);
}
