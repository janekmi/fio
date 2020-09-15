#ifndef LIBRPMA_ENGINE_H
#define LIBRPMA_ENGINE_H

#include <librpma.h>

#define rpma_td_verror(td, err, func) \
      td_vmsg((td), (err), rpma_err_2str(err), (func))

#endif // LIBRPMA_ENGINE_H
