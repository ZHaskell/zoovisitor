#ifndef HS_ZK
#define HS_ZK

#include <HsFFI.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zookeeper/zookeeper.h>

typedef struct Stat stat_t;

const stat_t* dup_stat(const stat_t* old_stat) {
  stat_t* new_stat = (stat_t*)malloc(sizeof(stat_t));
  new_stat = memcpy(new_stat, old_stat, sizeof(stat_t));
  return new_stat;
}

typedef struct hs_watcher_ctx_t {
  HsStablePtr mvar;
  HsInt cap;
  zhandle_t* zh;
  int type;
  int state;
  const char* path;
} hs_watcher_ctx_t;

/**
 *  From zookeeper: string_completion_t
 *
 * \brief signature of a completion function that returns a list of strings.
 *
 * This method will be invoked at the end of a asynchronous call and also as
 * a result of connection loss or timeout.
 * \param rc the error code of the call. Connection loss/timeout triggers
 * the completion with one of the following error codes:
 * ZCONNECTIONLOSS -- lost connection to the server
 * ZOPERATIONTIMEOUT -- connection timed out
 * Data related events trigger the completion with error codes listed the
 * Exceptions section of the documentation of the function that initiated the
 * call. (Zero indicates call was successful.)
 * \param value the value of the string returned.
 * \param data the pointer that was passed by the caller when the function
 *   that this completion corresponds to was invoked. The programmer
 *   is responsible for any memory freeing associated with the data
 *   pointer.
 */
typedef struct hs_string_completion_t {
  HsStablePtr mvar;
  HsInt cap;
  int rc;
  const char* value;
} hs_string_completion_t;

typedef struct hs_data_completion_t {
  HsStablePtr mvar;
  HsInt cap;
  int rc;
  const char* value;
  int value_len;
  const struct Stat* stat;
} hs_data_completion_t;

typedef struct hs_stat_completion_t {
  HsStablePtr mvar;
  HsInt cap;
  int rc;
  const struct Stat* stat;
} hs_stat_completion_t;

// ----------------------------------------------------------------------------

zhandle_t* hs_zookeeper_init(HsStablePtr mvar, HsInt cap,
                             hs_watcher_ctx_t* watcher_ctx, const char* host,
                             int recv_timeout, const clientid_t* clientid,
                             int flags);

int hs_zoo_acreate(HsStablePtr mvar, HsInt cap,
                   hs_string_completion_t* string_completion, zhandle_t* zh,
                   const char* path, const char* value, int offset,
                   int valuelen, const struct ACL_vector* acl, int mode);

int hs_zoo_aget(HsStablePtr mvar, HsInt cap,
                hs_data_completion_t* data_completion, zhandle_t* zh,
                const char* path, int watch);

int hs_zoo_aset(HsStablePtr mvar, HsInt cap,
                hs_stat_completion_t* stat_completion, zhandle_t* zh,
                const char* path, const char* buffer, int offset, int buflen,
                int version);

// End define HS_ZK
#endif
