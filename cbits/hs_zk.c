#include "hs_zk.h"

// ----------------------------------------------------------------------------
// Callback Functions
// ----------------------------------------------------------------------------

void hs_zookeeper_watcher_fn(zhandle_t* zh, int type, int state,
                             const char* path, void* watcherCtx) {
  hs_watcher_ctx_t* watcher_ctx = (hs_watcher_ctx_t*)watcherCtx;
  watcher_ctx->zh = zh;
  watcher_ctx->type = type;
  watcher_ctx->state = state;
  watcher_ctx->path = path;
  hs_try_putmvar(watcher_ctx->cap, watcher_ctx->mvar);
  hs_thread_done();
}

/**
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
void hs_string_completion_fn(int rc, const char* value, const void* data) {
  hs_string_completion_t* string_completion = (hs_string_completion_t*)data;
  string_completion->rc = rc;
  if (!rc) {
    string_completion->value = strdup(value);
  }
  hs_try_putmvar(string_completion->cap, string_completion->mvar);
  hs_thread_done();
}

void hs_data_completion_fn(int rc, const char* value, int value_len,
                           const struct Stat* stat, const void* data) {
  hs_data_completion_t* data_completion = (hs_data_completion_t*)data;
  data_completion->rc = rc;
  if (!rc) {
    data_completion->value = strndup(value, value_len);
    data_completion->value_len = value_len;
    data_completion->stat = dup_stat(stat);
  }
  hs_try_putmvar(data_completion->cap, data_completion->mvar);
  hs_thread_done();
}

/**
 * \brief signature of a completion function that returns a Stat structure.
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
 * \param stat a pointer to the stat information for the node involved in
 *   this function. If a non zero error code is returned, the content of
 *   stat is undefined. The programmer is NOT responsible for freeing stat.
 * \param data the pointer that was passed by the caller when the function
 *   that this completion corresponds to was invoked. The programmer
 *   is responsible for any memory freeing associated with the data
 *   pointer.
 */
void hs_stat_completion_fn(int rc, const struct Stat* stat, const void* data) {
  hs_stat_completion_t* stat_completion = (hs_stat_completion_t*)data;
  stat_completion->rc = rc;
  if (rc == 0) {
    stat_completion->stat = dup_stat(stat);
  }
  hs_try_putmvar(stat_completion->cap, stat_completion->mvar);
  hs_thread_done();
}

// ----------------------------------------------------------------------------

zhandle_t* hs_zookeeper_init(HsStablePtr mvar, HsInt cap,
                             hs_watcher_ctx_t* watcher_ctx, const char* host,
                             int recv_timeout, const clientid_t* clientid,
                             int flags) {
  watcher_ctx->mvar = mvar;
  watcher_ctx->cap = cap;
  zhandle_t* zh = zookeeper_init(host, hs_zookeeper_watcher_fn, recv_timeout,
                                 clientid, watcher_ctx, flags);
  return zh;
}

int hs_zoo_acreate(HsStablePtr mvar, HsInt cap,
                   hs_string_completion_t* string_completion, zhandle_t* zh,
                   const char* path, const char* value, int offset,
                   int valuelen, const struct ACL_vector* acl, int mode) {
  string_completion->mvar = mvar;
  string_completion->cap = cap;
  return zoo_acreate(zh, path, value + offset, valuelen, acl, mode,
                     hs_string_completion_fn, string_completion);
}

int hs_zoo_aget(HsStablePtr mvar, HsInt cap,
                hs_data_completion_t* data_completion, zhandle_t* zh,
                const char* path, int watch) {
  data_completion->mvar = mvar;
  data_completion->cap = cap;
  return zoo_aget(zh, path, watch, hs_data_completion_fn, data_completion);
}

/**
 * \brief sets the data associated with a node.
 *
 * \param zh the zookeeper handle obtained by a call to \ref zookeeper_init
 * \param path the name of the node. Expressed as a file name with slashes
 * separating ancestors of the node.
 * \param buffer the buffer holding data to be written to the node.
 * \param buflen the number of bytes from buffer to write.
 * \param version the expected version of the node. The function will fail if
 * the actual version of the node does not match the expected version. If -1 is
 * used the version check will not take place. * completion: If null,
 * the function will execute synchronously. Otherwise, the function will return
 * immediately and invoke the completion routine when the request completes.
 * \param completion the routine to invoke when the request completes. The
 * completion will be triggered with one of the following codes passed in as the
 * rc argument: ZOK operation completed successfully ZNONODE the node does not
 * exist. ZNOAUTH the client does not have permission. ZBADVERSION expected
 * version does not match actual version. \param data the data that will be
 * passed to the completion routine when the function completes. \return ZOK on
 * success or one of the following errcodes on failure: ZBADARGUMENTS - invalid
 * input parameters ZINVALIDSTATE - zhandle state is either
 * ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE ZMARSHALLINGERROR - failed
 * to marshall a request; possibly, out of memory
 */
int hs_zoo_aset(HsStablePtr mvar, HsInt cap,
                hs_stat_completion_t* stat_completion, zhandle_t* zh,
                const char* path, const char* buffer, int offset, int buflen,
                int version) {
  stat_completion->mvar = mvar;
  stat_completion->cap = cap;
  return zoo_aset(zh, path, buffer + offset, buflen, version,
                  hs_stat_completion_fn, stat_completion);
}

// ----------------------------------------------------------------------------
