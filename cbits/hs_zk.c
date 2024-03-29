#include "hs_zk.h"

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

const stat_t* dup_stat(const stat_t* old_stat);
const string_vector_t* dup_string_vector(const string_vector_t* old_strings);
const acl_vector_t* dup_acl_vector(const acl_vector_t* old_acls);

// ----------------------------------------------------------------------------
// Callback Functions
// ----------------------------------------------------------------------------

void hs_zookeeper_node_watcher_fn(zhandle_t* zh, int type, int state,
                                  const char* path, void* watcherCtx) {
  if (type != ZOO_SESSION_EVENT) {
    hs_watcher_ctx_t* watcher_ctx = (hs_watcher_ctx_t*)watcherCtx;
    watcher_ctx->zh = zh;
    watcher_ctx->type = type;
    watcher_ctx->state = state;
    watcher_ctx->path = strdup(path);
    hs_try_putmvar(watcher_ctx->cap, watcher_ctx->mvar);
  } else {
    fprintf(stderr,
            "zoovisitor.hs_zookeeper_node_watcher_fn: ignore event type: %d\n",
            type);
  }
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
}

/**
 * \brief signature of a completion function that returns data.
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
 * \param value the value of the information returned by the asynchronous call.
 *   If a non zero error code is returned, the content of value is undefined.
 *   The programmer is NOT responsible for freeing value.
 * \param value_len the number of bytes in value.
 * \param stat a pointer to the stat information for the node involved in
 *   this function. If a non zero error code is returned, the content of
 *   stat is undefined. The programmer is NOT responsible for freeing stat.
 * \param data the pointer that was passed by the caller when the function
 *   that this completion corresponds to was invoked. The programmer
 *   is responsible for any memory freeing associated with the data
 *   pointer.
 */
void hs_data_completion_fn(int rc, const char* value, int value_len,
                           const struct Stat* stat, const void* data) {
  hs_data_completion_t* data_completion = (hs_data_completion_t*)data;
  data_completion->rc = rc;
  if (!rc) {
    data_completion->value_len = value_len;
    if (value_len >= 0) {
      data_completion->value = strndup(value, value_len);
    }
    data_completion->stat = dup_stat(stat);
  }
  hs_try_putmvar(data_completion->cap, data_completion->mvar);
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
  if (!rc) {
    stat_completion->stat = dup_stat(stat);
  }
  hs_try_putmvar(stat_completion->cap, stat_completion->mvar);
}

/**
 * \brief signature of a completion function for a call that returns void.
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
 * \param data the pointer that was passed by the caller when the function
 *   that this completion corresponds to was invoked. The programmer
 *   is responsible for any memory freeing associated with the data
 *   pointer.
 */
void hs_void_completion_fn(int rc, const void* data) {
  hs_void_completion_t* void_completion = (hs_void_completion_t*)data;
  void_completion->rc = rc;
  hs_try_putmvar(void_completion->cap, void_completion->mvar);
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
 * \param strings a pointer to the structure containng the list of strings of
 * the names of the children of a node. If a non zero error code is returned,
 *   the content of strings is undefined. The programmer is NOT responsible
 *   for freeing strings.
 * \param data the pointer that was passed by the caller when the function
 *   that this completion corresponds to was invoked. The programmer
 *   is responsible for any memory freeing associated with the data
 *   pointer.
 */
void hs_strings_completion_fn(int rc, const string_vector_t* strings,
                              const void* data) {
  hs_strings_completion_t* strings_completion = (hs_strings_completion_t*)data;
  strings_completion->rc = rc;
  if (!rc) {
    strings_completion->strings = dup_string_vector(strings);
  }
  hs_try_putmvar(strings_completion->cap, strings_completion->mvar);
}

/**
 * \brief signature of a completion function that returns a list of strings and
 * stat.
 * .
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
 * \param strings a pointer to the structure containng the list of strings of
 * the names of the children of a node. If a non zero error code is returned,
 *   the content of strings is undefined. The programmer is NOT responsible
 *   for freeing strings.
 * \param stat a pointer to the stat information for the node involved in
 *   this function. If a non zero error code is returned, the content of
 *   stat is undefined. The programmer is NOT responsible for freeing stat.
 * \param data the pointer that was passed by the caller when the function
 *   that this completion corresponds to was invoked. The programmer
 *   is responsible for any memory freeing associated with the data
 *   pointer.
 */
void hs_strings_stat_completion_fn(int rc, const string_vector_t* strings,
                                   const struct Stat* stat, const void* data) {
  hs_strings_stat_completion_t* strings_stat =
      (hs_strings_stat_completion_t*)data;
  strings_stat->rc = rc;
  if (!rc) {
    strings_stat->strings = dup_string_vector(strings);
    strings_stat->stat = dup_stat(stat);
  }
  hs_try_putmvar(strings_stat->cap, strings_stat->mvar);
}

/**
 * \brief signature of a completion function that returns an ACL.
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
 * \param acl a pointer to the structure containng the ACL of a node. If a non
 *   zero error code is returned, the content of strings is undefined. The
 *   programmer is NOT responsible for freeing acl.
 * \param stat a pointer to the stat information for the node involved in
 *   this function. If a non zero error code is returned, the content of
 *   stat is undefined. The programmer is NOT responsible for freeing stat.
 * \param data the pointer that was passed by the caller when the function
 *   that this completion corresponds to was invoked. The programmer
 *   is responsible for any memory freeing associated with the data
 *   pointer.
 */
void hs_acl_completion_fn(int rc, struct ACL_vector* acl, struct Stat* stat,
                          const void* data) {
  hs_acl_completion_t* acl_completion = (hs_acl_completion_t*)data;
  acl_completion->rc = rc;
  if (!rc) {
    acl_completion->acl = dup_acl_vector(acl);
    acl_completion->stat = dup_stat(stat);
  }
  hs_try_putmvar(acl_completion->cap, acl_completion->mvar);
}

// ----------------------------------------------------------------------------
// Operations
// ----------------------------------------------------------------------------

int hs_zoo_acreate(zhandle_t* zh, const char* path, const char* value,
                   HsInt offset, HsInt valuelen, const struct ACL_vector* acl,
                   int mode, HsStablePtr mvar, HsInt cap,
                   hs_string_completion_t* string_completion) {
  string_completion->mvar = mvar;
  string_completion->cap = cap;
  return zoo_acreate(zh, path, value + offset, valuelen, acl, mode,
                     hs_string_completion_fn, string_completion);
}

int hs_zoo_aget(zhandle_t* zh, const char* path, int watch, HsStablePtr mvar,
                HsInt cap, hs_data_completion_t* data_completion) {
  data_completion->mvar = mvar;
  data_completion->cap = cap;
  return zoo_aget(zh, path, watch, hs_data_completion_fn, data_completion);
}

int hs_zoo_awget(zhandle_t* zh, const char* path, HsStablePtr mvar_w,
                 HsStablePtr mvar_f, HsInt cap, hs_watcher_ctx_t* watcher_ctx,
                 hs_data_completion_t* data_completion) {
  watcher_ctx->mvar = mvar_w;
  watcher_ctx->cap = cap;
  data_completion->mvar = mvar_f;
  data_completion->cap = cap;
  return zoo_awget(zh, path, hs_zookeeper_node_watcher_fn, watcher_ctx,
                   hs_data_completion_fn, data_completion);
}

int hs_zoo_aset(zhandle_t* zh, const char* path, const char* buffer,
                HsInt offset, HsInt buflen, int version, HsStablePtr mvar,
                HsInt cap, hs_stat_completion_t* stat_completion) {
  stat_completion->mvar = mvar;
  stat_completion->cap = cap;
  return zoo_aset(zh, path, buffer + offset, buflen, version,
                  hs_stat_completion_fn, stat_completion);
}

int hs_zoo_adelete(zhandle_t* zh, const char* path, int version,
                   HsStablePtr mvar, HsInt cap,
                   hs_void_completion_t* void_completion) {
  void_completion->mvar = mvar;
  void_completion->cap = cap;
  return zoo_adelete(zh, path, version, hs_void_completion_fn, void_completion);
}

int hs_zoo_aexists(zhandle_t* zh, const char* path, int watch, HsStablePtr mvar,
                   HsInt cap, hs_stat_completion_t* stat_completion) {
  stat_completion->mvar = mvar;
  stat_completion->cap = cap;
  return zoo_aexists(zh, path, watch, hs_stat_completion_fn, stat_completion);
}

int hs_zoo_awexists(zhandle_t* zh, const char* path, HsStablePtr mvar_w,
                    HsStablePtr mvar_f, HsInt cap,
                    hs_watcher_ctx_t* watcher_ctx,
                    hs_stat_completion_t* stat_completion) {
  watcher_ctx->mvar = mvar_w;
  watcher_ctx->cap = cap;
  stat_completion->mvar = mvar_f;
  stat_completion->cap = cap;
  return zoo_awexists(zh, path, hs_zookeeper_node_watcher_fn, watcher_ctx,
                      hs_stat_completion_fn, stat_completion);
}

int hs_zoo_aget_children(zhandle_t* zh, const char* path, int watch,
                         HsStablePtr mvar, HsInt cap,
                         hs_strings_completion_t* strings_completion) {
  strings_completion->mvar = mvar;
  strings_completion->cap = cap;
  return zoo_aget_children(zh, path, watch, hs_strings_completion_fn,
                           strings_completion);
}

int hs_zoo_awget_children(zhandle_t* zh, const char* path, HsStablePtr mvar_w,
                          HsStablePtr mvar_f, HsInt cap,
                          hs_watcher_ctx_t* watcher_ctx,
                          hs_strings_completion_t* strings_completion) {
  watcher_ctx->mvar = mvar_w;
  watcher_ctx->cap = cap;
  strings_completion->mvar = mvar_f;
  strings_completion->cap = cap;
  return zoo_awget_children(zh, path, hs_zookeeper_node_watcher_fn, watcher_ctx,
                            hs_strings_completion_fn, strings_completion);
}

int hs_zoo_aget_children2(zhandle_t* zh, const char* path, int watch,
                          HsStablePtr mvar, HsInt cap,
                          hs_strings_stat_completion_t* strings_stat) {
  strings_stat->mvar = mvar;
  strings_stat->cap = cap;
  return zoo_aget_children2(zh, path, watch, hs_strings_stat_completion_fn,
                            strings_stat);
}

int hs_zoo_awget_children2(zhandle_t* zh, const char* path, HsStablePtr mvar_w,
                           HsStablePtr mvar_f, HsInt cap,
                           hs_watcher_ctx_t* watcher_ctx,
                           hs_strings_stat_completion_t* strings_stat) {
  watcher_ctx->mvar = mvar_w;
  watcher_ctx->cap = cap;
  strings_stat->mvar = mvar_f;
  strings_stat->cap = cap;
  return zoo_awget_children2(zh, path, hs_zookeeper_node_watcher_fn,
                             watcher_ctx, hs_strings_stat_completion_fn,
                             strings_stat);
}

int hs_zoo_aget_acl(zhandle_t* zh, const char* path, HsStablePtr mvar,
                    HsInt cap, hs_acl_completion_t* completion) {
  completion->mvar = mvar;
  completion->cap = cap;
  return zoo_aget_acl(zh, path, hs_acl_completion_fn, completion);
}

int hs_zoo_amulti(zhandle_t* zh, int count, const zoo_op_t* ops,
                  zoo_op_result_t* results, HsStablePtr mvar, HsInt cap,
                  hs_void_completion_t* void_completion) {
  void_completion->mvar = mvar;
  void_completion->cap = cap;
  return zoo_amulti(zh, count, ops, results, hs_void_completion_fn,
                    void_completion);
}

void hs_zoo_create_op_init(zoo_op_t* op, const char* path, const char* value,
                           HsInt valoffset, HsInt valuelen,
                           const struct ACL_vector* acl, int flags,
                           char* path_buffer, int path_buffer_len) {
  zoo_create_op_init(op, path, value + valoffset, valuelen, acl, flags,
                     path_buffer, path_buffer_len);
}

void hs_zoo_set_op_init(zoo_op_t* op, const char* path, const char* value,
                        HsInt valoffset, HsInt valuelen, int version,
                        stat_t* stat) {
  zoo_set_op_init(op, path, value + valoffset, valuelen, version, stat);
}

// ----------------------------------------------------------------------------

void hs_zoo_set_std_log_stream(int s) {
  if (s == 1) {
    zoo_set_log_stream(stdout);
  } else if (s == 2) {
    zoo_set_log_stream(stderr);
  } else {
    fprintf(stderr, "hs_zoo_set_std_log_stream: invalid %d\n", s);
  }
}

// ----------------------------------------------------------------------------
// Helpers

const stat_t* dup_stat(const stat_t* old_stat) {
  stat_t* new_stat = (stat_t*)malloc(sizeof(stat_t));
  new_stat = memcpy(new_stat, old_stat, sizeof(stat_t));
  return new_stat;
}

const string_vector_t* dup_string_vector(const string_vector_t* old_strings) {
  int32_t count = old_strings->count;
  if (count < 0) {
    fprintf(stderr, "dup_string_vector error: count %d\n", count);
    return NULL;
  }
  string_vector_t* new_strings =
      (string_vector_t*)malloc(sizeof(string_vector_t));
  char** vals = malloc(count * sizeof(char*));
  for (int32_t i = 0; i < count; ++i) {
    vals[i] = strdup(old_strings->data[i]);
  }
  new_strings->count = count;
  new_strings->data = vals;
  return new_strings;
}

void free_string_vector(string_vector_t* strings) {
  if (strings == NULL || strings->data == NULL) {
    return;
  }

  for (int32_t i = 0; i < strings->count; ++i) {
    free(strings->data[i]);
  }
  free(strings->data);
  strings->data = NULL;
  free(strings);
}

const acl_vector_t* dup_acl_vector(const acl_vector_t* old_acls) {
  int32_t count = old_acls->count;
  if (count < 0) {
    fprintf(stderr, "dup_acl_vector error: count %d\n", count);
    return NULL;
  }
  acl_t* data = (acl_t*)malloc(count * sizeof(acl_t));
  acl_t* old_data = old_acls->data;
  for (int32_t i = 0; i < count; ++i) {
    data[i].perms = old_data[i].perms;
    data[i].id.scheme = strdup(old_data[i].id.scheme);
    data[i].id.id = strdup(old_data[i].id.id);
  }

  acl_vector_t* new_acls = (acl_vector_t*)malloc(sizeof(acl_vector_t));
  new_acls->count = count;
  new_acls->data = data;
  return new_acls;
}

void free_acl_vector(acl_vector_t* acls) {
  if (acls == NULL || acls->data == NULL) {
    return;
  }
  for (int32_t i = 0; i < acls->count; ++i) {
    free(acls->data[i].id.id);
    free(acls->data[i].id.scheme);
  }
  free(acls->data);
  free(acls);
}
