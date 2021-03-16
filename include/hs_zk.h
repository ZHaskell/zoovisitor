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

typedef struct hs_void_completion_t {
  HsStablePtr mvar;
  HsInt cap;
  int rc;
} hs_void_completion_t;

// ----------------------------------------------------------------------------

zhandle_t* hs_zookeeper_init(HsStablePtr mvar, HsInt cap,
                             hs_watcher_ctx_t* watcher_ctx, const char* host,
                             int recv_timeout, const clientid_t* clientid,
                             int flags);

int hs_zoo_acreate(zhandle_t* zh, const char* path, const char* value,
                   int offset, int valuelen, const struct ACL_vector* acl,
                   int mode, HsStablePtr mvar, HsInt cap,
                   hs_string_completion_t* string_completion);

int hs_zoo_aget(zhandle_t* zh, const char* path, int watch, HsStablePtr mvar,
                HsInt cap, hs_data_completion_t* data_completion);

int hs_zoo_aset(zhandle_t* zh, const char* path, const char* buffer, int offset,
                int buflen, int version, HsStablePtr mvar, HsInt cap,
                hs_stat_completion_t* stat_completion);

int hs_zoo_adelete(zhandle_t* zh, const char* path, int version,
                   HsStablePtr mvar, HsInt cap,
                   hs_void_completion_t* void_completion);

int hs_zoo_aexists(zhandle_t* zh, const char* path, int watch, HsStablePtr mvar,
                   HsInt cap, hs_stat_completion_t* stat_completion);

int hs_zoo_awexists(zhandle_t* zh, const char* path, HsStablePtr mvar_w,
                    HsStablePtr mvar_f, HsInt cap,
                    hs_watcher_ctx_t* watcher_ctx,
                    hs_stat_completion_t* stat_completion);

// End define HS_ZK
#endif
