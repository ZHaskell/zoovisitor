#ifndef HS_ZK
#define HS_ZK

#include <HsFFI.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zookeeper/zookeeper.h>

typedef struct Stat stat_t;
typedef struct String_vector string_vector_t;
typedef struct ACL acl_t;
typedef struct ACL_vector acl_vector_t;

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

typedef struct hs_strings_completion_t {
  HsStablePtr mvar;
  HsInt cap;
  int rc;
  const string_vector_t* strings;
} hs_strings_completion_t;

typedef struct hs_strings_stat_completion_t {
  HsStablePtr mvar;
  HsInt cap;
  int rc;
  const string_vector_t* strings;
  const stat_t* stat;
} hs_strings_stat_completion_t;

typedef struct hs_acl_completion_t {
  HsStablePtr mvar;
  HsInt cap;
  int rc;
  const struct ACL_vector* acl;
  const struct Stat* stat;
} hs_acl_completion_t;

// ----------------------------------------------------------------------------

int hs_zoo_aget_acl(zhandle_t* zh, const char* path, HsStablePtr mvar,
                    HsInt cap, hs_acl_completion_t* completion);

int hs_zoo_acreate(zhandle_t* zh, const char* path, const char* value,
                   HsInt offset, HsInt valuelen, const struct ACL_vector* acl,
                   int mode, HsStablePtr mvar, HsInt cap,
                   hs_string_completion_t* string_completion);

int hs_zoo_aget(zhandle_t* zh, const char* path, int watch, HsStablePtr mvar,
                HsInt cap, hs_data_completion_t* data_completion);

int hs_zoo_awget(zhandle_t* zh, const char* path, HsStablePtr mvar_w,
                 HsStablePtr mvar_f, HsInt cap, hs_watcher_ctx_t* watcher_ctx,
                 hs_data_completion_t* data_completion);

int hs_zoo_aset(zhandle_t* zh, const char* path, const char* buffer,
                HsInt offset, HsInt buflen, int version, HsStablePtr mvar,
                HsInt cap, hs_stat_completion_t* stat_completion);

int hs_zoo_adelete(zhandle_t* zh, const char* path, int version,
                   HsStablePtr mvar, HsInt cap,
                   hs_void_completion_t* void_completion);

int hs_zoo_aexists(zhandle_t* zh, const char* path, int watch, HsStablePtr mvar,
                   HsInt cap, hs_stat_completion_t* stat_completion);

int hs_zoo_awexists(zhandle_t* zh, const char* path, HsStablePtr mvar_w,
                    HsStablePtr mvar_f, HsInt cap,
                    hs_watcher_ctx_t* watcher_ctx,
                    hs_stat_completion_t* stat_completion);

int hs_zoo_aget_children(zhandle_t* zh, const char* path, int watch,
                         HsStablePtr mvar, HsInt cap,
                         hs_strings_completion_t* strings_completion);

int hs_zoo_awget_children(zhandle_t* zh, const char* path, HsStablePtr mvar_w,
                          HsStablePtr mvar_f, HsInt cap,
                          hs_watcher_ctx_t* watcher_ctx,
                          hs_strings_completion_t* strings_completion);

int hs_zoo_aget_children2(zhandle_t* zh, const char* path, int watch,
                          HsStablePtr mvar, HsInt cap,
                          hs_strings_stat_completion_t* strings_stat);

int hs_zoo_awget_children2(zhandle_t* zh, const char* path, HsStablePtr mvar_w,
                           HsStablePtr mvar_f, HsInt cap,
                           hs_watcher_ctx_t* watcher_ctx,
                           hs_strings_stat_completion_t* strings_stat);

int hs_zoo_amulti(zhandle_t* zh, int count, const zoo_op_t* ops,
                  zoo_op_result_t* results, HsStablePtr mvar, HsInt cap,
                  hs_void_completion_t* void_completion);

void hs_zoo_create_op_init(zoo_op_t* op, const char* path, const char* value,
                           HsInt valoffset, HsInt valuelen,
                           const struct ACL_vector* acl, int flags,
                           char* path_buffer, int path_buffer_len);

void hs_zoo_set_op_init(zoo_op_t* op, const char* path, const char* value,
                        HsInt valoffset, HsInt valuelen, int version,
                        stat_t* stat);

// End define HS_ZK
#endif
