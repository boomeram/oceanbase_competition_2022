/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_SESSION_OB_SQL_SESSION_INFO_
#define OCEANBASE_SQL_SESSION_OB_SQL_SESSION_INFO_

#include "io/easy_io_struct.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "common/ob_range.h"
#include "lib/net/ob_addr.h"
#include "share/ob_define.h"
#include "share/ob_ddl_common.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_name_def.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/list/ob_list.h"
#include "lib/allocator/page_arena.h"
#include "lib/objectpool/ob_pool.h"
#include "lib/time/ob_cur_time.h"
#include "lib/lock/ob_recursive_mutex.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/mysqlclient/ob_server_connection_pool.h"
#include "lib/stat/ob_session_stat.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "sql/ob_sql_config_provider.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/session/ob_session_val_map.h"
#include "sql/session/ob_basic_session_info.h"
#include "sql/monitor/ob_exec_stat.h"
#include "sql/monitor/ob_security_audit.h"
#include "sql/monitor/ob_security_audit_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_context.h"
#include "sql/monitor/full_link_trace/ob_flt_extra_info.h"

namespace oceanbase
{
namespace observer
{
class ObQueryDriver;
class ObSqlEndTransCb;
}
namespace pl
{
class ObPLPackageState;
class ObPL;
struct ObPLExecRecursionCtx;
struct ObPLSqlCodeInfo;
class ObPLContext;
class ObDbmsCursorInfo;
} // namespace pl

namespace obmysql
{
class ObMySQLRequestManager;
} // namespace obmysql
namespace share
{
struct ObSequenceValue;
}
using common::ObPsStmtId;
namespace sql
{
class ObResultSet;
class ObPlanCache;
class ObPsCache;
class ObPlanCacheManager;
class ObPsSessionInfo;
class ObPsStmtInfo;
class ObStmt;
class ObSQLSessionInfo;

class SessionInfoKey
{
public:
  SessionInfoKey() : sessid_(0), proxy_sessid_(0) { }
  SessionInfoKey(uint32_t sessid, uint64_t proxy_sessid = 0) : sessid_(sessid), proxy_sessid_(proxy_sessid) {}
  uint64_t hash() const
  { uint64_t hash_value = 0;
    hash_value = common::murmurhash(&sessid_, sizeof(sessid_), hash_value);
    return hash_value;
  };
  int compare(const SessionInfoKey & r)
  {
    int cmp = 0;
    if (sessid_ < r.sessid_) {
      cmp = -1;
    } else if (sessid_ > r.sessid_) {
      cmp = 1;
    } else {
      cmp = 0;
    }
    return cmp;
  }
public:
  uint32_t sessid_;
  uint64_t proxy_sessid_; //?????????compare, ??????????????????ObCTASCleanUp??????
};

struct ObContextUnit
{
  inline void free(common::ObIAllocator &alloc) {
    alloc.free(value_.ptr());
    alloc.free(attribute_.ptr());
  }
  int deep_copy(const common::ObString &attribute,
                const common::ObString &value,
                common::ObIAllocator &alloc) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ob_write_string(alloc, attribute, attribute_))) {
      SQL_ENG_LOG(WARN, "failed to copy attribute", K(ret));
    } else if (OB_FAIL(ob_write_string(alloc, value, value_))) {
      alloc.free(attribute_.ptr());
      SQL_ENG_LOG(WARN, "failed to copy value", K(ret));
    }
    return ret;
  }
  ObString attribute_;
  ObString value_;
  TO_STRING_KV(K(attribute_), K(value_));
  OB_UNIS_VERSION(1);
};

struct ObSessionEncryptInfo
{
  // ??????sql??????????????????????????????????????????????????????.
  // ?????????session????????????????????????.?????????????????????????????????.
  bool is_encrypt_;
  uint64_t last_modify_time_;
  ObSessionEncryptInfo() : is_encrypt_(false), last_modify_time_(0) {}
  inline void reset()
  {
    is_encrypt_ = false;
    last_modify_time_ = 0;
  }
};

struct ObSessionStat final
{
  ObSessionStat() : total_logical_read_(0), total_physical_read_(0), total_logical_write_(0),
                    total_lock_count_(0), total_cpu_time_us_(0), total_exec_time_us_(0),
                    total_alive_time_us_(0)
      {}
  void reset() { new (this) ObSessionStat(); }

  TO_STRING_KV(K_(total_logical_read), K_(total_physical_read), K_(total_logical_write),
      K_(total_lock_count), K_(total_cpu_time_us), K_(total_exec_time_us), K_(total_alive_time_us));

  uint64_t total_logical_read_;
  uint64_t total_physical_read_;
  uint64_t total_logical_write_;
  uint64_t total_lock_count_;
  uint64_t total_cpu_time_us_;
  uint64_t total_exec_time_us_;
  uint64_t total_alive_time_us_;
};

//???????????????????????????Session????????????????????????
class ObTenantCachedSchemaGuardInfo
{
public:
  ObTenantCachedSchemaGuardInfo()
    : schema_guard_(share::schema::ObSchemaMgrItem::MOD_CACHED_GUARD)
  { reset(); }
  ~ObTenantCachedSchemaGuardInfo() { reset(); }
  void reset();

  // ???????????????????????????schema service?????????version????????????
  // ????????????????????????????????????????????????refresh_tenant_schema_version??????
  share::schema::ObSchemaGetterGuard &get_schema_guard() { return schema_guard_; }
  int refresh_tenant_schema_guard(const uint64_t tenant_id);

  // ?????????schema_mgr???ref??????????????????????????????10s???schema_guard?????????revert?????????
  // 1. ??????session ?????????????????????????????????????????????????????? ???????????????
  // 2. ??????session ??????????????????????????????session_mgr???????????????????????????
  void try_revert_schema_guard();
private:
  share::schema::ObSchemaGetterGuard schema_guard_;
  // ??????????????????schema guard????????? ???????????? ??????????????????revert guard ref??????????????????schema mgr??????????????????
  int64_t ref_ts_;
  uint64_t tenant_id_;
  int64_t schema_version_;
};

enum SessionSyncInfoType {
  //SESSION_SYNC_SYS_VAR,   // for system variables
  //SESSION_SYNC_USER_VAR,  // for user variables
  SESSION_SYNC_APPLICATION_INFO, // for application info
  SESSION_SYNC_APPLICATION_CONTEXT, // for app ctx
  SESSION_SYNC_CLIENT_ID, // for client identifier
  SESSION_SYNC_CONTROL_INFO, // for full trace link control info
  SESSION_SYNC_MAX_TYPE,
};

class ObSessInfoEncoder {
public:
  ObSessInfoEncoder() : is_changed_(false) {}
  virtual ~ObSessInfoEncoder() {}
  virtual int serialize(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos) = 0;
  virtual int deserialize(ObSQLSessionInfo &sess, const char *buf, const int64_t length, int64_t &pos) = 0;
  virtual int64_t get_serialize_size(ObSQLSessionInfo& sess) const = 0;
  bool is_changed_;
};

//class ObSysVarEncoder : public ObSessInfoEncoder {
//public:
//  ObSysVarEncoder():ObSessInfoEncoder() {}
//  ~ObSysVarEncoder() {}
//  int serialize(ObBasicSessionInfo &sess, char *buf, const int64_t length, int64_t &pos);
//  int deserialize(ObBasicSessionInfo &sess, const char *buf, const int64_t length, int64_t &pos);
//  int64_t get_serialize_size(ObBasicSessionInfo& sess) const;
//  // implements of other variables need to monitor
//};

class ObAppInfoEncoder : public ObSessInfoEncoder {
public:
  ObAppInfoEncoder():ObSessInfoEncoder() {}
  ~ObAppInfoEncoder() {}
  int serialize(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos);
  int deserialize(ObSQLSessionInfo &sess, const char *buf, const int64_t length, int64_t &pos);
  int64_t get_serialize_size(ObSQLSessionInfo& sess) const;
  int set_client_info(ObSQLSessionInfo* sess, const ObString &client_info);
  int set_module_name(ObSQLSessionInfo* sess, const ObString &mod);
  int set_action_name(ObSQLSessionInfo* sess, const ObString &act);
};

//class ObUserVarEncoder : public ObSessInfoEncoder {
//public:
//  ObUserVarEncoder():ObSessInfoEncoder() {}
//  ~ObUserVarEncoder() {}
//  int serialize(ObBasicSessionInfo &sess, char *buf, const int64_t length, int64_t &pos);
//  int deserialize(ObBasicSessionInfo &sess, const char *buf, const int64_t length, int64_t &pos);
//  int64_t get_serialize_size(ObBasicSessionInfo& sess) const;
//  // implements of other variables need to monitor
//};

class ObAppCtxInfoEncoder : public ObSessInfoEncoder {
public:
  ObAppCtxInfoEncoder() : ObSessInfoEncoder() {}
  virtual ~ObAppCtxInfoEncoder() {}
  virtual int serialize(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos) override;
  virtual int deserialize(ObSQLSessionInfo &sess, const char *buf, const int64_t length, int64_t &pos) override;
  virtual int64_t get_serialize_size(ObSQLSessionInfo& sess) const override;
};
class ObClientIdInfoEncoder : public ObSessInfoEncoder {
public:
  ObClientIdInfoEncoder() : ObSessInfoEncoder() {}
  virtual ~ObClientIdInfoEncoder() {}
  virtual int serialize(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos) override;
  virtual int deserialize(ObSQLSessionInfo &sess, const char *buf, const int64_t length, int64_t &pos) override;
  virtual int64_t get_serialize_size(ObSQLSessionInfo &sess) const override;
};

class ObControlInfoEncoder : public ObSessInfoEncoder {
public:
  ObControlInfoEncoder() : ObSessInfoEncoder() {}
  virtual ~ObControlInfoEncoder() {}
  virtual int serialize(ObSQLSessionInfo &sess, char *buf, const int64_t length, int64_t &pos) override;
  virtual int deserialize(ObSQLSessionInfo &sess, const char *buf, const int64_t length, int64_t &pos) override;
  virtual int64_t get_serialize_size(ObSQLSessionInfo &sess) const override;
  static const int16_t CONINFO_BY_SESS = 0xC078;
};

typedef common::hash::ObHashMap<uint64_t, pl::ObPLPackageState *,
                                common::hash::NoPthreadDefendMode> ObPackageStateMap;
typedef common::hash::ObHashMap<uint64_t, share::ObSequenceValue,
                                common::hash::NoPthreadDefendMode> ObSequenceCurrvalMap;
typedef common::hash::ObHashMap<common::ObString,
                                ObContextUnit *,
                                common::hash::NoPthreadDefendMode,
                                common::hash::hash_func<common::ObString>,
                                common::hash::equal_to<common::ObString>,
                                common::hash::SimpleAllocer<typename common::hash::HashMapTypes<common::ObString, ObContextUnit *>::AllocType>,
                                common::hash::NormalPointer,
                                oceanbase::common::ObMalloc,
                                2> ObInnerContextHashMap;
struct ObInnerContextMap {
  ObInnerContextMap(common::ObIAllocator &alloc) : context_name_(),
                    context_map_(nullptr), alloc_(alloc) {}
  void destroy()
  {
    if (OB_NOT_NULL(context_map_)) {
      for (auto it = context_map_->begin(); it != context_map_->end(); ++it) {
        it->second->free(alloc_);
        alloc_.free(it->second);
      }
    }
    destroy_map();
  }
  void destroy_map()
  {
    if (OB_NOT_NULL(context_map_)) {
      context_map_->destroy();
      alloc_.free(context_map_);
    }
    if (OB_NOT_NULL(context_name_.ptr())) {
      alloc_.free(context_name_.ptr());
    }
  }
  int init()
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(context_map_ = static_cast<ObInnerContextHashMap *>
                                  (alloc_.alloc(sizeof(ObInnerContextHashMap))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to alloc mem for hash map", K(ret));
    } else {
      new (context_map_) ObInnerContextHashMap ();
      if (OB_FAIL(context_map_->create(hash::cal_next_prime(32),
                                      ObModIds::OB_HASH_BUCKET,
                                      ObModIds::OB_HASH_NODE))) {
        SQL_ENG_LOG(WARN, "failed to init hash map", K(ret));
      }
    }
    return ret;
  }
  ObString context_name_;
  ObInnerContextHashMap *context_map_;
  common::ObIAllocator &alloc_;
  OB_UNIS_VERSION(1);
};
typedef common::hash::ObHashMap<common::ObString, ObInnerContextMap *,
                                common::hash::NoPthreadDefendMode,
                                common::hash::hash_func<common::ObString>,
                                common::hash::equal_to<common::ObString>,
                                common::hash::SimpleAllocer<typename common::hash::HashMapTypes<common::ObString, ObInnerContextMap *>::AllocType>,
                                common::hash::NormalPointer,
                                oceanbase::common::ObMalloc,
                                2> ObContextsMap;
typedef common::LinkHashNode<SessionInfoKey> SessionInfoHashNode;
typedef common::LinkHashValue<SessionInfoKey> SessionInfoHashValue;
// ObBasicSessionInfo????????????????????????????????????????????????????????????SQL task???????????????????????????????????????
// ObPsInfoMgr??????prepared statement????????????
// ObSQLSessionInfo????????????????????????????????????????????????SQL??????????????????**?????????**??????????????????
class ObSQLSessionInfo: public common::ObVersionProvider, public ObBasicSessionInfo, public SessionInfoHashValue
{
  OB_UNIS_VERSION(1);
public:
  friend class LinkExecCtxGuard;
  // notice!!! register exec ctx to session for later access
  // used for temp session, such as session for rpc processor, px worker, das processor, etc
  // not used for main session
  class ExecCtxSessionRegister
  {
  public:
    ExecCtxSessionRegister(ObSQLSessionInfo &session, ObExecContext &exec_ctx)
    {
      session.set_cur_exec_ctx(&exec_ctx);
    }
  };
  friend class ExecCtxSessionRegister;
  enum SessionType
  {
    INVALID_TYPE,
    USER_SESSION,
    INNER_SESSION
  };
  // for switch stmt.
  class StmtSavedValue : public ObBasicSessionInfo::StmtSavedValue
  {
  public:
    StmtSavedValue()
      : ObBasicSessionInfo::StmtSavedValue()
    {
      reset();
    }
    inline void reset()
    {
      ObBasicSessionInfo::StmtSavedValue::reset();
      audit_record_.reset();
      session_type_ = INVALID_TYPE;
      inner_flag_ = false;
      is_ignore_stmt_ = false;
    }
  public:
    ObAuditRecordData audit_record_;
    SessionType session_type_;
    bool inner_flag_;
    bool is_ignore_stmt_;
  };

  class CursorCache {
    public:
      CursorCache() : mem_context_(nullptr), next_cursor_id_(1LL << 31), pl_cursor_map_() {}
      virtual ~CursorCache() { NULL != mem_context_ ? DESTROY_CONTEXT(mem_context_) : (void)(NULL); }
      int init(uint64_t tenant_id)
      {
        int ret = OB_SUCCESS;
        if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context_,
            lib::ContextParam().set_mem_attr(tenant_id, ObModIds::OB_PL)))) {
          SQL_ENG_LOG(WARN, "create memory entity failed");
        } else if (OB_ISNULL(mem_context_)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "null memory entity returned");
        } else if (!pl_cursor_map_.created() &&
                   OB_FAIL(pl_cursor_map_.create(common::hash::cal_next_prime(32),
                                                 ObModIds::OB_HASH_BUCKET, ObModIds::OB_HASH_NODE))) {
          SQL_ENG_LOG(WARN, "create sequence current value map failed", K(ret));
        } else { /*do nothing*/ }
        return ret;
      }
      int close_all(sql::ObSQLSessionInfo &session)
      {
        int ret = OB_SUCCESS;
        common::ObSEArray<uint64_t, 32> cursor_ids;
        ObSessionStatEstGuard guard(session.get_effective_tenant_id(), session.get_sessid());
        for (CursorMap::iterator iter = pl_cursor_map_.begin();  //ignore ret
            iter != pl_cursor_map_.end();
            ++iter) {
          pl::ObPLCursorInfo *cursor_info = iter->second;
          if (OB_ISNULL(cursor_info)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "cursor info is NULL", K(cursor_info), K(ret));
          } else {
            cursor_ids.push_back(cursor_info->get_id());
          }
        }
        for (int64_t i = 0; i < cursor_ids.count(); i++) {
          uint64_t cursor_id = cursor_ids.at(i);
          if (OB_FAIL(session.close_cursor(cursor_id))) {
            SQL_ENG_LOG(WARN, "failed to close cursor",
                        K(cursor_id), K(session.get_sessid()), K(ret));
          } else {
            SQL_ENG_LOG(INFO, "NOTICE: cursor is closed unexpectedly",
                        K(cursor_id), K(session.get_sessid()), K(ret));
          }
        }
        return ret;
      }
      inline bool is_inited() const { return NULL != mem_context_; }
      void reset()
      {
        pl_cursor_map_.reuse();
        next_cursor_id_ = 1LL << 31;
        if (NULL != mem_context_) {
          DESTROY_CONTEXT(mem_context_);
          mem_context_ = NULL;
        }
      }
      inline int64_t gen_cursor_id() { return __sync_add_and_fetch(&next_cursor_id_, 1); }
    public:
      lib::MemoryContext mem_context_;
      int64_t next_cursor_id_;
      typedef common::hash::ObHashMap<int64_t, pl::ObPLCursorInfo*,
                                      common::hash::NoPthreadDefendMode> CursorMap;
      CursorMap pl_cursor_map_;
  };

  class ObCachedTenantConfigInfo
  {
  public:
    ObCachedTenantConfigInfo(ObSQLSessionInfo *session) :
                                 is_external_consistent_(false),
                                 enable_batched_multi_statement_(false),
                                 enable_sql_extension_(false),
                                 saved_tenant_info_(0),
                                 enable_bloom_filter_(true),
                                 at_type_(ObAuditTrailType::NONE),
                                 sort_area_size_(128*1024*1024),
                                 last_check_ec_ts_(0),
                                 session_(session)
    {
    }
    ~ObCachedTenantConfigInfo() {}
    void refresh();
    bool get_is_external_consistent() const { return is_external_consistent_; }
    bool get_enable_batched_multi_statement() const { return enable_batched_multi_statement_; }
    bool get_enable_bloom_filter() const { return enable_bloom_filter_; }
    bool get_enable_sql_extension() const { return enable_sql_extension_; }
    ObAuditTrailType get_at_type() const { return at_type_; }
    int64_t get_sort_area_size() const { return ATOMIC_LOAD(&sort_area_size_); }
  private:
    //???????????????????????????session ???????????????????????????????????????
    bool is_external_consistent_;
    bool enable_batched_multi_statement_;
    bool enable_sql_extension_;
    uint64_t saved_tenant_info_;
    bool enable_bloom_filter_;
    ObAuditTrailType at_type_;
    int64_t sort_area_size_;
    int64_t last_check_ec_ts_;
    ObSQLSessionInfo *session_;
  };

  class ApplicationInfo {
    OB_UNIS_VERSION(1);
  public:
    common::ObString module_name_;  // name as set by the dbms_application_info(set_module)
    common::ObString action_name_;  // action as set by the dbms_application_info(set_action)
    common::ObString client_info_;  // for addition info
    void reset() {
      module_name_.reset();
      action_name_.reset();
      client_info_.reset();
    }
    TO_STRING_KV(K_(module_name), K_(action_name), K_(client_info));
  };

public:
  ObSQLSessionInfo();
  virtual ~ObSQLSessionInfo();

  int init(uint32_t sessid, uint64_t proxy_sessid,
           common::ObIAllocator *bucket_allocator,
           const ObTZInfoMap *tz_info = NULL,
           int64_t sess_create_time = 0,
           uint64_t tenant_id = OB_INVALID_TENANT_ID);
  //for test
  int test_init(uint32_t version, uint32_t sessid, uint64_t proxy_sessid,
           common::ObIAllocator *bucket_allocator);
  void destroy(bool skip_sys_var = false);
  void reset(bool skip_sys_var);
  void clean_status();
  void set_plan_cache_manager(ObPlanCacheManager *pcm) { plan_cache_manager_ = pcm; }
  void set_plan_cache(ObPlanCache *cache) { plan_cache_ = cache; }
  void set_ps_cache(ObPsCache *cache) { ps_cache_ = cache; }
  const common::ObWarningBuffer &get_show_warnings_buffer() const { return show_warnings_buf_; }
  const common::ObWarningBuffer &get_warnings_buffer() const { return warnings_buf_; }
  common::ObWarningBuffer &get_warnings_buffer() { return warnings_buf_; }
  void reset_warnings_buf()
  {
    warnings_buf_.reset();
    pl_exact_err_msg_.reset();
  }

  void reset_show_warnings_buf() { show_warnings_buf_.reset(); }
  ObPrivSet get_user_priv_set() const { return user_priv_set_; }
  ObPrivSet get_db_priv_set() const { return db_priv_set_; }
  ObPlanCache *get_plan_cache();
  ObPsCache *get_ps_cache();
  ObPlanCacheManager *get_plan_cache_manager() { return plan_cache_manager_; }
  obmysql::ObMySQLRequestManager *get_request_manager();
  void set_user_priv_set(const ObPrivSet priv_set) { user_priv_set_ = priv_set; }
  void set_db_priv_set(const ObPrivSet priv_set) { db_priv_set_ = priv_set; }
  void set_show_warnings_buf(int error_code);
  void update_show_warnings_buf();
  void set_global_sessid(const int64_t global_sessid)
  {
    global_sessid_ = global_sessid;
  }
  int64_t get_global_sessid() const { return global_sessid_; }
  void set_read_uncommited(bool read_uncommited) { read_uncommited_ = read_uncommited; }
  bool get_read_uncommited() const { return read_uncommited_; }
  void set_version_provider(const common::ObVersionProvider *version_provider)
  {
    version_provider_ = version_provider;
  }
  const common::ObVersion get_frozen_version() const
  {
    return version_provider_->get_frozen_version();
  }
  const common::ObVersion get_merged_version() const
  {
    return version_provider_->get_merged_version();
  }
  void set_config_provider(const ObSQLConfigProvider *config_provider)
  {
    config_provider_ = config_provider;
  }
  bool is_read_only() const { return config_provider_->is_read_only(); };
  int64_t get_nlj_cache_limit() const { return config_provider_->get_nlj_cache_limit(); };
  bool is_terminate(int &ret) const;

  void set_curr_trans_start_time(int64_t t) { curr_trans_start_time_ = t; };
  int64_t get_curr_trans_start_time() const { return curr_trans_start_time_; };

  void set_curr_trans_last_stmt_time(int64_t t) { curr_trans_last_stmt_time_ = t; };
  int64_t get_curr_trans_last_stmt_time() const { return curr_trans_last_stmt_time_; };

  void set_sess_create_time(const int64_t t) { sess_create_time_ = t; };
  int64_t get_sess_create_time() const { return sess_create_time_; };

  void set_last_refresh_temp_table_time(const int64_t t) { last_refresh_temp_table_time_ = t; };
  int64_t get_last_refresh_temp_table_time() const { return last_refresh_temp_table_time_; };

  void set_has_temp_table_flag() { has_temp_table_flag_ = true; };
  bool get_has_temp_table_flag() const { return has_temp_table_flag_; };
  void set_accessed_session_level_temp_table() { has_accessed_session_level_temp_table_ = true; }
  bool has_accessed_session_level_temp_table() const { return has_accessed_session_level_temp_table_; }
  // ???????????????
  int drop_temp_tables(const bool is_sess_disconn = true, const bool is_xa_trans = false);
  void refresh_temp_tables_sess_active_time(); //??????????????????sess active time
  int drop_reused_oracle_temp_tables();
  int delete_from_oracle_temp_tables(const obrpc::ObDropTableArg &const_drop_table_arg);

  void set_for_trigger_package(bool value) { is_for_trigger_package_ = value; }
  bool is_for_trigger_package() const { return is_for_trigger_package_; }
  void set_trans_type(transaction::ObTxClass t) { trans_type_ = t; }
  transaction::ObTxClass get_trans_type() const { return trans_type_; }

  void get_session_priv_info(share::schema::ObSessionPrivInfo &session_priv) const;
  void set_found_rows(const int64_t count) { found_rows_ = count; }
  int64_t get_found_rows() const { return found_rows_; }
  void set_affected_rows(const int64_t count)
  {
    affected_rows_ = count;
    if (affected_rows_ > 0) {
      trans_flags_.set_has_hold_row_lock(true);
    }
  }
  int64_t get_affected_rows() const { return affected_rows_; }
  bool has_user_super_privilege() const;
  bool has_user_process_privilege() const;
  int check_read_only_privilege(const bool read_only,
                                const ObSqlTraits &sql_traits);
  int check_global_read_only_privilege(const bool read_only,
                                       const ObSqlTraits &sql_traits);

  int remove_prepare(const ObString &ps_name);
  int get_prepare_id(const ObString &ps_name, ObPsStmtId &ps_id) const;
  int add_prepare(const ObString &ps_name, ObPsStmtId ps_id);
  int remove_ps_session_info(const ObPsStmtId stmt_id);
  int get_ps_session_info(const ObPsStmtId stmt_id,
                          ObPsSessionInfo *&ps_session_info) const;
  int64_t get_ps_session_info_size() const { return ps_session_info_map_.size(); }
  inline pl::ObPL *get_pl_engine() const { return GCTX.pl_engine_; }

  pl::ObPLCursorInfo *get_pl_implicit_cursor();
  const pl::ObPLCursorInfo *get_pl_implicit_cursor() const;

  pl::ObPLSqlCodeInfo *get_pl_sqlcode_info();
  const pl::ObPLSqlCodeInfo *get_pl_sqlcode_info() const;

  bool has_pl_implicit_savepoint();
  void clear_pl_implicit_savepoint();
  void set_has_pl_implicit_savepoint(bool v);

  inline pl::ObPLContext *get_pl_context() { return pl_context_; }
  inline const pl::ObPLContext *get_pl_context() const { return pl_context_; }
  inline void set_pl_stack_ctx(pl::ObPLContext *pl_stack_ctx)
  {
    pl_context_ = pl_stack_ctx;
  }

  bool is_pl_debug_on();

  inline void set_pl_attached_id(uint32_t id) { pl_attach_session_id_ = id; }
  inline uint32_t get_pl_attached_id() const { return pl_attach_session_id_; }

  inline common::hash::ObHashSet<common::ObString> *get_pl_sync_pkg_vars()
  {
    return pl_sync_pkg_vars_;
  }

  inline void set_pl_query_sender(observer::ObQueryDriver *driver) { pl_query_sender_ = driver; }
  inline observer::ObQueryDriver* get_pl_query_sender() { return pl_query_sender_; }

  inline void set_ps_protocol(bool is_ps_protocol) { pl_ps_protocol_ = is_ps_protocol; }
  inline bool is_ps_protocol() { return pl_ps_protocol_; }

  inline void set_ob20_protocol(bool is_20protocol) { is_ob20_protocol_ = is_20protocol; }
  inline bool is_ob20_protocol() { return is_ob20_protocol_; }

  int replace_user_variable(const common::ObString &name, const ObSessionVariable &value);
  int replace_user_variable(
    ObExecContext &ctx, const common::ObString &name, const ObSessionVariable &value);
  int replace_user_variables(const ObSessionValMap &user_var_map);
  int replace_user_variables(ObExecContext &ctx, const ObSessionValMap &user_var_map);
  int set_package_variables(ObExecContext &ctx, const ObSessionValMap &user_var_map);
  int set_package_variable(ObExecContext &ctx,
    const common::ObString &key, const common::ObObj &value, bool from_proxy = false);

  inline bool get_pl_can_retry() { return pl_can_retry_; }
  inline void set_pl_can_retry(bool can_retry) { pl_can_retry_ = can_retry; }

  CursorCache &get_cursor_cache() { return pl_cursor_cache_; }
  pl::ObPLCursorInfo *get_cursor(int64_t cursor_id);
  pl::ObDbmsCursorInfo *get_dbms_cursor(int64_t cursor_id);
  int add_cursor(pl::ObPLCursorInfo *cursor);
  int close_cursor(pl::ObPLCursorInfo *&cursor);
  int close_cursor(int64_t cursor_id);
  int make_cursor(pl::ObPLCursorInfo *&cursor);
  int init_cursor_cache();
  int make_dbms_cursor(pl::ObDbmsCursorInfo *&cursor,
                       uint64_t id = OB_INVALID_ID);
  int close_dbms_cursor(int64_t cursor_id);
  int print_all_cursor();

  inline void *get_inner_conn() { return inner_conn_; }
  inline void set_inner_conn(void *inner_conn)
  {
    inner_conn_ = inner_conn;
  }

  // show trace
  common::ObTraceEventRecorder *get_trace_buf();
  void clear_trace_buf();

  ObEndTransAsyncCallback &get_end_trans_cb() { return end_trans_cb_; }
  observer::ObSqlEndTransCb &get_mysql_end_trans_cb()
  {
    return end_trans_cb_.get_mysql_end_trans_cb();
  }
  int get_collation_type_of_names(const ObNameTypeClass type_class, common::ObCollationType &cs_type) const;
  int name_case_cmp(const common::ObString &name, const common::ObString &name_other,
                    const ObNameTypeClass type_class, bool &is_equal) const;
  int kill_query();
  int set_query_deadlocked();

  inline void set_inner_session()
  {
    inner_flag_ = true;
    session_type_ = INNER_SESSION;
  }
  inline void set_user_session()
  {
    inner_flag_ = false;
    session_type_ = USER_SESSION;
  }
  void set_session_type_with_flag();
  void set_session_type(SessionType session_type) { session_type_ = session_type; }
  inline SessionType get_session_type() const { return session_type_; }
  // sql from obclient, proxy, PL are all marked as user_session
  // NOTE: for sql from PL, is_inner() = true, is_user_session() = true
  inline bool is_user_session() const { return USER_SESSION == session_type_; }
  void set_early_lock_release(bool enable);
  bool get_early_lock_release() const { return enable_early_lock_release_; }

  bool is_inner() const
  {
    return inner_flag_;
  }
  void reset_audit_record(bool need_retry = false)
  {
    if (!need_retry) {
      audit_record_.reset();
    } else {
      // memset without try_cnt_ and exec_timestamp_
      int64_t try_cnt = audit_record_.try_cnt_;
      ObExecTimestamp exec_timestamp = audit_record_.exec_timestamp_;
      audit_record_.reset();
      audit_record_.try_cnt_ = try_cnt;
      audit_record_.exec_timestamp_ = exec_timestamp;
    }
  }
  ObAuditRecordData &get_raw_audit_record() { return audit_record_; }
  //??????????????????push record???audit buffer????????????????????????
  //?????????????????????session??????????????????????????????????????????????????????
  //???????????????
  const ObAuditRecordData &get_final_audit_record(ObExecuteMode mode);
  ObSessionStat &get_session_stat() { return session_stat_; }
  void update_stat_from_audit_record();
  void update_alive_time_stat();
  void handle_audit_record(bool need_retry, ObExecuteMode exec_mode);

  void set_is_remote(bool is_remote) { is_remote_session_ = is_remote; }
  bool is_remote_session() const { return is_remote_session_; }

  int save_session(StmtSavedValue &saved_value);
  int save_sql_session(StmtSavedValue &saved_value);
  int restore_sql_session(StmtSavedValue &saved_value);
  int restore_session(StmtSavedValue &saved_value);
  ObExecContext *get_cur_exec_ctx() { return cur_exec_ctx_; }

  int begin_nested_session(StmtSavedValue &saved_value, bool skip_cur_stmt_tables = false);
  int end_nested_session(StmtSavedValue &saved_value);

  //package state related
  inline  ObPackageStateMap &get_package_state_map() { return package_state_map_; }
  inline int get_package_state(uint64_t package_id, pl::ObPLPackageState *&package_state)
  {
    return package_state_map_.get_refactored(package_id, package_state);
  }
  inline int add_package_state(uint64_t package_id, pl::ObPLPackageState *package_state)
  {
    return package_state_map_.set_refactored(package_id, package_state);
  }
  inline int del_package_state(uint64_t package_id)
  {
    return package_state_map_.erase_refactored(package_id);
  }
  void reset_pl_debugger_resource();
  void reset_all_package_changed_info();
  void reset_all_package_state();
  int reset_all_serially_package_state();
  bool is_package_state_changed() const;
  bool get_changed_package_state_num() const;
  int add_changed_package_info(ObExecContext &exec_ctx);
  int shrink_package_info();

  // ?????? session ???????????? sequence.nextval ?????? sequence ??????
  // ????????? ObSequence ???????????????????????????????????? session ???
  int get_sequence_value(uint64_t tenant_id,
                         uint64_t seq_id,
                         share::ObSequenceValue &value);
  int set_sequence_value(uint64_t tenant_id,
                         uint64_t seq_id,
                         const share::ObSequenceValue &value);

  int get_context_values(const common::ObString &context_name,
                        const common::ObString &attribute,
                        common::ObString &value,
                        bool &exist);
  int set_context_values(const common::ObString &context_name,
                        const common::ObString &attribute,
                        const common::ObString &value);
  int clear_all_context(const common::ObString &context_name);
  int clear_context(const common::ObString &context_name,
                    const common::ObString &attribute);
  int64_t get_curr_session_context_size() const { return curr_session_context_size_; }
  void reuse_context_map() 
  {
    for (auto it = contexts_map_.begin(); it != contexts_map_.end(); ++it) {
      if (OB_NOT_NULL(it->second)) {
        it->second->destroy();
      }
    }
    contexts_map_.reuse();
    curr_session_context_size_ = 0;
  }

  int set_client_id(const common::ObString &client_identifier);

  bool has_sess_info_modified() const;
  int set_module_name(const common::ObString &mod);
  int set_action_name(const common::ObString &act);
  int set_client_info(const common::ObString &client_info);
  ApplicationInfo& get_client_app_info() { return client_app_info_; }
  int get_sess_encoder(const SessionSyncInfoType sess_sync_info_type, ObSessInfoEncoder* &encoder);
  const common::ObString& get_module_name() const { return client_app_info_.module_name_; }
  const common::ObString& get_action_name() const  { return client_app_info_.action_name_; }
  const common::ObString& get_client_info() const { return client_app_info_.client_info_; }
  const FLTControlInfo& get_control_info() const { return flt_control_info_; }
  FLTControlInfo& get_control_info() { return flt_control_info_; }
  void set_flt_control_info(const FLTControlInfo &con_info);
  bool is_send_control_info() { return is_send_control_info_; }
  void set_send_control_info(bool is_send) { is_send_control_info_ = is_send; }
  bool is_coninfo_set_by_sess() { return coninfo_set_by_sess_; }
  void set_coninfo_set_by_sess(bool is_set_by_sess) { coninfo_set_by_sess_ = is_set_by_sess; }
  bool is_trace_enable() { return trace_enable_; }
  void set_trace_enable(bool trace_enable) { trace_enable_ = trace_enable; }
  bool is_auto_flush_trace() {return auto_flush_trace_;}
  void set_auto_flush_trace(bool auto_flush_trace) { auto_flush_trace_ = auto_flush_trace; }
  //ObSysVarEncoder& get_sys_var_encoder() { return sys_var_encoder_; }
  //ObUserVarEncoder& get_usr_var_encoder() { return usr_var_encoder_; }
  ObAppInfoEncoder& get_app_info_encoder() { return app_info_encoder_; }
  ObAppCtxInfoEncoder &get_app_ctx_encoder() { return app_ctx_info_encoder_; }
  ObClientIdInfoEncoder &get_client_info_encoder() { return client_id_info_encoder_;}
  ObControlInfoEncoder &get_control_info_encoder() { return control_info_encoder_;}
  ObContextsMap &get_contexts_map() { return contexts_map_; }
  int get_mem_ctx_alloc(common::ObIAllocator *&alloc);
  int update_sess_sync_info(const SessionSyncInfoType sess_sync_info_type,
                                const char *buf, const int64_t length, int64_t &pos);
  int prepare_ps_stmt(const ObPsStmtId inner_stmt_id,
                      const ObPsStmtInfo *stmt_info,
                      ObPsStmtId &client_stmt_id,
                      bool &already_exists,
                      bool is_inner_sql);
  int get_inner_ps_stmt_id(ObPsStmtId cli_stmt_id, ObPsStmtId &inner_stmt_id);
  int close_ps_stmt(ObPsStmtId stmt_id);

  bool is_encrypt_tenant();

  ObSessionDDLInfo &get_ddl_info() { return ddl_info_; }
  void set_ddl_info(const ObSessionDDLInfo &ddl_info) { ddl_info_ = ddl_info; }
  bool is_table_name_hidden() const { return is_table_name_hidden_; }
  void set_table_name_hidden(const bool is_hidden) { is_table_name_hidden_ = is_hidden; }

  ObTenantCachedSchemaGuardInfo &get_cached_schema_guard_info() { return cached_schema_guard_info_; }
  int set_enable_role_array(const common::ObIArray<uint64_t> &role_id_array);
  common::ObIArray<uint64_t>& get_enable_role_array() { return enable_role_array_; }
  void set_in_definer_named_proc(bool in_proc) {in_definer_named_proc_ = in_proc; }
  bool get_in_definer_named_proc() {return in_definer_named_proc_; }
  bool get_prelock() { return prelock_; }
  void set_prelock(bool prelock) { prelock_ = prelock; }

  void set_priv_user_id(uint64_t priv_user_id) { priv_user_id_ = priv_user_id; }
  uint64_t get_priv_user_id() {
    return (priv_user_id_ == OB_INVALID_ID) ? get_user_id() : priv_user_id_; }
  int64_t get_xa_end_timeout_seconds() const;
  int set_xa_end_timeout_seconds(int64_t seconds);
  uint64_t get_priv_user_id_allow_invalid() { return priv_user_id_; }
  int get_xa_last_result() const { return xa_last_result_; }
  void set_xa_last_result(const int result) { xa_last_result_ = result; }
  // ?????????????????????????????????????????????????????????????????????????????????session????????????5s??????????????????
  void refresh_tenant_config() { cached_tenant_config_info_.refresh(); }
  bool is_support_external_consistent()
  {
    cached_tenant_config_info_.refresh();
    return cached_tenant_config_info_.get_is_external_consistent();
  }
  bool is_enable_batched_multi_statement()
  {
    cached_tenant_config_info_.refresh();
    return cached_tenant_config_info_.get_enable_batched_multi_statement();
  }
  bool is_enable_bloom_filter()
  {
    cached_tenant_config_info_.refresh();
    return cached_tenant_config_info_.get_enable_bloom_filter();
  }
  bool is_enable_sql_extension()
  {
    cached_tenant_config_info_.refresh();
    return cached_tenant_config_info_.get_enable_sql_extension();
  }
  bool is_registered_to_deadlock() const { return ATOMIC_LOAD(&is_registered_to_deadlock_); }
  void set_registered_to_deadlock(bool state) { ATOMIC_SET(&is_registered_to_deadlock_, state); }
  int get_tenant_audit_trail_type(ObAuditTrailType &at_type)
  {
    cached_tenant_config_info_.refresh();
    at_type = cached_tenant_config_info_.get_at_type();
    return common::OB_SUCCESS;
  }
  int64_t get_tenant_sort_area_size()
  {
    cached_tenant_config_info_.refresh();
    return cached_tenant_config_info_.get_sort_area_size();
  }
  int get_tmp_table_size(uint64_t &size);
  int ps_use_stream_result_set(bool &use_stream);
  void set_proxy_version(uint64_t v) { proxy_version_ = v; }
  uint64_t get_proxy_version() { return proxy_version_; }

  void set_ignore_stmt(bool v) { is_ignore_stmt_ = v; }
  bool is_ignore_stmt() const { return is_ignore_stmt_; }

  // piece
  void *get_piece_cache(bool need_init = false);

  void set_load_data_exec_session(bool v) { is_load_data_exec_session_ = v; }
  bool is_load_data_exec_session() const { return is_load_data_exec_session_; }
  inline ObSqlString &get_pl_exact_err_msg() { return pl_exact_err_msg_; }
  void set_got_conn_res(bool v) { got_conn_res_ = v; }
  bool has_got_conn_res() const { return got_conn_res_; }
  int on_user_connect(share::schema::ObSessionPrivInfo &priv_info, const ObUserInfo *user_info);
  int on_user_disconnect();
  virtual void reset_tx_variable();
public:
  bool has_tx_level_temp_table() const {
    return tx_level_temp_table_;
  }
  void set_tx_level_temp_table() {
    tx_level_temp_table_ = true;
  }
  //for dblink
  int register_dblink_conn_pool(common::sqlclient::ObCommonServerConnectionPool *dblink_conn_pool);
  int free_dblink_conn_pool();
private:
  int close_all_ps_stmt();
  void destroy_contexts_map(ObContextsMap &map, common::ObIAllocator &alloc);
  inline int init_mem_context(uint64_t tenant_id);
  void set_cur_exec_ctx(ObExecContext *cur_exec_ctx) { cur_exec_ctx_ = cur_exec_ctx; }

  static const int64_t MAX_STORED_PLANS_COUNT = 10240;
  static const int64_t MAX_IPADDR_LENGTH = 64;
private:
  bool is_inited_;
  // store the warning message from the most recent statement in the current session
  common::ObWarningBuffer warnings_buf_;
  common::ObWarningBuffer show_warnings_buf_;
  sql::ObEndTransAsyncCallback end_trans_cb_;
  ObAuditRecordData audit_record_;

  ObPrivSet user_priv_set_;
  ObPrivSet db_priv_set_;
  int64_t curr_trans_start_time_;
  int64_t curr_trans_last_stmt_time_;
  int64_t sess_create_time_;  //??????????????????, ???????????????????????????????????????
  int64_t last_refresh_temp_table_time_; //?????????????????????????????????sess active time??????, ?????????proxy????????????
  bool has_temp_table_flag_;  //??????????????????????????????
  bool has_accessed_session_level_temp_table_;  //???????????????Session?????????
  bool enable_early_lock_release_;
  // trigger.
  bool is_for_trigger_package_;
  transaction::ObTxClass trans_type_;
  const common::ObVersionProvider *version_provider_;
  const ObSQLConfigProvider *config_provider_;
  ObPlanCacheManager *plan_cache_manager_;
  char tenant_buff_[sizeof(share::ObTenantSpaceFetcher)];
  share::ObTenantSpaceFetcher* with_tenant_ctx_;
  obmysql::ObMySQLRequestManager *request_manager_;
  ObPlanCache *plan_cache_;
  ObPsCache *ps_cache_;
  //??????select stmt???scan????????????????????????????????????sql_calc_found_row??????found_row()?????????
  int64_t found_rows_;
  //??????dml?????????affected_row??????row_count()??????
  int64_t affected_rows_;
  int64_t global_sessid_;
  bool read_uncommited_; //???????????????????????????????????????????????????
  common::ObTraceEventRecorder *trace_recorder_;
  //??????????????????????????????write?????????????????????read_only??????????????????commit????????????????????????
  // if has_write_stmt_in_trans_ && read_only => can't not commit
  // else can commit
  // in_transaction_ has been merged into trans_flags_.
//  int64_t has_write_stmt_in_trans_;
  bool inner_flag_; // ??????????????????????????????session
  // 2.2????????????????????????????????????
  bool is_max_availability_mode_;
  typedef common::hash::ObHashMap<ObPsStmtId, ObPsSessionInfo *,
                                  common::hash::NoPthreadDefendMode> PsSessionInfoMap;
  PsSessionInfoMap ps_session_info_map_;
  inline int try_create_ps_session_info_map()
  {
    int ret = OB_SUCCESS;
    static const int64_t PS_BUCKET_NUM = 64;
    if (OB_UNLIKELY(!ps_session_info_map_.created())) {
      ret = ps_session_info_map_.create(common::hash::cal_next_prime(PS_BUCKET_NUM),
                                        common::ObModIds::OB_HASH_BUCKET_PS_SESSION_INFO,
                                        common::ObModIds::OB_HASH_NODE_PS_SESSION_INFO);
    }
    return ret;
  }

  typedef common::hash::ObHashMap<common::ObString, ObPsStmtId,
                                  common::hash::NoPthreadDefendMode> PsNameIdMap;
  PsNameIdMap ps_name_id_map_;
  inline int try_create_ps_name_id_map()
  {
    int ret = OB_SUCCESS;
    static const int64_t PS_BUCKET_NUM = 64;
    if (OB_UNLIKELY(!ps_name_id_map_.created())) {
      ret = ps_name_id_map_.create(common::hash::cal_next_prime(PS_BUCKET_NUM),
                                   common::ObModIds::OB_HASH_BUCKET_PS_SESSION_INFO,
                                   common::ObModIds::OB_HASH_NODE_PS_SESSION_INFO);
    }
    return ret;
  }

  ObPsStmtId next_client_ps_stmt_id_;
  bool is_remote_session_;//????????????????????????????????????????????????session
  SessionType session_type_;
  ObPackageStateMap package_state_map_;
  ObSequenceCurrvalMap sequence_currval_map_;
  ObContextsMap contexts_map_;
  int64_t curr_session_context_size_;

  pl::ObPLContext *pl_context_;
  CursorCache pl_cursor_cache_;
  // if any commit executed, the PL block can not be retried as a whole.
  // otherwise the PL block can be retried in all.
  // if false == pl_can_retry_, we can only retry query in PL blocks locally
  bool pl_can_retry_; //?????????????????????PL????????????????????????

  uint32_t pl_attach_session_id_; // ????????????session?????????dbms_debug.attach_session, ????????????session???ID

  observer::ObQueryDriver *pl_query_sender_; // send query result in mysql pl
  bool pl_ps_protocol_; // send query result use this protocol
  bool is_ob20_protocol_; // mark as whether use oceanbase 2.0 protocol

  int64_t last_plan_id_; // ???????????????????????? plan_id????????? show trace ????????? sql ????????????

  common::hash::ObHashSet<common::ObString> *pl_sync_pkg_vars_ = NULL;

  void *inner_conn_;  // ObInnerSQLConnection * will cause .h included from each other.

  ObSessionStat session_stat_;

  ObSessionEncryptInfo encrypt_info_;

  common::ObSEArray<uint64_t, 8> enable_role_array_;
  ObTenantCachedSchemaGuardInfo cached_schema_guard_info_;
  bool in_definer_named_proc_;
  uint64_t priv_user_id_;
  int64_t xa_end_timeout_seconds_;
  int xa_last_result_;
  // ?????????????????????????????????????????????????????????????????????????????????session????????????5s??????????????????
  ObCachedTenantConfigInfo cached_tenant_config_info_;
  bool prelock_;
  uint64_t proxy_version_;
  uint64_t min_proxy_version_ps_; // proxy???????????????????????????sql???????????????Stmt id
  //???????????????????????????????????????????????????ignore_stmt?????????cast_mode???
  //???????????????????????????stmt??????ignore flag,????????????session???????????????????????????????????????????????????
  //???CG???????????????????????????
  bool is_ignore_stmt_;
  ObSessionDDLInfo ddl_info_;
  bool is_table_name_hidden_;
  void *piece_cache_;
  bool is_load_data_exec_session_;
  // ??????session????????????????????????????????????
  bool is_registered_to_deadlock_;
  ObSqlString pl_exact_err_msg_;
  // Record whether this session has got connection resource, which means it increased connections count.
  // It's used for on_user_disconnect.
  // No matter whether apply for resource successfully, a session will call on_user_disconnect when disconnect.
  // While only session got connection resource can release connection resource and decrease connections count.
  bool got_conn_res_;
  bool tx_level_temp_table_;
  ObArray<common::sqlclient::ObCommonServerConnectionPool *> dblink_conn_pool_array_;  //for dblink to free connection when session drop.
  // get_session_allocator can only apply for fixed-length memory.
  // To customize the memory length, you need to use malloc_alloctor of mem_context
  lib::MemoryContext mem_context_;
  ApplicationInfo client_app_info_;
  char module_buf_[common::OB_MAX_MOD_NAME_LENGTH];
  char action_buf_[common::OB_MAX_ACT_NAME_LENGTH];
  char client_info_buf_[common::OB_MAX_CLIENT_INFO_LENGTH];
  FLTControlInfo flt_control_info_;
  bool trace_enable_ = false;
  bool is_send_control_info_ = false;  // whether send control info to client
  bool auto_flush_trace_ = false;
  bool coninfo_set_by_sess_ = false;

  ObSessInfoEncoder* sess_encoders_[SESSION_SYNC_MAX_TYPE] = {
                            //&sys_var_encoder_
                            //&usr_var_encoder_,
                            &app_info_encoder_,
                            &app_ctx_info_encoder_,
                            &client_id_info_encoder_,
                            &control_info_encoder_
                            };
  //ObSysVarEncoder sys_var_encoder_;
  //ObUserVarEncoder usr_var_encoder_;
  ObAppInfoEncoder app_info_encoder_;
  ObAppCtxInfoEncoder app_ctx_info_encoder_;
  ObClientIdInfoEncoder client_id_info_encoder_;
  ObControlInfoEncoder control_info_encoder_;
  //save the current sql exec context in session
  //and remove the record when the SQL execution ends
  //in order to access exec ctx through session during SQL execution
  ObExecContext *cur_exec_ctx_;
};

inline bool ObSQLSessionInfo::is_terminate(int &ret) const
{
  bool bret = false;
  if (QUERY_KILLED == get_session_state()) {
    bret = true;
    SQL_ENG_LOG(WARN, "query interrupted session",
                "query", get_current_query_string(),
                "key", get_sessid(),
                "proxy_sessid", get_proxy_sessid());
    ret = common::OB_ERR_QUERY_INTERRUPTED;
  } else if (QUERY_DEADLOCKED == get_session_state()) {
    bret = true;
    SQL_ENG_LOG(WARN, "query deadlocked",
                "query", get_current_query_string(),
                "key", get_sessid(),
                "proxy_sessid", get_proxy_sessid());
    ret = common::OB_DEAD_LOCK;
  } else if (SESSION_KILLED == get_session_state()) {
    bret = true;
    ret = common::OB_ERR_SESSION_INTERRUPTED;
  }
  return bret;
}

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_SESSION_OB_SQL_SESSION_INFO_
