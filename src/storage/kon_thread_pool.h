#pragma once
#include "lib/stat/ob_session_stat.h"
#include "observer/omt/ob_tenant.h"
#include "share/ob_thread_pool.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase {
class KonThreadPool : public share::ObThreadPool {
public:
  using KonFuncType = std::function<int(uint64_t)>;
  void set_func(KonFuncType func) { func_ = func; }
  void run1() override {
    common::ObTenantStatEstGuard stat_est_guard(MTL_ID());
    share::ObTenantBase *tenant_base = MTL_CTX();
    lib::Worker::CompatMode mode =
        ((omt::ObTenant *)tenant_base)->get_compat_mode();
    lib::Worker::set_compatibility_mode(mode);
    uint64_t idx = get_thread_idx();
    int ret = func_(idx);
    rets_[idx] = ret;
  }
  int set_thread_num(int64_t thread_num) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(set_thread_count(thread_num))) {
      KON_LOG(WARN, "fail to set thread count");
    } else if (rets_.prepare_allocate(thread_num)) {
      KON_LOG(WARN, "fail to prepare allocate");
    }
    return ret;
  }
  int execute() {
    int ret = OB_SUCCESS;
    set_run_wrapper(MTL_CTX());
    if (OB_FAIL(start())) {
      KON_LOG(WARN, "fail to start");
    } else {
      wait();
      for (int i = 0; OB_SUCC(ret) && i < rets_.count(); i++) {
        if (OB_FAIL(rets_[i])) {
          KON_LOG(WARN, "thread fail", KR(ret));
        }
      }
    }
    return ret;
  }

private:
  KonFuncType func_;
  common::ObSEArray<int, 8> rets_;
};
} // namespace oceanbase