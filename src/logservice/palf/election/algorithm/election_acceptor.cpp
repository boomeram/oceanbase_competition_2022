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

#include "share/ob_occam_time_guard.h"
#include "election_acceptor.h"
#include "common/ob_clock_generator.h"
#include "election_impl.h"
#include "lib/net/ob_addr.h"
#include "logservice/palf/election/interface/election_priority.h"
#include "logservice/palf/election/utils/election_common_define.h"
#include "logservice/palf/election/utils/election_event_recorder.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

#define CHECK_SILENCE()\
do {\
  if (ATOMIC_LOAD(&INIT_TS) < 0) {\
    ELECT_LOG(ERROR, "INIT_TS is less than 0, may not call GLOBAL_INIT_ELECTION_MODULE yet!", K(*this));\
    return;\
  } else if (OB_UNLIKELY(get_monotonic_ts() < ATOMIC_LOAD(&INIT_TS) + MAX_LEASE_TIME)) {\
    ELECT_LOG(INFO, "keep silence for safty, won't send response", K(*this));\
    return;\
  }\
} while(0)

template <typename Type>
struct ResponseType {};

template <>
struct ResponseType<ElectionPrepareRequestMsg> { using type = ElectionPrepareResponseMsg; };

template <>
struct ResponseType<ElectionAcceptRequestMsg> { using type = ElectionAcceptResponseMsg; };

class RequestChecker
{
private:
public:
  template <typename RequestMsg, typename Acceptor>
  static bool check_ballot_valid(const RequestMsg &msg, Acceptor *p_acceptor, const LogPhase phase)
  {
    ELECT_TIME_GUARD(500_ms);
    #define PRINT_WRAPPER K(msg), K(*p_acceptor)
    bool ret = false;
    if (OB_UNLIKELY(msg.get_ballot_number() < p_acceptor->ballot_number_)) {
      using T = typename ResponseType<RequestMsg>::type;
      T reject_msg = create_reject_message_(p_acceptor->p_election_->get_self_addr(),
                                            p_acceptor->p_election_->get_membership_version_(),
                                            msg);
      reject_msg.set_rejected(p_acceptor->ballot_number_);
      p_acceptor->p_election_->send_(reject_msg);
      LOG_PHASE(WARN, phase, "receive old ballot request, refused");
    } else {
      ret = true;
    }
    return ret;
    #undef PRINT_WRAPPER
  }
private:
  static ElectionPrepareResponseMsg create_reject_message_(const common::ObAddr &addr,
                                                           const LogConfigVersion &membership_version,
                                                           const ElectionPrepareRequestMsg &msg)
  {
    UNUSED(membership_version);
    return ElectionPrepareResponseMsg(addr, msg);
  }
  static ElectionAcceptResponseMsg create_reject_message_(const common::ObAddr &addr,
                                                          const LogConfigVersion &membership_version,
                                                          const ElectionAcceptRequestMsg &msg)
  {
    return ElectionAcceptResponseMsg(addr, membership_version, msg);
  }
};

ElectionAcceptor::ElectionAcceptor(ElectionImpl *p_election) :
ballot_number_(INVALID_VALUE),
ballot_of_time_window_(INVALID_VALUE),
is_time_window_opened_(false),
p_election_(p_election),
last_time_window_open_ts_(INVALID_VALUE),
last_dump_acceptor_info_ts_(INVALID_VALUE) {}

void ElectionAcceptor::advance_ballot_number_and_reset_related_states_(const int64_t new_ballot_number,
                                                                       const LogPhase phase)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER K(new_ballot_number), K(*this)
  if (new_ballot_number > ballot_number_) {
    ballot_number_ = new_ballot_number;
    ballot_of_time_window_ = ballot_number_;
    reset_time_window_states_(phase);
  } else {
    LOG_PHASE(ERROR, phase, "invalid argument");
  }
  #undef PRINT_WRAPPER
}

int ElectionAcceptor::start()
{
  ObAddr last_record_lease_owner;
  bool last_record_lease_valid_state = false;
  int ret = OB_SUCCESS;
  return p_election_->timer_->schedule_task_repeat(time_window_task_handle_,
                                                   250_ms,
                                                   [this,
                                                    last_record_lease_owner,
                                                    last_record_lease_valid_state]() mutable {
    ELECT_TIME_GUARD(500_ms);
    #define PRINT_WRAPPER KR(ret), K(*this)
    int ret = OB_SUCCESS;
    
    LockGuard lock_guard(p_election_->lock_);
    // ??????????????????????????????
    if (ObClockGenerator::getCurrentTime() > last_dump_acceptor_info_ts_ + 3_s) {
      last_dump_acceptor_info_ts_ = ObClockGenerator::getCurrentTime();
      ELECT_LOG(INFO, "dump acceptor info", K(*this));
    }
    // ???acceptor???Lease???????????????????????????????????????????????????????????????
    bool lease_valid_state = !lease_.is_expired();
    if (last_record_lease_valid_state != lease_valid_state) {// ????????????lease?????????????????????Lease????????????????????????
      if (lease_valid_state) {// Lease?????????????????????????????????acceptor?????????????????????????????????Leader??????
        LOG_ELECT_LEADER(INFO, "witness new leader");
      } else {// Lease????????????????????????????????????????????????Leader?????????????????????
        LOG_RENEW_LEASE(WARN, "lease expired");
        p_election_->event_recorder_.report_acceptor_lease_expired_event(lease_);
        lease_.reset();
      }
    }
    // ???acceptor??????Lease???owner?????????????????????????????????????????????????????????
    if (last_record_lease_owner != lease_.get_owner()) {
      if (last_record_lease_owner.is_valid() && lease_.get_owner().is_valid()) {
        LOG_CHANGE_LEADER(INFO, "lease owner changed");
        p_election_->event_recorder_.report_acceptor_witness_change_leader_event(last_record_lease_owner, lease_.get_owner());
      }
      last_record_lease_owner = lease_.get_owner();
    }
    if (is_time_window_opened_) {
      ElectionPrepareResponseMsg prepare_res_accept(p_election_->get_self_addr(), highest_priority_prepare_req_);
      bool can_vote = false;
      if (last_record_lease_valid_state && !lease_valid_state) {// ???????????????????????????????????????lease??????????????????????????????lease???????????????????????????
        can_vote = true;
        LOG_ELECT_LEADER(INFO, "vote when lease expired");
      } else if (ObClockGenerator::getCurrentTime() - last_time_window_open_ts_ >= CALCULATE_TIME_WINDOW_SPAN_TS()) {
        can_vote = true;
      } else {
        LOG_ELECT_LEADER(INFO, "can't vote now", K(last_record_lease_valid_state),
                         K(lease_valid_state), K(CALCULATE_TIME_WINDOW_SPAN_TS()),
                         KTIME_(last_time_window_open_ts));
      }
      if (can_vote) {
        // 1. ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????prepare??????
        if (ballot_of_time_window_ == highest_priority_prepare_req_.get_ballot_number() &&
            ballot_of_time_window_ > ballot_number_) {
          // 1.1 ???????????????????????????ballot number???
          ballot_number_ = ballot_of_time_window_;
          // 1.2 ???lease??????????????????lease?????????proposer???????????????lease????????????
          if (lease_.is_expired()) {// ???Lease?????????????????????Lease???Lease?????????????????????????????????????????????
            lease_.reset();
          }
          // 1.3 ??????prepare ok??????
          prepare_res_accept.set_accepted(ballot_number_, lease_);
          if (CLICK_FAIL(p_election_->send_(prepare_res_accept))) {
            LOG_ELECT_LEADER(ERROR, "fail to send prepare ok", K(prepare_res_accept));
          } else {
            p_election_->event_recorder_.report_vote_event(prepare_res_accept.get_receiver(), vote_reason_);
            LOG_ELECT_LEADER(INFO, "time window closed, send vote", K(prepare_res_accept));
          }
        } else {
          LOG_ELECT_LEADER(ERROR, "give up sending prepare response, casuse ballot number not match", K(prepare_res_accept));
        }
        is_time_window_opened_ = false;// ????????????????????????????????????
      }
    }
    last_record_lease_valid_state = lease_valid_state;
    #undef PRINT_WRAPPER
    return false;
  });
}

void ElectionAcceptor::stop()
{
  ELECT_TIME_GUARD(3_s);
  time_window_task_handle_.stop_and_wait();
}

void ElectionAcceptor::reset_time_window_states_(const LogPhase phase)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER K(*this)
  if (is_time_window_opened_) {
    is_time_window_opened_ = false;// ??????ballot number??????????????????????????????
    highest_priority_prepare_req_.reset();
    vote_reason_.reset();
    LOG_PHASE(INFO, phase, "time window closed, not vote");
  }
  #undef PRINT_WRAPPER
}

void ElectionAcceptor::on_prepare_request(const ElectionPrepareRequestMsg &prepare_req)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(prepare_req), K(*this)
  CHECK_SILENCE();// ??????????????????????????????????????????acceptor???????????????????????????????????????lease???????????????
  int ret = OB_SUCCESS;
  LogPhase phase = (prepare_req.get_role() == common::ObRole::FOLLOWER ? LogPhase::ELECT_LEADER : LogPhase::RENEW_LEASE);
  LOG_PHASE(INFO, phase, "handle prepare request");
  if (OB_UNLIKELY(false == p_election_->is_member_list_valid_())) {
    LOG_PHASE(INFO, phase, "ignore prepare when member_list is invalid");
  } else if (prepare_req.get_membership_version() < p_election_->get_membership_version_()) {
    LOG_PHASE(INFO, phase, "ignore lower membership version request");
  } else if (OB_LIKELY(RequestChecker::check_ballot_valid(prepare_req, this, phase))) {
    // 0. ??????leader prepare?????????????????????????????????????????????????????????
    if (prepare_req.get_role() == common::ObRole::LEADER) {
      if (prepare_req.get_ballot_number() <= ballot_number_) {
        LOG_PHASE(WARN, phase, "leader prepare message's ballot number is smaller than self");
      } else {
        advance_ballot_number_and_reset_related_states_(prepare_req.get_ballot_number(), phase);
        ElectionPrepareResponseMsg prepare_res_accept(p_election_->get_self_addr(),
                                                             prepare_req);
        prepare_res_accept.set_accepted(ballot_number_, lease_);
        if (CLICK_FAIL(p_election_->msg_handler_->send(prepare_res_accept))) {
          LOG_PHASE(WARN, phase, "send prepare response to leader prepare failed");
        } else {
          LOG_PHASE(INFO, phase, "receive valid leader prepare message, send vote to him");
        }
      }
    } else {
      // 1. ??????????????????????????????ballot number??????????????????????????????????????????
      if (is_time_window_opened_ && prepare_req.get_ballot_number() > ballot_of_time_window_) {
        reset_time_window_states_(phase);
      }
      // 2. ???????????????????????????????????????????????????
      if (!is_time_window_opened_ && prepare_req.get_ballot_number() > ballot_of_time_window_) {
        ballot_of_time_window_ = prepare_req.get_ballot_number();
        highest_priority_prepare_req_.reset();
        LOG_PHASE(DEBUG, phase, "advance ballot_of_time_window_");
        int64_t ballot_of_time_window_when_registered = ballot_of_time_window_;
        int64_t timewindow_span = 0;
        if (!lease_.is_expired()) {// ??????Lease????????????????????????????????????????????????????????????????????????????????????????????????Lease??????????????????
          timewindow_span = std::max(lease_.get_lease_end_ts() - get_monotonic_ts(), CALCULATE_TIME_WINDOW_SPAN_TS() / 2);
        } else {// ??????????????????????????????????????????????????????????????????????????????????????????
          timewindow_span = CALCULATE_TIME_WINDOW_SPAN_TS();
        }
        if (CLICK_FAIL(time_window_task_handle_.reschedule_after(timewindow_span))) {
          LOG_PHASE(ERROR, phase, "open time window failed");
        } else {
          is_time_window_opened_ = true;// ?????????????????????????????????????????????
          last_time_window_open_ts_ = ObClockGenerator::getCurrentTime();
          LOG_PHASE(INFO, phase, "open time window success", K(timewindow_span));
        }
      }
      // 3. ??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
      if (OB_SUCC(ret) && is_time_window_opened_) {// ?????????????????????????????????
        if (prepare_req.get_ballot_number() != ballot_of_time_window_) {
          LOG_PHASE(INFO, phase, "prepare request's ballot is not same as time window, just ignore");
        } else if (!highest_priority_prepare_req_.is_valid()) {
          highest_priority_prepare_req_ = prepare_req;
          LOG_PHASE(INFO, phase, "highest priority prepare message will be replaced casuse cached highest prioriy message is invalid");
          vote_reason_.assign("the only request");
        } else if (prepare_req.get_membership_version() > highest_priority_prepare_req_.get_membership_version()) {
          highest_priority_prepare_req_ = prepare_req;
          LOG_PHASE(INFO, phase, "highest priority prepare message will be replaced casuse new message's membership version is higher");
          vote_reason_.assign("membership_version is higher");
        } else if (prepare_req.get_membership_version() < highest_priority_prepare_req_.get_membership_version()) {
          LOG_PHASE(INFO, phase, "prepare message's membership version not less than self, but not greater than cached highest priority prepare message");
        } else {
          // 4. ??????????????????????????????????????????????????????
          if (p_election_->is_rhs_message_higher_(highest_priority_prepare_req_, prepare_req, vote_reason_, true, LogPhase::ELECT_LEADER)) {
            LOG_PHASE(INFO, phase, "highest priority prepare request will be replaced", K(vote_reason_));
            highest_priority_prepare_req_ = prepare_req;
          } else {
            LOG_PHASE(INFO, phase, "ignore prepare request, cause it has lower priority", K(vote_reason_));
          }
        }
      }
    }
  }
  #undef PRINT_WRAPPER
}

void ElectionAcceptor::on_accept_request(const ElectionAcceptRequestMsg &accept_req,
                                         int64_t *us_to_expired)
{
  ELECT_TIME_GUARD(500_ms);
  #define PRINT_WRAPPER KR(ret), K(accept_req), K(*this)
  CHECK_SILENCE();// ??????????????????????????????????????????acceptor???????????????????????????????????????lease?????????
  int ret = OB_SUCCESS;
  if (OB_LIKELY(RequestChecker::check_ballot_valid(accept_req,
                                                   this,
                                                   LogPhase::RENEW_LEASE))) {
    // 1. ??????ballot number?????????accept lease???ballot number??????
    if (accept_req.get_ballot_number() > ballot_number_) {
      advance_ballot_number_and_reset_related_states_(accept_req.get_ballot_number(), LogPhase::RENEW_LEASE);
    }
    // 2. ???????????????Lease
    lease_.update_from(accept_req);
    *us_to_expired = lease_.get_lease_end_ts() - get_monotonic_ts();
    // 3. ??????accept ok??????
    ElectionAcceptResponseMsg accept_res_accept(p_election_->get_self_addr(),
                                                p_election_->get_membership_version_(),
                                                accept_req);
    (void) p_election_->refresh_priority_();
    if (CLICK_FAIL(accept_res_accept.set_accepted(ballot_number_,
                                                  p_election_->get_priority_()))) {
      LOG_RENEW_LEASE(ERROR, "fail to copy priority", K(accept_res_accept));
    } else if (CLICK_FAIL(p_election_->send_(accept_res_accept))) {
      LOG_RENEW_LEASE(ERROR, "fail to send msg", K(accept_res_accept));
    }
  }
  #undef PRINT_WRAPPER
}

int64_t ElectionAcceptor::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(p_election_)) {
    common::databuff_printf(buf, buf_len, pos, "{p_election:NULL");
  } else {
    common::databuff_printf(buf, buf_len, pos, "{ls_id:{id:%ld}", p_election_->id_);
    common::databuff_printf(buf, buf_len, pos, ", addr:%s", to_cstring(p_election_->get_self_addr()));
  }
  common::databuff_printf(buf, buf_len, pos, ", ballot_number:%ld", ballot_number_);
  common::databuff_printf(buf, buf_len, pos, ", ballot_of_time_window:%ld", ballot_of_time_window_);
  common::databuff_printf(buf, buf_len, pos, ", lease:%s", to_cstring(lease_));
  common::databuff_printf(buf, buf_len, pos, ", is_time_window_opened:%s", to_cstring(is_time_window_opened_));
  common::databuff_printf(buf, buf_len, pos, ", vote_reason:%s", to_cstring(vote_reason_));
  common::databuff_printf(buf, buf_len, pos, ", last_time_window_open_ts:%s", ObTime2Str::ob_timestamp_str_range<YEAR, USECOND>(last_time_window_open_ts_));
  if (highest_priority_prepare_req_.is_valid()) {
    common::databuff_printf(buf, buf_len, pos, ", highest_priority_prepare_req_sender:%s",
                                                  to_cstring(highest_priority_prepare_req_.get_sender()));
    common::databuff_printf(buf, buf_len, pos, ", highest_priority_prepare_req_ballot:%ld",
                                                  highest_priority_prepare_req_.get_ballot_number());
    common::databuff_printf(buf, buf_len, pos, ", prepare_req_with_null_priority:%s",
                                                  to_cstring(!highest_priority_prepare_req_.is_buffer_valid()));
  }
  common::databuff_printf(buf, buf_len, pos, ", p_election:0x%lx}", (unsigned long)p_election_);
  return pos;
}

}
}
}