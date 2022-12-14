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

#define USING_LOG_PREFIX SQL_OPT

#include "sql/optimizer/ob_opt_est_cost_model.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include <math.h>
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::jit::expr;

const int64_t ObOptEstCostModel::DEFAULT_LOCAL_ORDER_DEGREE = 32;
const int64_t ObOptEstCostModel::DEFAULT_MAX_STRING_WIDTH = 64;
const int64_t ObOptEstCostModel::DEFAULT_FIXED_OBJ_WIDTH = 12;

int ObCostTableScanInfo::assign(const ObCostTableScanInfo &est_cost_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(range_columns_.assign(est_cost_info.range_columns_))) {
    LOG_WARN("failed to assign range columns", K(ret));
  } else if (OB_FAIL(access_columns_.assign(est_cost_info.access_columns_))) {
    LOG_WARN("failed to assign access columns", K(ret));
  } else if (OB_FAIL(access_column_items_.assign(est_cost_info.access_column_items_))) {
    LOG_WARN("failed to assign access columns", K(ret));
  } else if (OB_FAIL(index_access_column_items_.assign(est_cost_info.index_access_column_items_))) {
    LOG_WARN("failed to assign access columns", K(ret));
  } else {
    table_id_ = est_cost_info.table_id_;
    ref_table_id_ = est_cost_info.ref_table_id_;
    index_id_ = est_cost_info.index_id_;
    table_meta_info_ = est_cost_info.table_meta_info_;
    index_meta_info_.assign(est_cost_info.index_meta_info_);
    is_virtual_table_ = est_cost_info.is_virtual_table_;
    is_unique_ = est_cost_info.is_unique_;
    table_metas_ = est_cost_info.table_metas_;
    sel_ctx_ = est_cost_info.sel_ctx_;
    row_est_method_ = est_cost_info.row_est_method_;
    prefix_filter_sel_ = est_cost_info.prefix_filter_sel_;
    pushdown_prefix_filter_sel_ = est_cost_info.pushdown_prefix_filter_sel_;
    postfix_filter_sel_ = est_cost_info.postfix_filter_sel_;
    table_filter_sel_ = est_cost_info.table_filter_sel_;
    batch_type_ = est_cost_info.batch_type_;
    sample_info_ = est_cost_info.sample_info_;
    // no need to copy table scan param
  }
  return ret;
}

void ObTableMetaInfo::assign(const ObTableMetaInfo &table_meta_info)
{
  ref_table_id_ = table_meta_info.ref_table_id_;
  schema_version_ = table_meta_info.schema_version_;
  part_count_ = table_meta_info.part_count_;
  micro_block_size_ = table_meta_info.micro_block_size_;
  part_size_ = table_meta_info.part_size_;
  average_row_size_ = table_meta_info.average_row_size_;
  table_column_count_ = table_meta_info.table_column_count_;
  table_rowkey_count_ = table_meta_info.table_rowkey_count_;
  table_row_count_ = table_meta_info.table_row_count_;
  row_count_ = table_meta_info.row_count_;
  is_only_memtable_data_ = table_meta_info.is_only_memtable_data_;
  cost_est_type_ = table_meta_info.cost_est_type_;
  has_opt_stat_ = table_meta_info.has_opt_stat_;
  is_empty_table_ = table_meta_info.is_empty_table_;
  micro_block_count_ = table_meta_info.micro_block_count_;
}

double ObTableMetaInfo::get_micro_block_numbers() const
{
  double ret = 0.0;
  if (micro_block_count_ <= 0) {
    // calculate micore block count use storage statistics
    ret = 0;
  } else {
    // get micro block count from optimizer statistics
    ret = static_cast<double>(micro_block_count_);
  }
  return ret;
}

void ObIndexMetaInfo::assign(const ObIndexMetaInfo &index_meta_info)
{
  ref_table_id_ = index_meta_info.ref_table_id_;
  index_id_ = index_meta_info.index_id_;
  index_micro_block_size_ = index_meta_info.index_micro_block_size_;
  index_part_count_ = index_meta_info.index_part_count_;
  index_part_size_ = index_meta_info.index_part_size_;
  index_part_count_ = index_meta_info.index_part_count_;
  index_column_count_ = index_meta_info.index_column_count_;
  is_index_back_ = index_meta_info.is_index_back_;
  is_unique_index_ = index_meta_info.is_unique_index_;
  is_global_index_ = index_meta_info.is_global_index_;
  index_micro_block_count_ = index_meta_info.index_micro_block_count_;
}

double ObIndexMetaInfo::get_micro_block_numbers() const
{
  double ret = 0.0;
  if (index_micro_block_count_ <= 0) {
    // calculate micore block count use storage statistics
    ret = 0;
  } else {
    // get micro block count from optimizer statistics
    ret = static_cast<double>(index_micro_block_count_);
  }
  return ret;
}

/**
 * @brief    ??????Nested Loop Join?????????
 * @formula  cost(?????????) = get_next_row_cost
 *                         + left_cost + right_cost
 *                         + left_rows * rescan_cost
 *                         + JOIN_PER_ROW_COST * output_rows
 *                         + qual_cost
 */
int ObOptEstCostModel::cost_nestloop(const ObCostNLJoinInfo &est_cost_info,
                                    double &cost,
                                    ObIArray<ObExprSelPair> &all_predicate_sel)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  if (OB_ISNULL(est_cost_info.table_metas_) || OB_ISNULL(est_cost_info.sel_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point", K(est_cost_info.table_metas_), K(est_cost_info.sel_ctx_));
  } else {
    double left_rows = est_cost_info.left_rows_;
    double right_rows = est_cost_info.right_rows_;
    double cart_tuples = left_rows * right_rows; // tuples of Cartesian product
    double out_tuples = 0.0;
    double filter_selectivity = 0.0;
    double material_cost = 0.0;
    //selectivity for equal conds
    if (OB_FAIL(ObOptSelectivity::calculate_selectivity(*est_cost_info.table_metas_,
                                                        *est_cost_info.sel_ctx_,
                                                        est_cost_info.other_join_conditions_,
                                                        filter_selectivity,
                                                        all_predicate_sel))) {
      LOG_WARN("Failed to calculate filter selectivity", K(ret));
    } else {
      out_tuples = cart_tuples * filter_selectivity;

      // ?????????????????????????????????????????????????????????????????????????????????????????????get_next_row????????????
      // ??????????????????????????????????????????????????????
      double once_rescan_cost = 0.0;
      if (est_cost_info.need_mat_) {
        once_rescan_cost = cost_read_materialized(right_rows);
      } else {
        double rescan_cost = 0.0;
        if (est_cost_info.right_has_px_rescan_) {
          if (est_cost_info.parallel_ > 1) {
            rescan_cost = cost_params_.PX_RESCAN_PER_ROW_COST;
          } else {
            rescan_cost = cost_params_.PX_BATCH_RESCAN_PER_ROW_COST;
          }
        } else {
          rescan_cost = cost_params_.RESCAN_COST;
        }
        once_rescan_cost = est_cost_info.right_cost_ + rescan_cost
                           + right_rows * cost_params_.CPU_TUPLE_COST;
      }
      // total rescan cost
      if (LEFT_SEMI_JOIN == est_cost_info.join_type_
          || LEFT_ANTI_JOIN == est_cost_info.join_type_) {
        double match_sel = (est_cost_info.anti_or_semi_match_sel_ < OB_DOUBLE_EPSINON) ?
                            OB_DOUBLE_EPSINON : est_cost_info.anti_or_semi_match_sel_;
        out_tuples = left_rows * match_sel;
      }
      cost += left_rows * once_rescan_cost;
      //qual cost
      double qual_cost = cost_quals(left_rows * right_rows, est_cost_info.equal_join_conditions_) +
                         cost_quals(left_rows * right_rows, est_cost_info.other_join_conditions_);

      cost += qual_cost;

      double join_cost = cost_params_.JOIN_PER_ROW_COST * out_tuples;
      cost += join_cost;

      LOG_TRACE("OPT: [COST NESTLOOP JOIN]",
                K(cost), K(qual_cost), K(join_cost),K(once_rescan_cost),
                K(est_cost_info.left_cost_), K(est_cost_info.right_cost_),
                K(left_rows), K(right_rows), K(est_cost_info.right_width_),
                K(filter_selectivity), K(cart_tuples), K(material_cost));
    }
  }
  return ret;
}

/**
 * @brief    ??????Merge Join?????????
 * @formula  cost(?????????) = left_cost + right_cost
 *                         + get_next_row_cost
 *                         + qual_cost
 *                         + COST_JOIN_PER_ROW * output_rows
 *
 * @param[in]  est_cost_info       ????????????merge join?????????????????????
 * @param[out] merge_cost          merge join??????????????????
 */
int ObOptEstCostModel::cost_mergejoin(const ObCostMergeJoinInfo &est_cost_info,
                                 			double &cost)
{
  int ret = OB_SUCCESS;
  double left_selectivity = 0.0;
  double right_selectivity = 0.0;
  cost = 0.0;
  double left_rows = est_cost_info.left_rows_;
  double right_rows = est_cost_info.right_rows_;
  double left_width = est_cost_info.left_width_;
  double cond_tuples = 0.0;
  double out_tuples = 0.0;
  double cond_sel = est_cost_info.equal_cond_sel_;
  double filter_sel = est_cost_info.other_cond_sel_;
  if (IS_SEMI_ANTI_JOIN(est_cost_info.join_type_)) {
    if (LEFT_SEMI_JOIN == est_cost_info.join_type_) {
      cond_tuples = left_rows * cond_sel;
    } else if (LEFT_ANTI_JOIN == est_cost_info.join_type_) {
      cond_tuples = left_rows * (1 - cond_sel);
    } else if (RIGHT_SEMI_JOIN == est_cost_info.join_type_) {
      cond_tuples = right_rows * cond_sel;
    } else if (RIGHT_ANTI_JOIN == est_cost_info.join_type_) {
      cond_tuples = right_rows * (1 - cond_sel);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected join type", K(est_cost_info.join_type_), K(ret));
    }
  } else {
    cond_tuples = left_rows * right_rows * cond_sel;
  }
  out_tuples = cond_tuples * filter_sel;
  // get_next_row()???????????????????????????????????????
  cost += cost_params_.CPU_TUPLE_COST * (left_rows + right_rows);
  // ????????????
  cost += cost_quals(cond_tuples, est_cost_info.equal_join_conditions_) + 
          cost_quals(cond_tuples, est_cost_info.other_join_conditions_);
  // JOIN???????????????
  cost += cost_params_.JOIN_PER_ROW_COST  * out_tuples;
  cost += cost_material(left_rows, left_width);
  cost += cost_read_materialized(left_rows);
  LOG_TRACE("OPT: [COST MERGE JOIN]",
                  K(left_rows), K(right_rows),
                  K(cond_sel), K(filter_sel),
                  K(cond_tuples), K(out_tuples),
                  K(cost));


  return ret;
}

/**
 * @brief    ??????Hash Join?????????
 * @formula  cost(?????????) = left_cost + right_cost
 *                         + left_rows * BUILD_HASH_PER_ROW_COST
 *                         + material_cost
 *                         + right_rows * PROBE_HASH_PER_ROW_COST
 *                         + (left_rows + right_rows) * HASH_COST
 *                         + qual_cost
 *                         + JOIN_PER_ROW_COST * output_rows
 * @param[in]  est_cost_info       ????????????hash join?????????????????????
 * @param[out] hash_cost           hash join??????????????????
 * @param[in]  all_predicate_sel   ????????????????????????
 */
int ObOptEstCostModel::cost_hashjoin(const ObCostHashJoinInfo &est_cost_info,
                                		 double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double build_hash_cost = 0.0;
  double left_rows = est_cost_info.left_rows_;
  double right_rows = est_cost_info.right_rows_;
  double cond_sel = est_cost_info.equal_cond_sel_;
  double filter_sel = est_cost_info.other_cond_sel_;
  // number of tuples satisfying join-condition
  double cond_tuples = 0.0;
  // number of tuples satisfying filters, which is also the number of output tuples
  double out_tuples = 0.0;

  if (IS_SEMI_ANTI_JOIN(est_cost_info.join_type_)) {
    if (LEFT_SEMI_JOIN == est_cost_info.join_type_) {
      cond_tuples = left_rows * cond_sel;
    } else if (LEFT_ANTI_JOIN == est_cost_info.join_type_) {
      cond_tuples = left_rows * (1 - cond_sel);
    } else if (RIGHT_SEMI_JOIN == est_cost_info.join_type_) {
      cond_tuples = right_rows * cond_sel;
    } else if (RIGHT_ANTI_JOIN == est_cost_info.join_type_) {
      cond_tuples = right_rows * (1 - cond_sel);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected join type", K(est_cost_info.join_type_), K(ret));
    }
  }
  out_tuples = cond_tuples * filter_sel;
  double join_filter_cost = 0.0;
  for (int i = 0; i < est_cost_info.join_filter_infos_.count(); ++i) {
    const JoinFilterInfo& info = est_cost_info.join_filter_infos_.at(i);
    //bloom filter?????????????????????
    join_filter_cost += cost_hash(left_rows, info.lexprs_) + cost_hash(right_rows, info.rexprs_);
    if (info.need_partition_join_filter_) {
      //partition join filter??????
      join_filter_cost += cost_hash(left_rows, info.lexprs_);
    }
    right_rows *= info.join_filter_selectivity_;
  }
  cost += join_filter_cost;
  // build hash cost for left table
  build_hash_cost += cost_params_.CPU_TUPLE_COST * left_rows;
  build_hash_cost += cost_material(left_rows, est_cost_info.left_width_);
  build_hash_cost += cost_hash(left_rows, est_cost_info.equal_join_conditions_);
  build_hash_cost += cost_params_.BUILD_HASH_PER_ROW_COST * left_rows;
  // probe cost for right table
  cost += build_hash_cost;
  cost += cost_params_.CPU_TUPLE_COST * right_rows;
  cost += cost_hash(right_rows, est_cost_info.equal_join_conditions_);
  cost += cost_params_.PROBE_HASH_PER_ROW_COST * right_rows;
  cost += cost_quals(cond_tuples, est_cost_info.equal_join_conditions_)
                     + cost_quals(cond_tuples, est_cost_info.other_join_conditions_);
  cost += cost_params_.JOIN_PER_ROW_COST  * out_tuples;
  LOG_TRACE("OPT: [COST HASH JOIN]",
            K(left_rows), K(right_rows),
            K(cond_sel), K(filter_sel),
            K(cond_tuples), K(out_tuples),
            K(join_filter_cost),
            K(cost), K(build_hash_cost));

  return ret;
}

int ObOptEstCostModel::cost_sort_and_exchange(OptTableMetas *table_metas,
																							OptSelectivityCtx *sel_ctx,
																							const ObPQDistributeMethod::Type dist_method,
																							const bool is_distributed,
																							const bool is_local_order,
																							const double input_card,
																							const double input_width,
																							const double input_cost,
																							const int64_t out_parallel,
																							const int64_t in_server_cnt,
																							const int64_t in_parallel,
																							const ObIArray<OrderItem> &expected_ordering,
																							const bool need_sort,
																							const int64_t prefix_pos,
																							double &cost)
{
  int ret = OB_SUCCESS;
  double exch_cost = 0.0;
  double sort_cost = 0.0;
  bool need_exchange = (dist_method != ObPQDistributeMethod::NONE);
  bool exchange_need_merge_sort = need_exchange && (is_distributed || is_local_order) &&
                                  (!need_sort || ObPQDistributeMethod::LOCAL == dist_method);
  bool exchange_sort_local_order = need_exchange && !need_sort && is_local_order;
  bool need_exchange_down_sort = (ObPQDistributeMethod::LOCAL == dist_method ||
                                  ObPQDistributeMethod::NONE == dist_method) &&
                                  (need_sort || is_local_order);
  bool need_exchange_up_sort = need_sort && need_exchange && ObPQDistributeMethod::LOCAL != dist_method;

  cost = 0.0;
  if (need_exchange) {
    ObSEArray<OrderItem, 8> exchange_sort_keys;
    if (exchange_need_merge_sort && OB_FAIL(exchange_sort_keys.assign(expected_ordering))) {
      LOG_WARN("failed to assign sort keys", K(ret));
    } else {
      ObExchCostInfo exch_info(input_card,
                               input_width,
                               dist_method,
                               out_parallel,
                               in_parallel,
                               exchange_sort_local_order,
                               exchange_sort_keys,
                               in_server_cnt);
      if (OB_FAIL(ObOptEstCostModel::cost_exchange(exch_info, exch_cost))) {
        LOG_WARN("failed to cost exchange", K(ret));
      } else { /*do nothing*/ }
    }
  }

  if (OB_SUCC(ret) && (need_exchange_down_sort || need_exchange_up_sort)) {
    double card = input_card;
    double width = input_width;
    bool real_local_order = false;
    int64_t real_prefix_pos = 0;
    if (need_exchange_down_sort) {
      card /= out_parallel;
      real_prefix_pos = need_sort && !is_local_order ? prefix_pos : 0;
      real_local_order = need_sort ? false : is_local_order;
    } else {
      real_prefix_pos = need_exchange ? 0 : prefix_pos;
      if (ObPQDistributeMethod::BROADCAST != dist_method) {
        card /= in_parallel;
      }
    }
    ObSortCostInfo cost_info(card,
                             width,
                             real_prefix_pos,
                             expected_ordering,
                             real_local_order,
                             table_metas,
                             sel_ctx);
    if (OB_FAIL(ObOptEstCostModel::cost_sort(cost_info, sort_cost))) {
      LOG_WARN("failed to calc cost", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    cost = input_cost + exch_cost + sort_cost;
    LOG_TRACE("succeed to compute distributed sort cost", K(input_cost), K(exch_cost), K(sort_cost),
        K(need_sort), K(prefix_pos), K(is_local_order));
  }
  return ret;
}

int ObOptEstCostModel::cost_sort(const ObSortCostInfo &cost_info,
                            		 double &cost)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> order_exprs;
  ObSEArray<ObExprResType, 4> order_types;
  // top-n??????????????????????????????
  // ??????????????????est_sel_info??????????????????????????????????????????
  cost = 0.0;
  if (OB_FAIL(ObOptimizerUtil::get_expr_and_types(cost_info.order_items_,
                                                         order_exprs,
                                                         order_types))) {
    LOG_WARN("failed to get expr types", K(ret));
  } else if (order_exprs.empty()) {
    /*do nothing*/
  } else if (cost_info.is_local_merge_sort_) {
    if (OB_FAIL(cost_local_order_sort(cost_info, order_types, cost))) {
      LOG_WARN("failed to cost local order sort", K(ret));
    } else {
      // get_next_row??????????????????????????????
      cost += cost_params_.CPU_TUPLE_COST * cost_info.rows_;
    }
  } else if (cost_info.topn_ >= 0) {
    //top-n sort
    if (OB_FAIL(cost_topn_sort(cost_info, order_types, cost))) {
      LOG_WARN("failed to calc topn sort cost", K(ret));
    } else {
      // get_next_row??????????????????????????????
      cost += cost_params_.CPU_TUPLE_COST * cost_info.rows_;
    }
  } else if (cost_info.prefix_pos_ > 0) {
    // prefix sort
    if (OB_FAIL(cost_prefix_sort(cost_info, order_exprs, cost_info.topn_, cost))) {
      LOG_WARN("failed to calc prefix cost", K(ret));
    } else {
      // get_next_row??????????????????????????????
      cost += cost_params_.CPU_TUPLE_COST * cost_info.rows_;
    }
  }  else if (cost_info.part_cnt_ > 0) {
    // part sort
    if (OB_FAIL(cost_part_sort(cost_info, order_exprs, order_types, cost))) {
      LOG_WARN("failed to calc part cost", K(ret));
    } else {
      // get_next_row??????????????????????????????
      cost += cost_params_.CPU_TUPLE_COST * cost_info.rows_;
    }
  } else {
    // normal sort
    if (OB_FAIL(cost_sort(cost_info, order_types, cost))) {
      LOG_WARN("failed to calc cost", K(ret));
    } else {
      // get_next_row??????????????????????????????
      cost += cost_params_.CPU_TUPLE_COST * cost_info.rows_;
    }
  }
  LOG_TRACE("succeed to compute sort cost", K(cost_info), K(cost));
  return ret;
}

/**
 * @brief      ??????Sort????????????????????????
 * @formula    cost = material_cost + sort_cost
 *             material_cost = cost_material(...) + cost_read_materialized(...)
 *             sort_cost = cost_cmp_per_row * N * logN
 * @param[in]  cost_info   ?????????????????????????????????
 *                         row         ??????????????????
 *                         width       ????????????
 * @param[in]  order_cols  ?????????
 * @param[out] cost        ???????????????????????????
 */
int ObOptEstCostModel::cost_sort(const ObSortCostInfo &cost_info,
																const ObIArray<ObExprResType> &order_col_types,
																double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double real_sort_cost = 0.0;
  double material_cost = 0.0;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  if (rows < 1.0) {
    material_cost = 0;
  } else {
    material_cost = cost_material(rows, width) + cost_read_materialized(rows * LOG2(rows));
  }
  if (OB_FAIL(cost_sort_inner(order_col_types, rows, real_sort_cost))) {
    LOG_WARN("failed to calc cost", K(ret));
  } else {
    cost = material_cost + real_sort_cost;
    LOG_TRACE("OPT: [COST SORT]", K(cost), K(material_cost), K(real_sort_cost),
              K(rows), K(width), K(order_col_types), "is_prefix_sort", cost_info.prefix_pos_ > 0);
  }
  return ret;
}

/**
 * ????????????????????????2????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
 * @brief      ?????? PART_SORT Sort????????????????????????????????????????????????
 * @formula    cost = material_cost + hash_cost + sort_cost
 *             material_cost = cost_material(...) + cost_read_materialized(...)
 *             hash_cost = calc_hash * part_expr * rows + build_hash * rows
 *             sort_cost = cost_cmp_per_row * rows * theoretical_cmp_times
 * @param[in]  cost_info          ?????????????????????????????????
 *                                rows        ??????????????????
 *                                width       ????????????
 * @param[in]  order_exprs        ?????????????????????????????????????????? part by?????????????????? order by
 * @param[in]  order_col_types    ??????????????????????????????order by ????????????????????????
 * @param[in]  part_cnt          ??????????????? part by ???????????????????????????????????????
 * @param[out] cost               ???????????????????????????
 */
int ObOptEstCostModel::cost_part_sort(const ObSortCostInfo &cost_info,
                                      const ObIArray<ObRawExpr *> &order_exprs,
                                      const ObIArray<ObExprResType> &order_col_types,
                                      double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double real_sort_cost = 0.0;
  double material_cost = 0.0;
  double calc_hash_cost = 0.0;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  double distinct_parts = rows;

  ObSEArray<ObRawExpr*, 4> part_exprs;
  ObSEArray<ObExprResType, 4> sort_types;
  for (int64_t i = 0; OB_SUCC(ret) && i < order_exprs.count(); ++i) {
    if (i < cost_info.part_cnt_) {
      if (OB_FAIL(part_exprs.push_back(order_exprs.at(i)))) {
        LOG_WARN("fail to push back expr", K(ret));
      }
    } else {
      if (OB_FAIL(sort_types.push_back(order_col_types.at(i)))) {
        LOG_WARN("fail to push back type", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptSelectivity::calculate_distinct(*cost_info.table_metas_,
                                                      *cost_info.sel_ctx_,
                                                      part_exprs,
                                                      rows,
                                                      distinct_parts))) {
      LOG_WARN("failed to calculate distinct", K(ret));
    } else if (OB_UNLIKELY(distinct_parts < 1.0 || distinct_parts > rows)) {
      distinct_parts = rows;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(rows < 0.0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid row count", K(rows), K(ret));
    } else if (rows < 1.0) {
      // do nothing
    } else {
      double comp_cost = 0.0;
      if (sort_types.count() > 0 && OB_FAIL(get_sort_cmp_cost(sort_types, comp_cost))) {
        LOG_WARN("failed to get cmp cost", K(ret));
      } else if (OB_UNLIKELY(comp_cost < 0.0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("negative cost", K(comp_cost), K(ret));
      } else {
        real_sort_cost = rows * LOG2(rows / distinct_parts) * comp_cost;
        material_cost = cost_material(rows, width) + cost_read_materialized(rows);
        calc_hash_cost = cost_hash(rows, part_exprs) + rows * cost_params_.BUILD_HASH_PER_ROW_COST / 2.0;
        cost = real_sort_cost + material_cost + calc_hash_cost;
        LOG_TRACE("OPT: [COST HASH SORT]", K(cost), K(real_sort_cost), K(calc_hash_cost),
                  K(material_cost), K(rows), K(width), K(cost_info.part_cnt_));
      }
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_prefix_sort(const ObSortCostInfo &cost_info,
																				const ObIArray<ObRawExpr *> &order_exprs,
																				const int64_t topn_count,
																				double &cost)
{
  int ret = OB_SUCCESS;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  double cost_per_group = 0.0;
  if (OB_ISNULL(cost_info.table_metas_) || OB_ISNULL(cost_info.sel_ctx_) ||
      OB_UNLIKELY(cost_info.prefix_pos_ <= 0 || cost_info.prefix_pos_ >= order_exprs.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected error", K(cost_info.table_metas_), K(cost_info.sel_ctx_),
        K(cost_info.prefix_pos_), K(order_exprs.count()), K(ret));
  } else {
    ObSEArray<ObRawExpr*, 4> prefix_ordering;
    ObSEArray<OrderItem, 4> ordering_per_group;
    for (int64_t i = 0; OB_SUCC(ret) && i < cost_info.prefix_pos_; i++) {
      if (OB_ISNULL(order_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(prefix_ordering.push_back(order_exprs.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
    for (int64_t i = cost_info.prefix_pos_; OB_SUCC(ret) && i < order_exprs.count(); ++i) {
      if (OB_ISNULL(order_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ordering_per_group.push_back(OrderItem(order_exprs.at(i))))) {
        LOG_WARN("failed to push array", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      // ?????????????????????????????????????????????????????????????????????topn??????
      int64_t prefix_pos = 0;
      double num_rows_per_group = 0;
      double num_distinct_rows = rows;
      if (OB_FAIL(ObOptSelectivity::calculate_distinct(*cost_info.table_metas_,
                                                       *cost_info.sel_ctx_,
                                                       prefix_ordering,
                                                       rows,
                                                       num_distinct_rows))) {
        LOG_WARN("failed to calculate distinct", K(ret));
      } else if (OB_UNLIKELY(std::fabs(num_distinct_rows) < OB_DOUBLE_EPSINON)) {
        num_rows_per_group = 0;
      } else {
        num_rows_per_group = rows / num_distinct_rows;
      }
      if (topn_count >= 0) {
        // topn prefix sort
        double remaining_count = topn_count;
        while (remaining_count > 0) {
          ObSortCostInfo cost_info_per_group(num_rows_per_group,
                                             width,
                                             prefix_pos,
                                             ordering_per_group,
                                             false);
          cost_info_per_group.topn_ = remaining_count;
          if (OB_FAIL(cost_sort(cost_info_per_group, cost_per_group))) {
            LOG_WARN("failed to cost sort", K(ret));
          } else {
            cost += cost_per_group;
            remaining_count -= num_rows_per_group;
          }
        }
      } else {
        // normal prefix sort
        ObSortCostInfo cost_info_per_group(num_rows_per_group,
                                           width,
                                           prefix_pos,
                                           ordering_per_group,
                                           false,
                                           cost_info.table_metas_,
                                           cost_info.sel_ctx_);
        if (OB_FAIL(cost_sort(cost_info_per_group, cost_per_group))) {
          LOG_WARN("failed to calc cost", K(ret));
        } else {
          cost = cost_per_group * num_distinct_rows;
          LOG_TRACE("OPT: [COST PREFIX SORT]", K(cost), K(cost_per_group), K(num_distinct_rows));
        }
      }
    }
  }
  return ret;
}

/**
 * @brief ?????????????????????????????????????????????
 *
 *    cost = cost_cmp * rows * log(row_count)
 */
int ObOptEstCostModel::cost_sort_inner(const ObIArray<ObExprResType> &types,
																			double row_count,
																			double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  if (OB_UNLIKELY(0.0 > row_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row count", K(row_count), K(ret));
  } else if (row_count < 1.0) {
    // LOG2(x) ???x??????1???????????????????????????????????????
    cost = 0.0;
  } else {
    double cost_cmp = 0.0;
    if (OB_FAIL(get_sort_cmp_cost(types, cost_cmp))) {
      LOG_WARN("failed to get cmp cost", K(ret));
    } else if (OB_UNLIKELY(0.0 > cost_cmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("negative cost", K(cost_cmp), K(ret));
    } else {
      cost = cost_cmp * row_count * LOG2(row_count);
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_local_order_sort_inner(const common::ObIArray<sql::ObExprResType> &types,
																									double row_count,
																									double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  if (OB_UNLIKELY(0.0 > row_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row count", K(row_count), K(ret));
  } else if (row_count < 1.0) {
    // LOG2(x) ???x??????1???????????????????????????????????????
    cost = 0.0;
  } else {
    double cost_cmp = 0.0;
    if (OB_FAIL(get_sort_cmp_cost(types, cost_cmp))) {
      LOG_WARN("failed to get cmp cost", K(ret));
    } else if (OB_UNLIKELY(0.0 > cost_cmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("negative cost", K(cost_cmp), K(ret));
    } else {
      cost = cost_cmp * row_count * LOG2(ObOptEstCostModel::DEFAULT_LOCAL_ORDER_DEGREE);
    }
  }
  return ret;
}

/**
 * @brief      ??????TOP-N Sort????????????????????????
 * @formula    cost = material_cost + sort_cost
 *             material_cost = cost_material(...)
 *             sort_cost = cost_cmp_per_row * rows * logN
 * @param[in]  cost_info   ?????????????????????????????????
 *                         rows        ??????????????????
 *                         topn        TOP-N
 *                         width       ????????????
 * @param[in]  store_cols  ??????????????????????????????
 * @param[in]  order_cols  ?????????
 * @param[out] cost        ???????????????????????????
 */
int ObOptEstCostModel::cost_topn_sort(const ObSortCostInfo &cost_info,
																			const ObIArray<ObExprResType> &types,
																			double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  double topn = cost_info.topn_;
  double real_sort_cost = 0.0;
  double material_cost = 0.0;
  if (0 == types.count() || topn < 0) {
    // do nothing
  } else {
    if (topn > rows) {
      topn = rows;
    }
    // top-n sort????????????n??????????????????rows???
    // ????????????topn sort????????????????????????????????????(n + rows) / 2
    material_cost = cost_material(topn, width);
    if (OB_FAIL(cost_topn_sort_inner(types, rows, topn, real_sort_cost))) {
      LOG_WARN("failed to calc cost", K(ret));
    } else {
      cost = material_cost + real_sort_cost;
      LOG_TRACE("OPT: [COST TOPN SORT]", K(cost), K(material_cost),
                K(real_sort_cost), K(rows), K(width), K(topn));
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_local_order_sort(const ObSortCostInfo &cost_info,
																						const ObIArray<ObExprResType> &types,
																						double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double real_sort_cost = 0.0;
  double material_cost = 0.0;
  double rows = cost_info.rows_;
  double width = cost_info.width_;
  material_cost = cost_material(rows, width) + cost_read_materialized(rows);
  if (OB_FAIL(cost_local_order_sort_inner(types, rows, real_sort_cost))) {
    LOG_WARN("failed to calc cost", K(ret));
  } else {
    cost = material_cost + real_sort_cost;
    LOG_TRACE("OPT: [COST LOCAL ORDER SORT]", K(cost), K(material_cost), K(real_sort_cost),
              K(rows), K(width), K(types));
  }
  return ret;
}

/**
 * @brief        ??????topn???????????????????????????????????????
 *
 *    cost = cost_cmp * rows * log(n)
 */
int ObOptEstCostModel::cost_topn_sort_inner(const ObIArray<ObExprResType> &types,
																						double rows,
																						double n,
																						double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  if (OB_UNLIKELY(0.0 > rows)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid number of rows", K(rows), K(ret));
  } else if (n < 1.0) {
    // LOG2(x) ???x??????1???????????????????????????????????????
    cost = 0.0;
  } else {
    double cost_cmp = 0.0;
    if (OB_FAIL(get_sort_cmp_cost(types, cost_cmp))) {
      LOG_WARN("failed to get cmp cost", K(ret));
    } else if (OB_UNLIKELY(0.0 > cost_cmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("negative cost", K(cost_cmp), K(ret));
    } else {
        cost = cost_cmp * rows * LOG2(n);
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_exchange(const ObExchCostInfo &cost_info,
                                		 double &ex_cost)
{
  int ret = OB_SUCCESS;
  double ex_out_cost = 0.0;
  double ex_in_cost = 0.0;
  ObExchOutCostInfo out_est_cost_info(cost_info.rows_,
                                      cost_info.width_,
                                      cost_info.dist_method_,
                                      cost_info.out_parallel_,
                                      cost_info.in_server_cnt_);
  ObExchInCostInfo in_est_cost_info(cost_info.rows_,
                                    cost_info.width_,
                                    cost_info.dist_method_,
                                    cost_info.in_parallel_,
                                    cost_info.in_server_cnt_,
                                    cost_info.is_local_order_,
                                    cost_info.sort_keys_);
  if (OB_FAIL(ObOptEstCostModel::cost_exchange_out(out_est_cost_info, ex_out_cost))) {
    LOG_WARN("failed to cost exchange in output", K(ret));
  } else if (OB_FAIL(ObOptEstCostModel::cost_exchange_in(in_est_cost_info, ex_in_cost))) {
    LOG_WARN("failed to cost exchange in", K(ret));
  } else {
    ex_cost = ex_out_cost + ex_in_cost;
  }
  return ret;
}

int ObOptEstCostModel::cost_exchange_in(const ObExchInCostInfo &cost_info,
                                   			double &cost)
{
  int ret = OB_SUCCESS;
  double per_dop_rows = 0.0;
  ObSEArray<ObRawExpr*, 4> order_exprs;
  ObSEArray<ObExprResType, 4> order_types;
  cost = 0;
  if (OB_UNLIKELY(cost_info.parallel_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_expr_and_types(cost_info.sort_keys_,
                                                         order_exprs,
                                                         order_types))) {
    LOG_WARN("failed to get order expr and order types", K(ret));
  } else if (ObPQDistributeMethod::BC2HOST == cost_info.dist_method_) {
    per_dop_rows = cost_info.rows_ * cost_info.server_cnt_ / cost_info.parallel_;
  } else if (ObPQDistributeMethod::BROADCAST == cost_info.dist_method_) {
    per_dop_rows = cost_info.rows_;
  } else {
    per_dop_rows = cost_info.rows_ / cost_info.parallel_;
  }
  if (OB_SUCC(ret)) {
    cost = cost_params_.CPU_TUPLE_COST * per_dop_rows;
    cost += cost_params_.NETWORK_DESER_PER_BYTE_COST * per_dop_rows * cost_info.width_;
    if (ObPQDistributeMethod::BROADCAST == cost_info.dist_method_) {
      //????????????????????????????????????????????????????????????
      cost += ObOptEstCostModel::cost_material(per_dop_rows, cost_info.width_);
    }
    if (!cost_info.sort_keys_.empty() && per_dop_rows > 0) {
      double merge_degree = 0;
      double cmp_cost = 0.0;
      if (cost_info.is_local_order_) {
        cost += ObOptEstCostModel::cost_material(per_dop_rows, cost_info.width_);
        merge_degree = ObOptEstCostModel::DEFAULT_LOCAL_ORDER_DEGREE * cost_info.parallel_;
      } else {
        merge_degree = cost_info.parallel_;
      }
      if (merge_degree > per_dop_rows) {
        merge_degree = per_dop_rows;
      }
      if (OB_FAIL(get_sort_cmp_cost(order_types, cmp_cost))) {
        LOG_WARN("failed to get sort cmp cost", K(ret));
      } else {
        cost += per_dop_rows * LOG2(merge_degree) * cmp_cost;
      }
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_exchange_out(const ObExchOutCostInfo &cost_info,
                                    		 double &cost)
{
  int ret = OB_SUCCESS;
  double per_dop_rows = 0.0;
  cost = 0.0;
  if (OB_UNLIKELY(cost_info.parallel_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(cost_info.parallel_), K(ret));
  } else if (ObPQDistributeMethod::BC2HOST == cost_info.dist_method_ ||
             ObPQDistributeMethod::BROADCAST == cost_info.dist_method_) {
    per_dop_rows = cost_info.rows_ * cost_info.server_cnt_ / cost_info.parallel_;
  } else {
    per_dop_rows = cost_info.rows_ / cost_info.parallel_;
  }
  if (OB_SUCC(ret)) {
    // add repartition cost, hash-hash cost ?
    cost = cost_params_.CPU_TUPLE_COST * per_dop_rows;
    cost += cost_params_.NETWORK_SER_PER_BYTE_COST * per_dop_rows * cost_info.width_;
    cost += cost_params_.NETWORK_TRANS_PER_BYTE_COST * per_dop_rows * cost_info.width_;
  }
  return ret;
}
/**
 * @brief      ??????Merge Group By????????????????????????
 * @note       ????????????group by???????????????????????????????????????
 *
 * @formula    cost = CPU_TUPLE_COST *rows
 *                    + qual_cost(CMP_DEFAULT * num_group_columns * rows)
 *                    + PER_AGGR_FUNC_COST * num_aggr_columns * rows
 *
 * @param[in]  rows            ??????????????????
 * @param[in]  group_columns   group by?????????
 * @param[in]  agg_col_count   ?????????????????????
 * @return                     ?????????????????????
 */
double ObOptEstCostModel::cost_merge_group(double rows,
																					double res_rows,
																					double row_width,
																					const ObIArray<ObRawExpr *> &group_columns,
																					int64_t agg_col_count)
{
  double cost = 0.0;
  cost += cost_params_.CPU_TUPLE_COST * rows;
  //material cost
  cost += cost_material(res_rows, row_width);
  cost += cost_quals(rows, group_columns);
  cost += cost_params_.PER_AGGR_FUNC_COST * static_cast<double>(agg_col_count) * rows;
  LOG_TRACE("OPT: [COST MERGE GROUP BY]", K(cost), K(agg_col_count),
            K(rows), K(res_rows));
  return cost;
}

/**
 * @brief      ??????Hash Group By????????????????????????
 * @formula    cost = CPU_TUPLE_COST * rows
 *                    + BUILD_HASH_COST * res_rows
 *                    + PROBE_HASH_COST * rows
 *                    + hash_calculation_cost
 *                    + PER_AGGR_FUNC_COST * num_aggr_columns * rows
 * @param[in]  rows            ????????????
 * @param[in]  group_columns   group by??????
 * @param[in]  res_rows        ????????????
 * @param[in]  agg_col_count   ?????????????????????
 * @return                     ?????????????????????
 */
double ObOptEstCostModel::cost_hash_group(double rows,
																					double res_rows,
																					double row_width,
																					const ObIArray<ObRawExpr *> &group_columns,
																					int64_t agg_col_count)
{
  double cost = 0;
  cost += cost_params_.CPU_TUPLE_COST * rows;
  cost += cost_material(res_rows, row_width);
  cost += cost_params_.BUILD_HASH_PER_ROW_COST * res_rows;
  cost += cost_params_.PROBE_HASH_PER_ROW_COST * rows;
  cost += cost_hash(rows, group_columns);
  cost += cost_params_.PER_AGGR_FUNC_COST * static_cast<double>(agg_col_count) * rows;
  LOG_TRACE("OPT: [HASH GROUP BY]", K(cost), K(agg_col_count), K(rows), K(res_rows));
  return cost;
}

/**
 * @brief      ??????Scalar Group By????????????????????????
 * @formula    cost = PER_AGGR_FUNC_COST * num_aggr_columns * rows
 * @param[in]  rows            ??????????????????
 * @param[in]  agg_col_count   ?????????????????????
 * @return                     ?????????????????????
 */
double ObOptEstCostModel::cost_scalar_group(double rows, int64_t agg_col_count)
{
  double cost = 0.0;
  cost += cost_params_.CPU_TUPLE_COST * rows;
  cost += cost_params_.PER_AGGR_FUNC_COST * static_cast<double>(agg_col_count) * rows;
  LOG_TRACE("OPT: [SCALAR GROUP BY]", K(cost), K(agg_col_count), K(rows));
  return cost;
}

/**
 * @brief      ??????Merge Distinct ????????????????????????
 * @formula    cost = get_next_row_cost
 *                  + cost_quals
 * @param[in]  rows               ????????????
 * @param[in]  distinct_columns   distinct??????
 * @return                        ?????????????????????
 */
double ObOptEstCostModel::cost_merge_distinct(double rows,
																							double res_rows,
																							double width,
																							const ObIArray<ObRawExpr *> &distinct_columns)
{
  double cost = 0.0;
  cost += cost_params_.CPU_TUPLE_COST * rows;
  cost += cost_quals(rows, distinct_columns);
  LOG_TRACE("OPT: [COST MERGE DISTINCT]", K(cost), K(rows), K(res_rows));
  return cost;
}

/**
 * @brief        ??????Hash Distinct????????????????????????
 * @formula      cost = get_next_row_cost
 *                    + HASH_BUILD_COST * res_rows
 *                    + HASH_PROBE_COST * rows
 *                    + hash_calculation_cost
 * @param[in]    rows               ????????????
 * @param[in]    res_rows           ?????????????????????distinct???
 * @param[in]    distinct_columns   distinct???
 */
double ObOptEstCostModel::cost_hash_distinct(double rows,
																						double res_rows,
																						double width,
																						const ObIArray<ObRawExpr *> &distinct_columns)
{
  double cost = 0.0;
  // get_next_row()?????????
  cost += cost_params_.CPU_TUPLE_COST * rows;
  //material cost
  cost += cost_material(res_rows, width);
  // ??????hash table?????????
  cost += cost_params_.BUILD_HASH_PER_ROW_COST * res_rows;
  // probe?????????
  cost += cost_params_.PROBE_HASH_PER_ROW_COST * rows;
  // ??????hash?????????
  cost += cost_hash(rows, distinct_columns);

  LOG_TRACE("OPT: [COST HASH DISTINCT]", K(cost), K(rows), K(res_rows));
  return cost;
}

/**
 * @brief     ?????? Select ?????? Sequence ?????????????????????
 */
double ObOptEstCostModel::cost_sequence(double rows, double uniq_sequence_cnt)
{
  return cost_params_.CPU_TUPLE_COST * rows + cost_params_.CPU_OPERATOR_COST * uniq_sequence_cnt;
}
/**
 * @brief      ??????Limit????????????????????????
 * @formula    cost = rows * CPU_TUPLE_COST
 * @return     ?????????????????????
 */
double ObOptEstCostModel::cost_get_rows(double rows)
{
  return rows * cost_params_.CPU_TUPLE_COST;
}

/**
 * @brief      ????????????????????????????????????????????????
 */
double ObOptEstCostModel::cost_read_materialized(double rows)
{
  return rows * cost_params_.READ_MATERIALIZED_PER_ROW_COST;
}

/**
 * @brief      ??????Material????????????????????????
 * @formula    cost = MATERIALZE_PER_BYTE_COST * average_row_size * rows
 * @param[in]  rows             ?????????????????????
 * @param[in]  average_row_size ?????????????????????????????????
 * @return     ?????????????????????
 */
double ObOptEstCostModel::cost_material(const double rows, const double average_row_size)
{
  double cost = cost_params_.MATERIALIZE_PER_BYTE_WRITE_COST * average_row_size * rows;
  LOG_TRACE("OPT: [COST MATERIAL]", K(cost), K(rows), K(average_row_size));
  return cost;
}


double ObOptEstCostModel::cost_late_materialization_table_get(int64_t column_cnt)
{
  double op_cost = 0.0;
  double io_cost = cost_params_.MICRO_BLOCK_SEQ_COST;
  double cpu_cost = (cost_params_.CPU_TUPLE_COST
                         + cost_params_.PROJECT_COLUMN_SEQ_INT_COST * column_cnt);
  op_cost = io_cost + cpu_cost;
  return op_cost;
}

void ObOptEstCostModel::cost_late_materialization_table_join(double left_card,
																														double left_cost,
																														double right_card,
																														double right_cost,
																														double &op_cost,
																														double &cost)
{
  op_cost = 0.0;
  cost = 0.0;
  // ?????????????????????????????????????????????????????????????????????????????????????????????get_next_row????????????
  // ??????????????????????????????????????????????????????
  double once_rescan_cost = right_cost + right_card * cost_params_.CPU_TUPLE_COST;
  op_cost += left_card * once_rescan_cost + left_card * cost_params_.JOIN_PER_ROW_COST;
  // ?????????????????????get_next_row?????????
  cost += left_cost + ObOptEstCostModel::cost_params_.CPU_TUPLE_COST * left_card;
  cost += op_cost;
}

void ObOptEstCostModel::cost_late_materialization(double left_card,
																									double left_cost,
																									int64_t column_count,
																									double &cost)
{
  double op_cost = 0.0;
  double right_card = 1.0;
  double right_cost = cost_late_materialization_table_get(column_count);
  cost_late_materialization_table_join(left_card,
                                       left_cost,
                                       right_card,
                                       right_cost,
                                       op_cost,
                                       cost);
}

// entry point to estimate table cost
int ObOptEstCostModel::cost_table(ObCostTableScanInfo &est_cost_info,
																	int64_t parallel,
																	double query_range_row_count,
																	double phy_query_range_row_count,
																	double &cost,
																	double &index_back_cost)
{
  int ret = OB_SUCCESS;
  if (is_virtual_table(est_cost_info.ref_table_id_)) {
    if (OB_FAIL(cost_virtual_table(est_cost_info, cost))) {
      LOG_WARN("failed to estimate virtual table cost", K(ret));
    } else {
      index_back_cost = 0;
    }
  } else {
    if (OB_FAIL(cost_normal_table(est_cost_info,
                                  parallel,
                                  query_range_row_count,
                                  phy_query_range_row_count,
                                  cost,
                                  index_back_cost))) {
      LOG_WARN("failed to estimate table cost", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

// estimate cost for virtual table
int ObOptEstCostModel::cost_virtual_table(ObCostTableScanInfo &est_cost_info,
                                     			double &cost)
{
  int ret = OB_SUCCESS;
  if (0 == est_cost_info.ranges_.count()) {
    cost = VIRTUAL_INDEX_GET_COST * static_cast<double>(OB_EST_DEFAULT_VIRTUAL_TABLE_ROW_COUNT);
    LOG_TRACE("OPT:[VT] virtual table without range, use default stat", K(cost));
  } else {
    cost = VIRTUAL_INDEX_GET_COST * static_cast<double>(est_cost_info.ranges_.count());
    // refine the cost if it is not exact match
    if (!est_cost_info.is_unique_) {
      cost *= 100.0;
    }
    LOG_TRACE("OPT:[VT] virtual table with range, init est", K(cost),
               K(est_cost_info.ranges_.count()));
  }
  return ret;
}

// estimate cost for real table
// 1. ??????filter?????????
// 2. ??????????????????????????????
// 3. ??????key ranges, ????????????ObBatch
// 4. ???????????????ObBatch??????
// 5. ???????????????ObBatch??????
// 6. ????????????????????????
int ObOptEstCostModel::cost_normal_table(ObCostTableScanInfo &est_cost_info,
																				int64_t parallel,
																				const double query_range_row_count,
																				const double phy_query_range_row_count,
																				double &cost,
																				double &index_back_cost)

{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOptEstCostModel::cost_table_one_batch(est_cost_info,
                                                 parallel,
                                                 est_cost_info.batch_type_,
                                                 query_range_row_count,
                                                 phy_query_range_row_count,
                                                 cost,
                                                 index_back_cost))) {
    LOG_WARN("Failed to estimate cost", K(ret), K(est_cost_info));
  } else {
    LOG_TRACE("OPT:[ESTIMATE FINISH]", K(cost), K(index_back_cost),
              K(phy_query_range_row_count), K(query_range_row_count),
              K(est_cost_info));
  }
  return ret;
}

int ObOptEstCostModel::cost_table_one_batch(const ObCostTableScanInfo &est_cost_info,
																						const int64_t parallel,
																						const ObSimpleBatch::ObBatchType &type,
																						const double logical_row_count,
																						const double physical_row_count,
																						double &cost,
																						double &index_back_cost)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(logical_row_count < 0.0) || OB_UNLIKELY(parallel < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected error", K(logical_row_count), K(parallel), K(ret));
  } else {
    int64_t part_cnt = est_cost_info.index_meta_info_.index_part_count_;
    double per_part_log_cnt = logical_row_count / part_cnt;
    double per_part_phy_cnt = physical_row_count / part_cnt;
    if (ObSimpleBatch::T_GET == type || ObSimpleBatch::T_MULTI_GET == type) {
      if (OB_FAIL(cost_table_get_one_batch(est_cost_info,
                                           per_part_log_cnt,
                                           cost,
                                           index_back_cost))) {
        LOG_WARN("Failed to estimate get cost", K(ret));
      } else {
        cost = cost * part_cnt / parallel;
        index_back_cost = index_back_cost * part_cnt / parallel;
      }
    } else if (ObSimpleBatch::T_SCAN == type || ObSimpleBatch::T_MULTI_SCAN == type) {
      if (OB_FAIL(cost_table_scan_one_batch(est_cost_info,
                                            per_part_log_cnt,
                                            per_part_phy_cnt,
                                            cost,
                                            index_back_cost))) {
        LOG_WARN("Failed to estimate scan cost", K(ret));
      } else {
        cost = cost * part_cnt / parallel;
        index_back_cost = index_back_cost * part_cnt / parallel;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid batch type", K(ret), K(type));
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_table_get_one_batch(const ObCostTableScanInfo &est_cost_info,
																								const double output_row_count,
																								double &cost,
																								double &index_back_cost)
{
  int ret = OB_SUCCESS;
  const ObTableMetaInfo *table_meta_info = est_cost_info.table_meta_info_;
  const ObIndexMetaInfo &index_meta_info = est_cost_info.index_meta_info_;
  bool is_index_back = index_meta_info.is_index_back_;
  if (OB_ISNULL(table_meta_info) ||
      OB_UNLIKELY(output_row_count < 0) ||
      OB_UNLIKELY(est_cost_info.postfix_filter_sel_ < 0) ||
      OB_UNLIKELY(est_cost_info.postfix_filter_sel_ > 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(output_row_count),
      K_(est_cost_info.postfix_filter_sel), K(ret));
  } else {
    double get_cost = 0.0;
    double get_index_back_cost = 0.0;
    if (is_index_back) {
      double base_cost = 0.0;
      double ib_cost = 0.0;
      double network_cost = 0.0;
      //????????????
      if (OB_FAIL(cost_table_get_one_batch_inner(output_row_count,
                                                 est_cost_info,
                                                 true,
                                                 base_cost))) {
        LOG_WARN("failed to calc cost", K(output_row_count), K(ret));
      } else {
        double index_back_row_count = output_row_count;
        // revise number of output row if is row sample scan
        if (est_cost_info.sample_info_.is_row_sample()) {
          index_back_row_count *= 0.01 * est_cost_info.sample_info_.percent_;
        }
        LOG_TRACE("OPT:[COST GET]", K(index_back_row_count));

        //????????????get?????????????????????
        index_back_row_count = index_back_row_count * est_cost_info.postfix_filter_sel_;
        if (OB_FAIL(cost_table_get_one_batch_inner(index_back_row_count,
                                                   est_cost_info,
                                                   false,
                                                   ib_cost))) {
          LOG_WARN("failed to calc cost", K(index_back_row_count), K(ret));
        } else if (index_meta_info.is_global_index_ &&
                   OB_FAIL(cost_table_lookup_rpc(index_back_row_count,
                                                est_cost_info,
                                                network_cost))) {
          LOG_WARN("failed to get newwork transform cost for global index", K(ret));
        } else {
          get_cost = base_cost + ib_cost + network_cost;
          get_index_back_cost = ib_cost + network_cost;
          LOG_TRACE("OPT:[COST GET]", K(output_row_count), K(index_back_row_count),
                    K(get_cost), K(base_cost), K(ib_cost), K(network_cost),
                    K_(est_cost_info.postfix_filter_sel));
        }
      }
    } else {
      bool is_scan_index = est_cost_info.index_meta_info_.index_id_ != est_cost_info.index_meta_info_.ref_table_id_;
      if (OB_FAIL(cost_table_get_one_batch_inner(output_row_count,
                                                 est_cost_info,
                                                 is_scan_index,
                                                 get_cost))) {
        LOG_WARN("failed to calc cost", K(output_row_count), K(ret));
      } else {
        LOG_TRACE("OPT:[COST GET]", K(output_row_count), K(get_cost));
      }
    }
    cost = get_cost;
    index_back_cost = get_index_back_cost;
  }
  return ret;
}

int ObOptEstCostModel::cost_table_scan_one_batch(const ObCostTableScanInfo &est_cost_info,
																								const double logical_output_row_count,
																								const double physical_output_row_count,
																								double &cost,
																								double &index_back_cost)
{
  int ret = OB_SUCCESS;
  const ObTableMetaInfo *table_meta_info = est_cost_info.table_meta_info_;
  const ObIndexMetaInfo &index_meta_info = est_cost_info.index_meta_info_;
  bool is_index_back = index_meta_info.is_index_back_;
  if (OB_ISNULL(table_meta_info) ||
      OB_UNLIKELY(index_meta_info.index_column_count_ <= 0) ||
      OB_UNLIKELY(logical_output_row_count < 0) ||
      OB_UNLIKELY(physical_output_row_count < 0) ||
      OB_UNLIKELY(est_cost_info.postfix_filter_sel_ < 0) ||
      OB_UNLIKELY(est_cost_info.postfix_filter_sel_ > 1) ||
      OB_UNLIKELY(table_meta_info->table_rowkey_count_ < 0) ||
      OB_UNLIKELY(table_meta_info->table_row_count_ <= 0) ||
      OB_UNLIKELY(table_meta_info->part_count_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K_(index_meta_info.index_column_count),
        K(logical_output_row_count), K_(est_cost_info.postfix_filter_sel),
        K(table_meta_info->table_rowkey_count_), K(table_meta_info->table_row_count_),
        K(table_meta_info->part_count_), K(physical_output_row_count), K(ret));
  } else {
    double scan_cost = 0.0;
    double scan_index_back_cost = 0.0;
    if (is_index_back) {
      double base_cost = 0.0;
      double ib_cost = 0.0;
      double network_cost = 0.0;
      if (OB_FAIL(cost_table_scan_one_batch_inner(physical_output_row_count,
                                                  est_cost_info,
                                                  true,
                                                  base_cost))) {
        LOG_WARN("Failed to calc scan scan_cost", K(ret));
      } else {
        double index_back_row_count = logical_output_row_count;
        // revise number of output row if is row sample scan
        if (est_cost_info.sample_info_.is_row_sample()) {
          index_back_row_count *= 0.01 * est_cost_info.sample_info_.percent_;
        }
        LOG_TRACE("OPT:[COST SCAN SIMPLE ROW COUNT]", K(index_back_row_count));
        index_back_row_count = index_back_row_count * est_cost_info.postfix_filter_sel_;
        if (OB_FAIL(cost_table_get_one_batch_inner(index_back_row_count,
                                                   est_cost_info,
                                                   false,
                                                   ib_cost))) {
          LOG_WARN("Failed to calc get scan_cost", K(ret));
        } else if (index_meta_info.is_global_index_ &&
                   OB_FAIL(cost_table_lookup_rpc(index_back_row_count,
                                                est_cost_info,
                                                network_cost))) {
          LOG_WARN("failed to get newwork transform scan_cost for global index", K(ret));
        } else {
          scan_cost = base_cost + ib_cost + network_cost;
          scan_index_back_cost = ib_cost + network_cost;
          LOG_TRACE("OPT:[COST SCAN]", K(logical_output_row_count), K(index_back_row_count),
                    K(scan_cost), K(base_cost), K(ib_cost),K(network_cost),
                    "postfix_sel", est_cost_info.postfix_filter_sel_);
        }
      }
    } else {
      bool is_scan_index = est_cost_info.index_meta_info_.index_id_ != est_cost_info.index_meta_info_.ref_table_id_;
      if (OB_FAIL(cost_table_scan_one_batch_inner(physical_output_row_count,
                                                  est_cost_info,
                                                  is_scan_index,
                                                  scan_cost))) {
        LOG_WARN("Failed to calc scan cost", K(ret));
      } else {
        LOG_TRACE("OPT:[COST SCAN]", K(logical_output_row_count), K(scan_cost));
      }
    }
    if (OB_SUCC(ret)) {
      cost = scan_cost;
      index_back_cost = scan_index_back_cost;
    }
  }
  return ret;
}

/**
 * ??????TableScan?????????
 * formula: cost     = io_cost + memtable_cost + memtable_merge_cost + cpu_cost
 *          io_cost  = MICRO_BLOCK_SEQ_COST * num_micro_blocks
 *                     + PROJECT_COLUMN_SEQ_COST * num_column * num_rows
 *          cpu_cost = qual_cost + num_rows * CPU_TUPLE_COST
 */
int ObOptEstCostModel::cost_table_scan_one_batch_inner(double row_count,
																											const ObCostTableScanInfo &est_cost_info,
																											bool is_scan_index,
																											double &cost)
{
  int ret = OB_SUCCESS;
  double project_cost = 0.0;
  const ObIndexMetaInfo &index_meta_info = est_cost_info.index_meta_info_;
  const ObTableMetaInfo *table_meta_info = est_cost_info.table_meta_info_;
  bool is_index_back = index_meta_info.is_index_back_;
  
  if (OB_ISNULL(table_meta_info) ||
      OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(row_count), K(ret));
  } else if ((!is_scan_index || !is_index_back) &&
             OB_FAIL(cost_full_table_scan_project(row_count,
                                                  est_cost_info,
                                                  project_cost))) {
    LOG_WARN("failed to cost project", K(ret));
  } else if (is_scan_index &&
             is_index_back &&
             OB_FAIL(cost_project(row_count,
                                  est_cost_info.index_access_column_items_,
                                  true,
                                  project_cost))) {
    LOG_WARN("failed to cost project", K(ret));
  } else {
    //????????????????????? = ?????????/????????????
    double num_micro_blocks = index_meta_info.get_micro_block_numbers();
    //???????????? = ???????????? * ????????????
    double num_micro_blocks_read = 0;
    if (OB_LIKELY(table_meta_info->table_row_count_ > 0) &&
        row_count <= table_meta_info->table_row_count_) {
      num_micro_blocks_read = std::ceil(num_micro_blocks
                                        * row_count
                                        / static_cast<double> (table_meta_info->table_row_count_));
    } else {
      num_micro_blocks_read = num_micro_blocks;
    }

    // revise number of rows if is row sample scan
    // ??????????????????????????????????????????????????????????????????
    if (est_cost_info.sample_info_.is_row_sample()) {
      row_count *= 0.01 * est_cost_info.sample_info_.percent_;
    }

    // IO??????????????????????????????????????????????????????????????????
    double io_cost = 0.0;
    double first_block_cost = cost_params_.MICRO_BLOCK_RND_COST;
    double rows_in_one_block = static_cast<double> (table_meta_info->table_row_count_) / num_micro_blocks;
    rows_in_one_block = rows_in_one_block <= 1 ? 1.000001 : rows_in_one_block;
    if (!est_cost_info.pushdown_prefix_filters_.empty()) {
      if (est_cost_info.can_use_batch_nlj_) {
        first_block_cost = cost_params_.BATCH_NL_SCAN_COST;
      } else {
        first_block_cost = cost_params_.NL_SCAN_COST;
      }
    }
    if (num_micro_blocks_read < 1) {
      io_cost = first_block_cost;
    } else {
      io_cost = first_block_cost + cost_params_.MICRO_BLOCK_SEQ_COST * (num_micro_blocks_read-1);
    }

    // ????????????????????????filter?????????
    double qual_cost = 0.0;
    if (!is_index_back) {
      // ????????????
      ObSEArray<ObRawExpr*, 8> filters;
      if (OB_FAIL(append(filters, est_cost_info.postfix_filters_)) ||
          OB_FAIL(append(filters, est_cost_info.table_filters_))) {
        LOG_WARN("failed to append fiilters", K(ret));
      } else {
        qual_cost += cost_quals(row_count, filters);
      }
    } else {
      // ????????????
      qual_cost += cost_quals(row_count, est_cost_info.postfix_filters_);
    }
    // CPU???????????????get_next_row??????????????????????????????
    double cpu_cost = row_count * cost_params_.CPU_TUPLE_COST
                      + qual_cost;
    // ???memtable?????????????????????????????????
    double memtable_cost = 0;
    // memtable????????????????????????????????????????????????
    double memtable_merge_cost = 0;
    //????????????????????????????????????????????????IO???CPU??????????????????
    double scan_cpu_cost = row_count * cost_params_.TABLE_SCAN_CPU_TUPLE_COST + project_cost;
    cpu_cost += scan_cpu_cost;
    if (io_cost > cpu_cost) {
        cost = io_cost + memtable_cost + memtable_merge_cost;
    } else {
        cost = cpu_cost + memtable_cost + memtable_merge_cost;
    }

    LOG_TRACE("OPT:[COST TABLE SCAN INNER]", K(num_micro_blocks), K(table_meta_info->table_row_count_),
              K(cost), K(io_cost), K(cpu_cost), K(memtable_cost), K(memtable_merge_cost), K(qual_cost),
              K(project_cost), K(num_micro_blocks_read), K(row_count));
  }
  return ret;
}

/*
 * estimate the network transform and rpc cost for global index,
 * so far, this cost model should be revised by banliu
 */
int ObOptEstCostModel::cost_table_lookup_rpc(double row_count,
																						const ObCostTableScanInfo &est_cost_info,
																						double &cost)
{
  int ret = OB_SUCCESS;
  const ObTableMetaInfo *table_meta_info = est_cost_info.table_meta_info_;
  cost = 0.0;
  if (OB_ISNULL(table_meta_info) ||
      OB_UNLIKELY(table_meta_info->table_column_count_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table column count should not be 0", K(table_meta_info->table_column_count_), K(ret));
  } else if (est_cost_info.index_meta_info_.is_global_index_) {
    double column_count = est_cost_info.access_column_items_.count();
    double transform_size = (table_meta_info->average_row_size_ * row_count * column_count)
                            /static_cast<double>(table_meta_info->table_column_count_);
    cost = transform_size * cost_params_.NETWORK_TRANS_PER_BYTE_COST +
            row_count * cost_params_.TABLE_LOOPUP_PER_ROW_RPC_COST;
  } else { /*do nothing*/ }
  LOG_TRACE("OPT::[COST_TABLE_GET_NETWORK]", K(cost), K(ret), K(table_meta_info->average_row_size_),
            K(row_count), K(table_meta_info->table_column_count_));
  return ret;
}

/**
 * ??????TableGet????????????
 * formula: cost     = io_cost + memtable_cost + memtable_merge_cost + cpu_cost
 *          io_cost  = MICRO_BLOCK_RND_COST * num_micro_blocks_read
 *                     + num_rows * (FETCH_ROW_RND_COST + PROJECT_COLUMN_RND_COST * num_columns)
 *          cpu_cost = qual_cost + num_rows * (CPU_TUPLE_COST + project_cost)
 */
int ObOptEstCostModel::cost_table_get_one_batch_inner(double row_count,
																											const ObCostTableScanInfo &est_cost_info,
																											bool is_scan_index,
																											double &cost)
{
  int ret = OB_SUCCESS;
  double project_cost = 0.0;
  const ObIndexMetaInfo &index_meta_info = est_cost_info.index_meta_info_;
  const ObTableMetaInfo *table_meta_info = est_cost_info.table_meta_info_;
  bool is_index_back = index_meta_info.is_index_back_;
  if (OB_ISNULL(table_meta_info) ||
      OB_UNLIKELY(row_count < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", K(row_count), K(ret));
  } else if ((!is_scan_index || !is_index_back) &&
             OB_FAIL(cost_project(row_count,
                                  est_cost_info.access_column_items_,
                                  false,
                                  project_cost))) {
    LOG_WARN("failed to cost project", K(ret));
  } else if (is_scan_index &&
             is_index_back &&
             OB_FAIL(cost_project(row_count,
                                  est_cost_info.index_access_column_items_,
                                  false,
                                  project_cost))) {
    LOG_WARN("failed to cost project", K(ret));
  } else {
    //????????????????????????
    double num_micro_blocks = 0;
    if (is_index_back && !is_scan_index) {
      num_micro_blocks = table_meta_info->get_micro_block_numbers();
    } else {
      num_micro_blocks = index_meta_info.get_micro_block_numbers();
    }
    double num_micro_blocks_read = 0;
    if (OB_LIKELY(table_meta_info->table_row_count_ > 0) &&
        row_count <= table_meta_info->table_row_count_) {
      double table_row_count = static_cast<double>(table_meta_info->table_row_count_);
      num_micro_blocks_read = num_micro_blocks * (1.0 - std::pow((1.0 - row_count / table_row_count), table_row_count / num_micro_blocks));
      num_micro_blocks_read = std::ceil(num_micro_blocks_read);
    } else {
      num_micro_blocks_read = num_micro_blocks;
    }

    // IO???????????????????????????????????????????????????????????????????????????????????????
    double rows_in_one_block = static_cast<double> (table_meta_info->table_row_count_) / num_micro_blocks;
    rows_in_one_block = rows_in_one_block <= 1 ? 1.000001 : rows_in_one_block;
    double first_block_cost = cost_params_.MICRO_BLOCK_RND_COST;
    if (est_cost_info.is_inner_path_) {
      if (est_cost_info.can_use_batch_nlj_) {
        first_block_cost = cost_params_.BATCH_NL_GET_COST;
      } else {
        first_block_cost = cost_params_.NL_GET_COST;
      }
    }
    double io_cost = 0;
    if (num_micro_blocks_read < 1) {
      io_cost = 0;
    } else {
      io_cost = first_block_cost + cost_params_.MICRO_BLOCK_RND_COST * (num_micro_blocks_read-1);
    }
    double fetch_row_cost = cost_params_.FETCH_ROW_RND_COST * row_count;
    io_cost += fetch_row_cost;

    // revise number of rows if is row sample scan
    // ??????????????????????????????????????????????????????????????????
    if (est_cost_info.sample_info_.is_row_sample()) {
      row_count *= 0.01 * est_cost_info.sample_info_.percent_;
    }

    // ????????????????????????filter?????????
    // TODO shengle  ?????????????????????????????????, ????????????????????????????????????filter???????????????, ????????????????????????
    double qual_cost = 0.0;
    if (!is_index_back) {
      // ????????????
      ObSEArray<ObRawExpr*, 8> filters;
      if (OB_FAIL(append(filters, est_cost_info.postfix_filters_)) ||
          OB_FAIL(append(filters, est_cost_info.table_filters_))) {
        LOG_WARN("failed to append fiilters", K(ret));
      } else {
        qual_cost += cost_quals(row_count, filters);
      }
    } else {
      // ????????????
      if (!is_scan_index) {
        // ??????
        qual_cost += cost_quals(row_count, est_cost_info.table_filters_);
      } else {
        qual_cost += cost_quals(row_count, est_cost_info.postfix_filters_);
      }
    }
    // CPU???????????????get_next_row??????????????????????????????
    double cpu_cost = row_count * cost_params_.CPU_TUPLE_COST
                      + qual_cost;
    // ???memtable?????????????????????????????????
    double memtable_cost = 0;
    // memtable????????????????????????????????????????????????
    double memtable_merge_cost = 0;
    //????????????????????????????????????????????????IO???CPU??????????????????
    double scan_cpu_cost = row_count * cost_params_.TABLE_SCAN_CPU_TUPLE_COST + project_cost;
    cost = io_cost + scan_cpu_cost + cpu_cost + memtable_cost + memtable_merge_cost;
    LOG_TRACE("OPT:[COST TABLE GET INNER]", K(cost), K(io_cost), K(cpu_cost), K(fetch_row_cost),
              K(qual_cost), K(memtable_cost), K(memtable_merge_cost), K(num_micro_blocks_read),
              K(row_count));
  }
  return ret;
}

int ObOptEstCostModel::get_sort_cmp_cost(const common::ObIArray<sql::ObExprResType> &types,
                                   			 double &cost)
{
  int ret = OB_SUCCESS;
  double cost_ret = 0.0;
  if (OB_UNLIKELY(types.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid col count", "col count", types.count(), K(ret));
  } else {
    double factor = 1.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < types.count(); ++i) {
      ObObjTypeClass tc = types.at(i).get_type_class();
      if (OB_UNLIKELY(tc >= ObMaxTC)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("not supported type class", K(tc), K(ret));
      } else {
        //Correctly estimating cmp cost need NDVs of each sort col:
        //  if first col is identical, then we needn't compare the second col and so on.
        //But now we cannot get hand on NDV easily, just use
        //  cmp_cost_col0 + cmp_cost_col1 / DEF_NDV + cmp_cost_col2 / DEF_NDV^2 ...
        double cost_for_col = comparison_params_[tc];
        if (OB_UNLIKELY(cost_for_col < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not supported type class", K(tc), K(ret));
        } else {
          cost_ret += cost_for_col * factor;
          factor /= 10.0;
        }
      }
    }
    if (OB_SUCC(ret)) {
      cost = cost_ret;
    }
  }
  return ret;
}

int ObOptEstCostModel::cost_window_function(double rows, double width, double win_func_cnt, double &cost)
{
  int ret = OB_SUCCESS;
  cost += rows * cost_params_.CPU_TUPLE_COST;
  cost += ObOptEstCostModel::cost_material(rows, width) + ObOptEstCostModel::cost_read_materialized(rows);
  cost += rows * cost_params_.PER_WIN_FUNC_COST * win_func_cnt;
  return ret;
}

/**
 * @brief     ??????filter?????????
 * @formula   cost = rows * CPU_TUPLE_COST + cost_quals
 * @param[in] rows        ????????????
 * @param[in] filters filter??????
 * @return    ????????????
 */
double ObOptEstCostModel::cost_filter_rows(double rows, ObIArray<ObRawExpr*> &filters)
{
  return rows * cost_params_.CPU_TUPLE_COST + cost_quals(rows, filters);
}

/**
 *  @brief   ??????SubplanFilter?????????
 *
 *  @formula ?????????????????????????????????????????????????????????filter???filter??????3????????????
 *           1. onetime expr??????????????????filter?????????????????????????????????????????????
 *           2. initplan    : ???????????????filter??????????????????????????????????????????????????????????????????
 *           3. ??????         : ???????????????filter????????????????????????
 */
int ObOptEstCostModel::cost_subplan_filter(const ObSubplanFilterCostInfo &info,
                                           double &cost)
{
  int ret = OB_SUCCESS;
  cost = 0.0;
  double onetime_cost = 0.0;
  if (info.children_.count() > 0) {
    cost += info.children_.at(0).rows_ * cost_params_.CPU_TUPLE_COST;
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < info.children_.count(); ++i) {
    const ObBasicCostInfo &child = info.children_.at(i);
    //???????????????onetime expr;
    if (info.onetime_idxs_.has_member(i)) { // onetime cost
      // ????????????????????????onetime expr
      // ???????????????????????????????????????????????????
      onetime_cost += child.cost_;
    } else if (info.initplan_idxs_.has_member(i)) { // init plan cost
      // ????????????????????????initplan
      // ?????????????????????????????????????????????????????????
      onetime_cost += child.cost_ + child.rows_ * cost_params_.CPU_TUPLE_COST
          + cost_material(child.rows_, child.width_);
      cost += info.children_.at(0).rows_ * cost_read_materialized(child.rows_);
    } else { // other cost
      // ??????????????????????????????????????????
      cost += info.children_.at(0).rows_ * (child.cost_ + child.rows_ * cost_params_.CPU_TUPLE_COST);
      if (child.exchange_allocated_) {
        cost += cost_params_.PX_RESCAN_PER_ROW_COST * info.children_.at(0).rows_;
      }
    }
  } // for info_childs end

  if (OB_SUCC(ret)) {
    cost += onetime_cost;
    LOG_TRACE("OPT: [COST SUBPLAN FILTER]", K(cost), K(onetime_cost), K(info));
  }
  return ret;
}

int ObOptEstCostModel::cost_union_all(const ObCostMergeSetInfo &info, double &cost)
{
  int ret = OB_SUCCESS;
  double total_rows = 0.0;
  for (int64_t i = 0; i < info.children_.count(); ++i) {
    total_rows += info.children_.at(i).rows_;
  }
  cost = total_rows * cost_params_.CPU_TUPLE_COST;
  return ret;
}

/**
 * @brief ????????????????????????????????????union / except / intersect???
 * @param[in]  info    ????????????????????????????????????????????????
 * @param[out] cost ?????????????????????????????????????????????
 * ??????merge set???????????????set op??????????????????????????????????????????????????????
 */
int ObOptEstCostModel::cost_merge_set(const ObCostMergeSetInfo &info, double &cost)
{
  int ret = OB_SUCCESS;
  double sum_rows = 0;
  double width = 0.0;
  for (int64_t i = 0; i < info.children_.count(); ++i) {
    sum_rows += info.children_.at(i).rows_;
    width = info.children_.at(i).width_;
  }
  cost = 0.0;
  //get next row cost
  cost += sum_rows * cost_params_.CPU_TUPLE_COST;
  cost += cost_material(sum_rows, width);
  //operator cost???cmp_cost + cpu_cost
  LOG_TRACE("OPT: [COST MERGE SET]", K(cost), K(sum_rows), K(width));
  return ret;
}

/**
 * @brief ????????????????????????????????????union / except / intersect???
 * @param[in]  info    ????????????????????????????????????????????????
 * @param[out] cost    ?????????????????????????????????????????????
 * ??????hash set???????????????set op?????????????????????????????????????????????????????????
 */
int ObOptEstCostModel::cost_hash_set(const ObCostHashSetInfo &info, double &cost)
{
  int ret = OB_SUCCESS;
  double build_rows = 0.0;
  double probe_rows = 0.0;
  if (ObSelectStmt::UNION == info.op_) {
    build_rows = info.left_rows_ + info.right_rows_;
    probe_rows = info.left_rows_ + info.right_rows_;
  } else if (ObSelectStmt::INTERSECT == info.op_) {
    build_rows = info.left_rows_;
    probe_rows = info.left_rows_ + info.right_rows_;
  } else if (ObSelectStmt::EXCEPT == info.op_) {
    build_rows = info.left_rows_;
    probe_rows = info.left_rows_ + info.right_rows_;
  }

  cost = 0.0;
  //get_next_row() ??????
  cost += cost_params_.CPU_TUPLE_COST * (info.left_rows_ + info.right_rows_);
  //material cost
  cost += cost_material(info.left_rows_, info.left_width_) +
             cost_material(info.right_rows_, info.right_width_);
  //build hash table cost
  cost += cost_params_.BUILD_HASH_PER_ROW_COST * build_rows;
  //probe hash table cost
  cost += cost_params_.PROBE_HASH_PER_ROW_COST * probe_rows;
  //?????? hash ?????????
  cost += cost_hash(info.left_rows_ + info.right_rows_, info.hash_columns_);

  LOG_TRACE("OPT: [COST HASH SET]", K(cost));
  return ret;
}


/**
 * @brief                 ??????hash????????????
 * @note(@ banliu.zyd)    ????????????????????????hash???????????????????????????????????????????????????????????????
 *                        ???????????????hash??????????????????????????????????????????????????????????????????????????????
 *                        ???????????????????????????????????????????????????????????????????????????????????????
 * @param[in] rows        ????????????
 * @param[in] hash_exprs  hash?????????
 *
 */
double ObOptEstCostModel::cost_hash(double rows, const ObIArray<ObRawExpr *> &hash_exprs)
{
  double cost_per_row = 0.0;
  for (int64_t i = 0; i < hash_exprs.count(); ++i) {
    const ObRawExpr *expr = hash_exprs.at(i);
    if (OB_ISNULL(expr)) {
      LOG_WARN("qual should not be NULL, but we don't set error return code here, just skip it");
    } else {
      ObObjTypeClass calc_type = expr->get_result_type().get_calc_type_class();
      if (OB_UNLIKELY(hash_params_[calc_type] < 0)) {
        LOG_WARN("hash type not supported, skipped", K(calc_type));
      } else {
        cost_per_row += hash_params_[calc_type];
      }
    }
  }
  return rows * cost_per_row;
}

int ObOptEstCostModel::cost_project(double rows,
																		const ObIArray<ColumnItem> &columns,
																		bool is_seq,
																		double &cost)
{
  int ret = OB_SUCCESS;
  double project_one_row_cost = 0.0;
  for (int i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ColumnItem &column_item = columns.at(i);
    ObRawExpr *expr = column_item.expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->get_ref_count() <= 0) {
      //do nothing
    } else {
      const ObExprResType &type = expr->get_result_type();
      if (type.is_integer_type()) {
        // int
        if (is_seq) {
          project_one_row_cost += cost_params_.PROJECT_COLUMN_SEQ_INT_COST;
        } else {
          project_one_row_cost += cost_params_.PROJECT_COLUMN_RND_INT_COST;
        }
      } else if (type.get_accuracy().get_length() > 0) {
        // ObStringTC
        int64_t string_width = type.get_accuracy().get_length();
        string_width = std::min(string_width, ObOptEstCostModel::DEFAULT_MAX_STRING_WIDTH);
        if (is_seq) {
          project_one_row_cost += cost_params_.PROJECT_COLUMN_SEQ_CHAR_COST * string_width;
        } else {
          project_one_row_cost += cost_params_.PROJECT_COLUMN_RND_CHAR_COST * string_width;
        }
      } else if (type.get_accuracy().get_precision() > 0 || type.is_oracle_integer()) {
        // number, time
        if (is_seq) {
          project_one_row_cost += cost_params_.PROJECT_COLUMN_SEQ_NUMBER_COST;
        } else {
          project_one_row_cost += cost_params_.PROJECT_COLUMN_RND_NUMBER_COST;
        }
      } else {
        // default for DEFAULT PK
        if (is_seq) {
          project_one_row_cost += cost_params_.PROJECT_COLUMN_SEQ_INT_COST;
        } else {
          project_one_row_cost += cost_params_.PROJECT_COLUMN_RND_INT_COST;
        }
      }
    }
  }
  cost = project_one_row_cost * rows;
  LOG_TRACE("COST PROJECT:", K(cost), K(rows), K(columns));
  return ret;
}

int ObOptEstCostModel::cost_full_table_scan_project(double rows, 
                                                    const ObCostTableScanInfo &est_cost_info, 
                                                    double &cost)
{
  int ret = OB_SUCCESS;
  double cost_project_filter_column = 0;
  double project_one_row_cost = 0;
  double project_full_row_count = rows * est_cost_info.table_filter_sel_
                                       * est_cost_info.join_filter_sel_;
  ObSEArray<ObRawExpr*, 4> filter_columns;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(est_cost_info.postfix_filters_, 
                                                  filter_columns))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(est_cost_info.table_filters_, 
                                                         filter_columns))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_FAIL(cost_project(project_full_row_count, 
                                  est_cost_info.access_column_items_, 
                                  true,
                                  cost))) {
    LOG_WARN("failed to calc project cost", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < filter_columns.count(); ++i) {
    ObRawExpr *expr = filter_columns.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->get_ref_count() <= 0) {
      //do nothing
    } else {
      const ObExprResType &type = expr->get_result_type();
      if (type.is_integer_type()) {
        // int
        project_one_row_cost += cost_params_.PROJECT_COLUMN_SEQ_INT_COST;
      } else if (type.get_accuracy().get_length() > 0) {
        // ObStringTC
        int64_t string_width = type.get_accuracy().get_length();
        string_width = std::min(string_width, ObOptEstCostModel::DEFAULT_MAX_STRING_WIDTH);
        project_one_row_cost += cost_params_.PROJECT_COLUMN_SEQ_CHAR_COST * string_width;
      } else if (type.get_accuracy().get_precision() > 0 || type.is_oracle_integer()) {
        // number, time
        project_one_row_cost += cost_params_.PROJECT_COLUMN_SEQ_NUMBER_COST;
      } else {
        // default for DEFAULT PK
        project_one_row_cost += cost_params_.PROJECT_COLUMN_SEQ_INT_COST;
      }
    }
  }
  if (OB_SUCC(ret)) {
    cost_project_filter_column = project_one_row_cost * rows;
    cost += cost_project_filter_column;
    LOG_TRACE("COST TABLE SCAN PROJECT:", K(rows), K(project_full_row_count), 
                                      K(cost_project_filter_column), K(cost));
  }
  return ret;
}

/**
 * @brief              ???????????????????????????
 * @note(@ banliu.zyd) ?????????????????????????????????????????????????????????????????????????????????????????????
 *                     ??????????????????????????????????????????????????????????????????????????????????????????
 *                     ???????????????????????????????????????????????????????????????????????????????????????
 * @param[in] rows     ????????????
 * @param[in] quals    ????????????
 *
 */
// ???????????? = ?????? * sum(?????????????????????????????????)
double ObOptEstCostModel::cost_quals(double rows, const ObIArray<ObRawExpr *> &quals, bool need_scale)
{
  double factor = 1.0;
  double cost_per_row = 0.0;
  for (int64_t i = 0; i < quals.count(); ++i) {
    const ObRawExpr *qual = quals.at(i);
    if (OB_ISNULL(qual)) {
      LOG_WARN("qual should not be NULL, but we don't set error return code here, just skip it");
    } else {
      ObObjTypeClass calc_type = qual->get_result_type().get_calc_type_class();
      if (OB_UNLIKELY(comparison_params_[calc_type] < 0)) {
        LOG_WARN("comparison type not supported, skipped", K(calc_type));
      } else {
        cost_per_row += comparison_params_[calc_type] * factor;
        if (need_scale) {
          factor /= 10.0;
        }
      }
    }
  }
  return rows * cost_per_row;
}

int ObOptEstCostModel::cost_insert(ObDelUpCostInfo& cost_info, double &cost)
{
  int ret = OB_SUCCESS;
  cost = cost_params_.CPU_TUPLE_COST * cost_info.affect_rows_ +
         cost_params_.INSERT_PER_ROW_COST * cost_info.affect_rows_ +
         cost_params_.INSERT_INDEX_PER_ROW_COST * cost_info.index_count_ +
         cost_params_.INSERT_CHECK_PER_ROW_COST * cost_info.constraint_count_;
  return ret;
}

int ObOptEstCostModel::cost_update(ObDelUpCostInfo& cost_info, double &cost)
{
  int ret = OB_SUCCESS;
  cost = cost_params_.CPU_TUPLE_COST * cost_info.affect_rows_ +
         cost_params_.UPDATE_PER_ROW_COST * cost_info.affect_rows_ +
         cost_params_.UPDATE_INDEX_PER_ROW_COST * cost_info.index_count_ +
         cost_params_.UPDATE_CHECK_PER_ROW_COST * cost_info.constraint_count_;
  return ret;
}

int ObOptEstCostModel::cost_delete(ObDelUpCostInfo& cost_info, double &cost)
{
  int ret = OB_SUCCESS;
  cost = cost_params_.CPU_TUPLE_COST * cost_info.affect_rows_ +
         cost_params_.DELETE_PER_ROW_COST * cost_info.affect_rows_ +
         cost_params_.DELETE_INDEX_PER_ROW_COST * cost_info.index_count_ +
         cost_params_.DELETE_CHECK_PER_ROW_COST * cost_info.constraint_count_;
  return ret;
}
