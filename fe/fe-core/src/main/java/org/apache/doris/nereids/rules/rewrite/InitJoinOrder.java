// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.StatsDerive.DeriveContext;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Due to the limitation on the data size in the memo, when optimizing large SQL queries, once this
 * limitation is triggered, some subtrees of the plan tree may not undergo optimization. Therefore,
 * we need to set a reasonably good initial join order before optimizing the plan tree.
 */
public class InitJoinOrder extends OneRewriteRuleFactory {
    private static final Logger LOG = LoggerFactory.getLogger(InitJoinOrder.class);

    // Traditional threshold: left table should be much smaller than right table
    private static final double SWAP_THRESHOLD_TRADITIONAL = 0.1;

    // Relaxed threshold for small tables
    private static final double SWAP_THRESHOLD_RELAXED = 0.3;

    // Absolute small table threshold (10 million rows)
    private static final long SMALL_TABLE_ABSOLUTE_THRESHOLD = 10000000;
    private final StatsDerive derive = new StatsDerive(false);

    @Override
    public Rule build() {
        return logicalJoin()
                .whenNot(LogicalJoin::isMarkJoin)
                .thenApply(ctx -> {
                    if (ctx.statementContext.getConnectContext().getSessionVariable().isDisableJoinReorder()
                            || !ctx.statementContext.getConnectContext().getSessionVariable().enableInitJoinOrder
                            || ctx.cascadesContext.isLeadingDisableJoinReorder()
                            || ((LogicalJoin<?, ?>) ctx.root).isLeadingJoin()) {
                        return null;
                    }
                    LogicalJoin<? extends Plan, ? extends Plan> join = (LogicalJoin<?, ?>) ctx.root;
                    return swapJoinChildrenIfNeed(join, ctx.cascadesContext);
                })
                .toRule(RuleType.INIT_JOIN_ORDER);
    }

    private Plan swapJoinChildrenIfNeed(LogicalJoin<? extends Plan, ? extends Plan> join, CascadesContext context) {
        // Idempotency guard: each LogicalJoin is evaluated at most once by this rule.
        // BottomUpVisitorRewriteJob rebuilds parent nodes via withChildren after any
        // child swap, which would otherwise cause this rule to re-evaluate the same
        // logical join repeatedly. Under concurrent load, stats may be unstable
        // across passes, flipping the swap decision and producing a non-terminating
        // rewrite loop. JoinReorderContext is carried through withChildren and
        // through swapAndMark, so the marker survives across rebuilds.
        if (join.getJoinReorderContext().hasInitJoinOrder()) {
            return null;
        }
        if (join.getJoinType().isLeftSemiOrAntiJoin() || join.getJoinType().isAsofJoin()) {
            // TODO: currently, the transform rules for right semi/anti/asof join is not complete,
            //  for example LogicalJoinSemiJoinTransposeProject (tpch 22) only works for left semi/anti join
            //  if we swap left semi/anti to right semi/anti, we lost the opportunity to optimize join order
            //  and for asof join, the asof right join's performance is poor, we also disable swap
            return null;
        }
        List<CatalogRelation> scans = join.collectToList(CatalogRelation.class::isInstance);
        Optional<String> disableReason = StatsCalculator.disableJoinReorderIfStatsInvalid(scans, context);
        if (!disableReason.isPresent()) {
            JoinType swapType = join.getJoinType().swap();
            if (swapType == null) {
                return null;
            }
            AbstractPlan left = (AbstractPlan) join.left();
            AbstractPlan right = (AbstractPlan) join.right();
            if (left.getStats() == null) {
                left.accept(derive, new DeriveContext());
            }
            if (right.getStats() == null) {
                right.accept(derive, new DeriveContext());
            }

            // requires "left.getStats().getRowCount() > 0" to avoid dead loop when negative row count is estimated.
            double leftRowCount = left.getStats().getRowCount();
            double rightRowCount = right.getStats().getRowCount();

            // Both row counts should be positive for meaningful comparison.
            // The outer disableJoinReorderIfStatsInvalid check already ensures
            // rowCount != UNKNOWN_ROW_COUNT (-1); estimation values (even if not
            // perfectly accurate) are considered reliable for join order decisions.
            if (leftRowCount <= 0 || rightRowCount <= 0) {
                return null;
            }

            // Strategy 1: Left table is absolutely small (<= 10M rows) and right table is larger.
            // Ensures very small tables are always placed on the build side (right side).
            if (leftRowCount <= SMALL_TABLE_ABSOLUTE_THRESHOLD && leftRowCount < rightRowCount) {
                // Use swap() to properly handle ASOF JOIN MATCH_CONDITION commutation
                return swapAndMark(join);
            }

            // Strategy 2: Left table is within broadcast limit and right table is at least 2x larger.
            // Aligns with broadcast join threshold for better memory efficiency.
            double broadcastLimit = context.getStatementContext().getConnectContext()
                    .getSessionVariable().getBroadcastRowCountLimit();
            if (leftRowCount <= broadcastLimit && leftRowCount * 2 < rightRowCount) {
                return swapAndMark(join);
            }

            // Strategy 3: Left table is much smaller than right table (relaxed threshold).
            // Handles cases where the ratio is significant even if not within broadcast limit.
            if (leftRowCount < rightRowCount * SWAP_THRESHOLD_RELAXED) {
                return swapAndMark(join);
            }

            // Strategy 4: Memory-based decision.
            // If right table as build side would exceed memory limit but left would not, swap.
            double leftMemory = left.getStats().computeSize(left.getOutput());
            double rightMemory = right.getStats().computeSize(right.getOutput());
            double memLimit = context.getStatementContext().getConnectContext()
                    .getSessionVariable().getMaxExecMemByte()
                    * context.getStatementContext().getConnectContext()
                    .getSessionVariable().getBroadcastHashtableMemLimitPercentage();

            if (rightMemory > memLimit && leftMemory <= memLimit) {
                return swapAndMark(join);
            }

            // Strategy 5: Fallback to traditional threshold for very small ratios.
            if (leftRowCount < rightRowCount * SWAP_THRESHOLD_TRADITIONAL) {
                return swapAndMark(join);
            }
        }
        return null;
    }

    private LogicalJoin<? extends Plan, ? extends Plan> swapAndMark(
            LogicalJoin<? extends Plan, ? extends Plan> join) {
        LogicalJoin<? extends Plan, ? extends Plan> swapped = join.swap();
        // LogicalJoin.swap() builds a fresh LogicalJoin with a default JoinReorderContext.
        // Copy the original context across so other reorder-state flags
        // (hasCommute, hasLAsscom, isSaltJoinGenerated, ...) are preserved,
        // then set our own marker.
        swapped.getJoinReorderContext().copyFrom(join.getJoinReorderContext());
        swapped.getJoinReorderContext().setHasInitJoinOrder(true);
        return swapped;
    }

}
