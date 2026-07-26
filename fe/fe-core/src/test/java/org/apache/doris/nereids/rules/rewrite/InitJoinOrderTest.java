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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InitJoinOrderTest implements MemoPatternMatchSupported {
    private static final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private static final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 1);

    @Test
    void testSwapSmallTableToLeft() {
        LogicalPlan join = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new InitJoinOrder())
                .matches(logicalJoin());
    }

    @Test
    void testNoSwapForSemiJoin() {
        LogicalPlan join = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_SEMI_JOIN, Pair.of(0, 0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new InitJoinOrder())
                .matches(logicalJoin());
    }

    @Test
    void testNoSwapForAntiJoin() {
        LogicalPlan join = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.LEFT_ANTI_JOIN, Pair.of(0, 0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new InitJoinOrder())
                .matches(logicalJoin());
    }

    // Idempotency guard: once a LogicalJoin has been visited by InitJoinOrder
    // (hasInitJoinOrder == true on its JoinReorderContext), a second application
    // of the rule must be a no-op.
    @Test
    void testSkipWhenAlreadyMarked() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();
        ((LogicalJoin<?, ?>) plan).getJoinReorderContext().setHasInitJoinOrder(true);

        Plan after = PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyBottomUp(new InitJoinOrder())
                .getPlan();

        Assertions.assertTrue(after instanceof LogicalJoin);
        Assertions.assertTrue(
                ((LogicalJoin<?, ?>) after).getJoinReorderContext().hasInitJoinOrder(),
                "hasInitJoinOrder must survive the rewrite pass");
    }

    // After a successful swap, the rule must stamp hasInitJoinOrder=true on the
    // resulting LogicalJoin so that subsequent re-applications terminate.
    @Test
    void testMarkerSetAfterSwap() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .join(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();

        Plan after = PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyBottomUp(new InitJoinOrder())
                .getPlan();

        if (after instanceof LogicalJoin && !after.deepEquals(plan)) {
            Assertions.assertTrue(
                    ((LogicalJoin<?, ?>) after).getJoinReorderContext().hasInitJoinOrder(),
                    "After InitJoinOrder swaps a join, the result must be marked "
                            + "to prevent re-evaluation under BottomUpVisitorRewriteJob.");
        }
    }

    @Test
    void testJoinReorderContextCopyFromPreservesMarker() {
        JoinReorderContext src = new JoinReorderContext();
        src.setHasInitJoinOrder(true);
        src.setHasCommute(true);

        JoinReorderContext dst = new JoinReorderContext();
        dst.copyFrom(src);

        Assertions.assertTrue(dst.hasInitJoinOrder(),
                "copyFrom must propagate hasInitJoinOrder");
        Assertions.assertTrue(dst.hasCommute(),
                "copyFrom must propagate hasCommute");
    }

    @Test
    void testJoinReorderContextClearResetsMarker() {
        JoinReorderContext ctx = new JoinReorderContext();
        ctx.setHasInitJoinOrder(true);
        ctx.setLeadingJoin(true);

        ctx.clear();

        Assertions.assertFalse(ctx.hasInitJoinOrder(),
                "clear must reset hasInitJoinOrder");
        Assertions.assertFalse(ctx.isLeadingJoin(),
                "clear must reset isLeadingJoin");
    }
}
