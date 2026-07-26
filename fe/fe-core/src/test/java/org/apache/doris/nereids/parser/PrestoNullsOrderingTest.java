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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Verify the default null-ordering of an ORDER BY key across sql dialects.
 *
 * <p>Doris default: ascending order defaults to NULLS FIRST.
 * Presto/Postgres default: ascending order defaults to NULLS LAST.
 * The dialect-gated behavior only affects an ascending key with no explicit
 * NULLS FIRST / NULLS LAST; descending keys and explicit clauses are unchanged.
 */
public class PrestoNullsOrderingTest {

    private final NereidsParser parser = new NereidsParser();
    private ConnectContext ctx;
    private MockedStatic<ConnectContext> mockedStaticCtx;

    @BeforeEach
    public void setUp() {
        ctx = new ConnectContext();
        mockedStaticCtx = Mockito.mockStatic(ConnectContext.class, Mockito.CALLS_REAL_METHODS);
        mockedStaticCtx.when(ConnectContext::get).thenReturn(ctx);
    }

    @AfterEach
    public void tearDown() {
        if (mockedStaticCtx != null) {
            mockedStaticCtx.close();
            mockedStaticCtx = null;
        }
    }

    private OrderKey firstOrderKey(String sql) {
        LogicalPlan plan = parser.parseSingle(sql);
        LogicalSort<?> sort = findSort(plan);
        Assertions.assertNotNull(sort, "no LogicalSort found in plan for: " + sql);
        Assertions.assertFalse(sort.getOrderKeys().isEmpty(), "no order key found for: " + sql);
        return sort.getOrderKeys().get(0);
    }

    private LogicalSort<?> findSort(Plan plan) {
        if (plan instanceof LogicalSort) {
            return (LogicalSort<?>) plan;
        }
        for (Plan child : plan.children()) {
            LogicalSort<?> found = findSort(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Test
    public void testDorisDialectDefaultsToNullsFirstForAsc() {
        ctx.getSessionVariable().setSqlDialect("doris");

        // implicit ascending -> NULLS FIRST (Doris default)
        Assertions.assertTrue(firstOrderKey("SELECT c1 FROM t1 ORDER BY c1").isNullFirst());
        // explicit ascending -> NULLS FIRST
        Assertions.assertTrue(firstOrderKey("SELECT c1 FROM t1 ORDER BY c1 ASC").isNullFirst());
        // descending -> NULLS LAST
        Assertions.assertFalse(firstOrderKey("SELECT c1 FROM t1 ORDER BY c1 DESC").isNullFirst());
    }

    @Test
    public void testPrestoDialectDefaultsToNullsLastForAsc() {
        ctx.getSessionVariable().setSqlDialect("presto");

        // implicit ascending -> NULLS LAST (Presto default, the ported behavior)
        Assertions.assertFalse(firstOrderKey("SELECT c1 FROM t1 ORDER BY c1").isNullFirst());
        // explicit ascending -> NULLS LAST
        Assertions.assertFalse(firstOrderKey("SELECT c1 FROM t1 ORDER BY c1 ASC").isNullFirst());
        // descending -> NULLS LAST (unchanged)
        Assertions.assertFalse(firstOrderKey("SELECT c1 FROM t1 ORDER BY c1 DESC").isNullFirst());
    }

    @Test
    public void testExplicitNullsClauseOverridesDialectDefault() {
        // explicit NULLS FIRST / LAST must win regardless of dialect
        ctx.getSessionVariable().setSqlDialect("presto");
        Assertions.assertTrue(firstOrderKey("SELECT c1 FROM t1 ORDER BY c1 ASC NULLS FIRST").isNullFirst());
        Assertions.assertFalse(firstOrderKey("SELECT c1 FROM t1 ORDER BY c1 ASC NULLS LAST").isNullFirst());
        Assertions.assertTrue(firstOrderKey("SELECT c1 FROM t1 ORDER BY c1 DESC NULLS FIRST").isNullFirst());

        ctx.getSessionVariable().setSqlDialect("doris");
        Assertions.assertFalse(firstOrderKey("SELECT c1 FROM t1 ORDER BY c1 ASC NULLS LAST").isNullFirst());
        Assertions.assertTrue(firstOrderKey("SELECT c1 FROM t1 ORDER BY c1 DESC NULLS FIRST").isNullFirst());
    }
}
