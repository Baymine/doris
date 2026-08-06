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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionName;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TExprNode;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for the FE static cost gate ({@link ShortCircuitCostGate}) that sets the BE
 * short_circuit_evaluation flag on branchy expressions: CASE ({@link ExprToThriftVisitor}'s
 * visitCaseExpr) and the if / ifnull / nvl / coalesce builtins (visitFunctionCallExpr). Cheap
 * branches must stay eager (flag false) so the selector gather never backfires; expensive value
 * branches (Decimal256 division, wide arithmetic, function calls) must flip to lazy. A branch with
 * a non-deterministic / side-effecting function (rand/uuid/sleep) must stay eager regardless of
 * cost.
 */
public class CaseExprLazyGateTest {

    private ConnectContext ctx;

    @BeforeEach
    public void setUp() {
        ctx = new ConnectContext();
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private SessionVariable sv() {
        return ctx.getSessionVariable();
    }

    private boolean lazyFlagOf(Expr expr) {
        TExprNode msg = new TExprNode();
        expr.accept(ExprToThriftVisitor.INSTANCE, msg);
        return msg.isSetShortCircuitEvaluation() && msg.isShortCircuitEvaluation();
    }

    private SlotRef intSlot() {
        return new SlotRef(Type.INT, true);
    }

    private SlotRef decimal256Slot() {
        // precision 76 resolves to the DECIMAL256 primitive type (> DECIMAL128's max of 38).
        return new SlotRef(ScalarType.createDecimalV3Type(76, 6), true);
    }

    // dec256a / dec256b -> a Decimal256-typed divide, the real flamegraph hotspot.
    private ArithmeticExpr decimal256Divide() {
        return new ArithmeticExpr(ArithmeticExpr.Operator.DIVIDE, decimal256Slot(), decimal256Slot(),
                ScalarType.createDecimalV3Type(76, 6), NullableMode.DEPEND_ON_ARGUMENT, true);
    }

    private SlotRef decimal128Slot() {
        // precision 38 resolves to the DECIMAL128 primitive type (16-byte slot).
        return new SlotRef(ScalarType.createDecimalV3Type(38, 6), true);
    }

    // A Decimal128 multiply: a wide+heavy op that is NOT a Decimal256 divide, to prove the gate is
    // general (operation complexity x data width) and not tuned to one workload.
    private ArithmeticExpr decimal128Multiply() {
        return new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, decimal128Slot(), decimal128Slot(),
                ScalarType.createDecimalV3Type(38, 6), NullableMode.DEPEND_ON_ARGUMENT, true);
    }

    private FunctionCallExpr call(String name, Expr... args) {
        List<Expr> argList = Lists.newArrayList(args);
        // FunctionCallExpr's thrift conversion requires a bound (non-null, non-aggregate) fn; a real
        // query binds this during analysis. Attach a minimal non-aggregate builtin so the visitor
        // reaches the short-circuit cost gate without running the full analyzer.
        List<Type> argTypes = new ArrayList<>();
        for (Expr arg : args) {
            argTypes.add(arg.getType() == null ? Type.INT : arg.getType());
        }
        ScalarFunction fn = new ScalarFunction(new FunctionName(name), argTypes, Type.INT, false, true);
        return new FunctionCallExpr(fn, new FunctionParams(argList), true);
    }

    // A call bound to a non-BUILTIN (user-defined) function, whose determinism we cannot guarantee.
    private FunctionCallExpr udfCall(String name, Expr... args) {
        FunctionCallExpr fnCall = call(name, args);
        fnCall.getFn().setBinaryType(Function.BinaryType.JAVA_UDF);
        return fnCall;
    }

    private CaseExpr caseOf(List<CaseWhenClause> whens, Expr elseExpr) {
        return new CaseExpr(whens, elseExpr, true);
    }

    // A cheap CASE: WHEN int_col = 1 THEN 'a' ELSE 'b'. Only literals/slots in the branches.
    private CaseExpr cheapCase() {
        BinaryPredicate when = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        List<CaseWhenClause> whens = Lists.newArrayList(
                new CaseWhenClause(when, new StringLiteral("a")));
        return caseOf(whens, new StringLiteral("b"));
    }

    // An expensive CASE: WHEN int_col = 1 THEN a/b WHEN int_col = 2 THEN c/d ELSE e/f, all Decimal256.
    private CaseExpr decimal256Case() {
        BinaryPredicate when1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        BinaryPredicate when2 = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(2));
        List<CaseWhenClause> whens = Lists.newArrayList(
                new CaseWhenClause(when1, decimal256Divide()),
                new CaseWhenClause(when2, decimal256Divide()));
        return caseOf(whens, decimal256Divide());
    }

    @Test
    public void testCheapCaseStaysEagerByDefault() {
        // Defaults: enable_cost_based_short_circuit=true, threshold=30, short_circuit_evaluation=false.
        // 'a' + 'b' cost = 2, well below the threshold, so the flag stays off (eager VCaseExpr).
        Assertions.assertFalse(lazyFlagOf(cheapCase()));
    }

    @Test
    public void testDecimal256DivideCaseGoesLazyByDefault() {
        // Three Decimal256 divides in THEN/ELSE (~50 each) blow past the default threshold of 30.
        Assertions.assertTrue(lazyFlagOf(decimal256Case()));
    }

    @Test
    public void testFunctionCallThenGoesLazyByDefault() {
        // WHEN int_col = 1 THEN concat('x', 'y') ELSE 'z': a function call in THEN scores 100.
        BinaryPredicate when = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        List<CaseWhenClause> whens = Lists.newArrayList(
                new CaseWhenClause(when, call("concat", new StringLiteral("x"), new StringLiteral("y"))));
        Assertions.assertTrue(lazyFlagOf(caseOf(whens, new StringLiteral("z"))));
    }

    @Test
    public void testWideNonDivideArithmeticGoesLazy() {
        // Generality guard: the gate must fire on wide+heavy branches that are neither Decimal256
        // nor division. Three Decimal128 (16-byte) multiplies carry real per-row cost and cross the
        // threshold on their own, proving the model is not tuned to the Decimal256-divide workload.
        BinaryPredicate w1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        BinaryPredicate w2 = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(2));
        List<CaseWhenClause> whens = Lists.newArrayList(
                new CaseWhenClause(w1, decimal128Multiply()),
                new CaseWhenClause(w2, decimal128Multiply()));
        Assertions.assertTrue(lazyFlagOf(caseOf(whens, decimal128Multiply())));
    }

    @Test
    public void testCheapIntArithmeticStaysEager() {
        // Narrow arithmetic (int + int) is cheap even though it is an ArithmeticExpr: stays eager.
        BinaryPredicate when = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        ArithmeticExpr add = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, intSlot(), intSlot(),
                Type.INT, NullableMode.DEPEND_ON_ARGUMENT, true);
        List<CaseWhenClause> whens = Lists.newArrayList(new CaseWhenClause(when, add));
        Assertions.assertFalse(lazyFlagOf(caseOf(whens, new IntLiteral(0))));
    }

    @Test
    public void testNondeterministicThenStaysEager() {
        // P1: a THEN whose subtree contains rand()/uuid()/sleep() must NOT go lazy even though its
        // cost is high (function call = 100), because lazy evaluation would change how many times
        // and on which rows the function runs -- observable. Keep the eager (all-rows) path.
        BinaryPredicate when = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        List<CaseWhenClause> whens = Lists.newArrayList(new CaseWhenClause(when, call("rand")));
        Assertions.assertFalse(lazyFlagOf(caseOf(whens, new IntLiteral(0))));
    }

    @Test
    public void testSleepThenStaysEager() {
        // P1: sleep() carries a timing side effect; a branch containing it must stay eager.
        BinaryPredicate when = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        List<CaseWhenClause> whens = Lists.newArrayList(
                new CaseWhenClause(when, call("sleep", new IntLiteral(1))));
        Assertions.assertFalse(lazyFlagOf(caseOf(whens, new IntLiteral(0))));
    }

    @Test
    public void testNondeterministicInWhenConditionStaysEager() {
        // P1 (WHEN scope): the lazy path evaluates each later WHEN only on not-yet-matched rows, so
        // a non-deterministic function in ANY when condition -- not just a THEN -- must keep eager.
        // WHEN rand() > 0.5 THEN <expensive> WHEN k = 2 THEN <expensive> ELSE <expensive>.
        BinaryPredicate randWhen = new BinaryPredicate(BinaryPredicate.Operator.GT,
                call("rand"), new FloatLiteral(0.5));
        BinaryPredicate when2 = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(2));
        List<CaseWhenClause> whens = Lists.newArrayList(
                new CaseWhenClause(randWhen, decimal256Divide()),
                new CaseWhenClause(when2, decimal256Divide()));
        Assertions.assertFalse(lazyFlagOf(caseOf(whens, decimal256Divide())));
    }

    @Test
    public void testExpandedNondeterministicListStaysEager() {
        // P2 (list completeness): the guard reuses the authoritative non-determinism lists, not a
        // 3-name subset. now() (time) and connection_id() (session) must both keep the CASE eager.
        BinaryPredicate w1 = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        List<CaseWhenClause> nowCase = Lists.newArrayList(new CaseWhenClause(w1, call("now")));
        Assertions.assertFalse(lazyFlagOf(caseOf(nowCase, new IntLiteral(0))));

        BinaryPredicate w2 = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        List<CaseWhenClause> connCase = Lists.newArrayList(new CaseWhenClause(w2, call("connection_id")));
        Assertions.assertFalse(lazyFlagOf(caseOf(connCase, new IntLiteral(0))));
    }

    @Test
    public void testUdfBranchStaysEager() {
        // A user-defined (non-BUILTIN) scalar function has no determinism guarantee; the guard must
        // conservatively keep the CASE eager even though its cost (100) would otherwise trip lazy.
        BinaryPredicate when = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        List<CaseWhenClause> whens = Lists.newArrayList(
                new CaseWhenClause(when, udfCall("my_udf", intSlot())));
        Assertions.assertFalse(lazyFlagOf(caseOf(whens, new IntLiteral(0))));
    }

    @Test
    public void testBuiltinCounterpartOfUdfNameGoesLazy() {
        // Sanity: the same call as a BUILTIN (deterministic) does trip the gate -- proving the UDF
        // exclusion keys on binary type, not the name, and does not over-block builtins.
        BinaryPredicate when = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        List<CaseWhenClause> whens = Lists.newArrayList(
                new CaseWhenClause(when, call("my_udf", intSlot())));
        Assertions.assertTrue(lazyFlagOf(caseOf(whens, new IntLiteral(0))));
    }

    @Test
    public void testIfWithExpensiveBranchGoesLazy() {
        // P2: single-WHEN CASE is rewritten to if() by CaseWhenToIf before it reaches CaseExpr, so
        // the gate must also cover if(). if(cond, a/b, c/d) with Decimal256 divides in both value
        // args (args after the first) crosses the threshold.
        BinaryPredicate cond = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        Assertions.assertTrue(lazyFlagOf(call("if", cond, decimal256Divide(), decimal256Divide())));
    }

    @Test
    public void testCheapIfStaysEager() {
        // if(cond, 'a', 'b') has only literal value branches -> stays eager.
        BinaryPredicate cond = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        Assertions.assertFalse(lazyFlagOf(call("if", cond, new StringLiteral("a"), new StringLiteral("b"))));
    }

    @Test
    public void testIfWithNondeterministicBranchStaysEager() {
        // P1 across P2: if() whose value branch contains rand() must stay eager too.
        BinaryPredicate cond = new BinaryPredicate(BinaryPredicate.Operator.EQ, intSlot(), new IntLiteral(1));
        Assertions.assertFalse(lazyFlagOf(call("if", cond, call("rand"), new IntLiteral(0))));
    }

    @Test
    public void testCoalesceWithExpensiveBranchGoesLazy() {
        // coalesce value branches are all args after the first; expensive Decimal256 divides trip it.
        Assertions.assertTrue(lazyFlagOf(
                call("coalesce", decimal256Slot(), decimal256Divide(), decimal256Divide())));
    }

    @Test
    public void testNonShortCircuitFunctionIgnoresAutoGate() {
        // A regular function (concat) has no short-circuit form; the auto-gate must not set the flag
        // for it even with expensive args -- only the explicit legacy override may.
        Assertions.assertFalse(lazyFlagOf(call("concat", decimal256Divide(), decimal256Divide())));
        sv().shortCircuitEvaluation = true;
        Assertions.assertTrue(lazyFlagOf(call("concat", new StringLiteral("a"), new StringLiteral("b"))));
    }

    @Test
    public void testExplicitShortCircuitOverridesCheapCase() {
        // The legacy explicit switch forces lazy regardless of cost (back-compat).
        sv().shortCircuitEvaluation = true;
        Assertions.assertTrue(lazyFlagOf(cheapCase()));
    }

    @Test
    public void testMasterSwitchOffKeepsExpensiveCaseEager() {
        // With auto-gating disabled and no explicit override, even an expensive CASE stays eager.
        sv().setEnableCostBasedShortCircuit(false);
        Assertions.assertFalse(lazyFlagOf(decimal256Case()));
    }

    @Test
    public void testThresholdBoundaryFlips() {
        CaseExpr expensive = decimal256Case();
        // Far above any reachable cost: gate stays eager.
        sv().setShortCircuitCostThreshold(100000);
        Assertions.assertFalse(lazyFlagOf(expensive));
        // Threshold of 1: any non-trivial branch trips it, including the cheap CASE.
        sv().setShortCircuitCostThreshold(1);
        Assertions.assertTrue(lazyFlagOf(cheapCase()));
    }

    @Test
    public void testNoConnectContextLeavesFlagUnset() {
        // Conversion must not NPE and must not set the flag when there is no session context.
        ConnectContext.remove();
        TExprNode msg = new TExprNode();
        cheapCase().accept(ExprToThriftVisitor.INSTANCE, msg);
        Assertions.assertFalse(msg.isSetShortCircuitEvaluation());
    }
}
