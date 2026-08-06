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
import org.apache.doris.catalog.Type;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Static cost gate deciding whether a branchy expression (CASE, IF, IFNULL/NVL, COALESCE) should
 * use lazy short-circuit evaluation on the BE -- each value branch evaluated only on the rows that
 * reach it -- instead of the eager path that evaluates every branch on all rows.
 *
 * <p>Lazy evaluation wins when the value branches are expensive (the per-row work it skips dwarfs
 * the selector gather it adds), and loses when they are cheap. So the gate turns lazy on only when
 * the summed static cost of the value branches crosses a threshold. The cost is a general
 * operation-complexity x data-width model, not tuned to any single workload, mirroring the BE
 * scan-layer conjunct cost (VExpr::conjunct_node_cost).
 *
 * <p>Value branches whose subtree contains a non-deterministic or side-effecting function
 * (rand/random/uuid/sleep) are excluded: lazy evaluation would change how many times, and on which
 * rows, such a function runs, which is observable. Those keep the eager path.
 */
public class ShortCircuitCostGate {

    // Mirrors the authoritative non-determinism lists in
    // org.apache.doris.planner.normalize.ExprNormalizeVisitor (nonDeterministicFunctions +
    // nonDeterministicTimeFunctions), inlined here (upstream keeps those private) plus SLEEP for
    // its timing side effect. Kept as a single union set because the gate treats all of them the
    // same way: a branch containing any must stay eager.
    private static final Set<String> NON_DETERMINISTIC_OR_SIDE_EFFECT_FUNCTIONS =
            ImmutableSet.<String>builder()
                    .add("RAND")
                    .add("RANDOM")
                    .add("RANDOM_BYTES")
                    .add("CONNECTION_ID")
                    .add("DATABASE")
                    .add("USER")
                    .add("UUID")
                    .add("CURRENT_USER")
                    .add("UUID_NUMERIC")
                    .add("NOW")
                    .add("CURDATE")
                    .add("CURRENT_DATE")
                    .add("UTC_TIMESTAMP")
                    .add("CURTIME")
                    .add("CURRENT_TIMESTAMP")
                    .add("CURRENT_TIME")
                    .add("UNIX_TIMESTAMP")
                    .add("SLEEP")
                    .build();

    private ShortCircuitCostGate() {}

    /**
     * Decide whether the BE should use lazy short-circuit evaluation for a branchy expression.
     *
     * @param sv the session variables (explicit override + auto-gate switches)
     * @param guardBranches every sub-expression the lazy path would evaluate on a row subset rather
     *                      than on all rows -- for CASE this is all children (later WHEN conditions
     *                      are themselves evaluated only on not-yet-matched rows), for IF/COALESCE
     *                      the args after the first. If any contains a non-deterministic or
     *                      side-effecting function, lazy would change its call count / per-row draw,
     *                      so we keep the eager path.
     * @param costBranches the value-producing sub-expressions whose per-row work lazy can skip (CASE
     *                     THENs + ELSE, IF/COALESCE args after the first). Only these are summed for
     *                     the cost threshold; conditions run on all surviving rows regardless.
     */
    public static boolean shouldShortCircuit(SessionVariable sv, List<Expr> guardBranches,
            List<Expr> costBranches) {
        // The explicit legacy switch forces lazy regardless of cost, for back-compat.
        if (sv.isShortCircuitEvaluation()) {
            return true;
        }
        if (!sv.isEnableCostBasedShortCircuit()) {
            return false;
        }
        // Any branch the lazy path evaluates on a subset must be deterministic and side-effect-free,
        // else its call count / per-row value would change observably.
        for (Expr branch : guardBranches) {
            if (containsNondeterministicOrSideEffect(branch)) {
                return false;
            }
        }
        long cost = 0;
        for (Expr branch : costBranches) {
            cost += exprCost(branch);
        }
        return cost >= sv.getShortCircuitCostThreshold();
    }

    // The value-producing branches of a CASE -- the THENs and the optional ELSE. WHEN conditions run
    // on all surviving rows regardless, so they do not affect the cost tradeoff and are excluded from
    // the cost sum (but are still guarded for non-determinism via the full children list). Child
    // layout is [case?, when, then, when, then, ..., else?]: skip the optional leading case expr and
    // the when conditions; collect the thens and the optional trailing else.
    public static List<Expr> caseValueBranches(CaseExpr caseExpr) {
        List<Expr> children = caseExpr.getChildren();
        List<Expr> branches = new ArrayList<>();
        int loopStart = caseExpr.isHasCaseExpr() ? 1 : 0;
        int loopEnd = caseExpr.isHasElseExpr() ? children.size() - 1 : children.size();
        // whenClauses are complete (when, then) pairs: skip the when at i, take the then at i + 1.
        for (int i = loopStart; i + 1 < loopEnd; i += 2) {
            branches.add(children.get(i + 1));
        }
        if (caseExpr.isHasElseExpr()) {
            branches.add(children.get(children.size() - 1));
        }
        return branches;
    }

    // The value-producing branches for a short-circuitable builtin (if/ifnull/nvl/coalesce): the
    // arguments after the first. Empty for any other function -- the cost gate then only honors an
    // explicit short_circuit_evaluation override and never auto-enables, matching prior behavior.
    // (The first arg always runs on all rows as the condition, so it is neither a lazy-subset guard
    // concern nor part of the cost.)
    public static List<Expr> shortCircuitValueBranches(FunctionCallExpr fnCall) {
        String name = fnCall.getFnName().getFunction();
        boolean shortCircuitable = name.equalsIgnoreCase("if") || name.equalsIgnoreCase("ifnull")
                || name.equalsIgnoreCase("nvl") || name.equalsIgnoreCase("coalesce");
        List<Expr> children = fnCall.getChildren();
        if (!shortCircuitable || children.size() <= 1) {
            return new ArrayList<>();
        }
        return new ArrayList<>(children.subList(1, children.size()));
    }

    private static boolean containsNondeterministicOrSideEffect(Expr expr) {
        if (expr == null) {
            return false;
        }
        if (expr instanceof FunctionCallExpr
                && isNondeterministicOrHasSideEffect((FunctionCallExpr) expr)) {
            return true;
        }
        for (Expr child : expr.getChildren()) {
            if (containsNondeterministicOrSideEffect(child)) {
                return true;
            }
        }
        return false;
    }

    // True if calling this function a different number of times or on a different subset of rows
    // would be observable -- the value is non-deterministic per invocation (rand/uuid/session/time
    // functions), the call carries a side effect (sleep), or it is a user-defined function whose
    // purity/determinism we cannot guarantee. Lazy short-circuit evaluation runs branches on a
    // subset of rows, so such functions must keep the eager (all-rows) path. With no determinism
    // metadata for a non-BUILTIN function (Java/RPC/Hive/native UDF), we conservatively forgo the
    // optimization rather than risk changing observable behavior.
    private static boolean isNondeterministicOrHasSideEffect(FunctionCallExpr fnCall) {
        Function fn = fnCall.getFn();
        if (fn != null && fn.getBinaryType() != Function.BinaryType.BUILTIN) {
            return true;
        }
        String name = fnCall.getFnName().getFunction().toUpperCase();
        return NON_DETERMINISTIC_OR_SIDE_EFFECT_FUNCTIONS.contains(name);
    }

    // Static per-node cost, summed over the branch subtree. Cost is modeled generally as
    // operation complexity x data width, not tuned for any single workload:
    //   - operation complexity: divide/modulo are the heaviest scalar ops (software loops on wide
    //     types), then multiply, then add/subtract, then comparisons/casts; function calls are
    //     expensive regardless (string / regexp / json); leaves (slots, literals) are ~free.
    //   - data width: a wide operand (Decimal128/LargeInt = 16 bytes, Decimal256 = 32 bytes) costs
    //     proportionally more per row than an 8-byte int/double. Taken from the type's slot size.
    // The product means any wide+heavy combination scores high on its own -- Decimal256 divide,
    // Decimal128 multiply, deep nested arithmetic, string functions -- while cheap int/literal
    // branches stay low.
    static long exprCost(Expr expr) {
        if (expr == null) {
            return 0;
        }
        long selfCost;
        if (expr instanceof SlotRef || expr instanceof LiteralExpr) {
            selfCost = 1;
        } else if (expr instanceof ArithmeticExpr) {
            selfCost = arithmeticBaseCost(((ArithmeticExpr) expr).getOp()) * widthMultiplier(expr);
        } else if (expr instanceof CastExpr || expr instanceof BinaryPredicate) {
            selfCost = 2 * widthMultiplier(expr);
        } else if (expr instanceof FunctionCallExpr) {
            selfCost = 100;
        } else if (expr instanceof CaseExpr) {
            selfCost = 10;
        } else {
            selfCost = 2;
        }
        for (Expr child : expr.getChildren()) {
            selfCost += exprCost(child);
        }
        return selfCost;
    }

    // Operation complexity, independent of data type. Divide/modulo dominate because on wide
    // integer/decimal types they run iterative software routines; multiply is next; add/subtract
    // and the rest are a handful of instructions.
    private static long arithmeticBaseCost(ArithmeticExpr.Operator op) {
        if (op == ArithmeticExpr.Operator.DIVIDE || op == ArithmeticExpr.Operator.INT_DIVIDE
                || op == ArithmeticExpr.Operator.MOD) {
            return 12;
        }
        if (op == ArithmeticExpr.Operator.MULTIPLY) {
            return 5;
        }
        return 3;
    }

    // Per-row width factor from the type's slot size in bytes: <=8 bytes -> 1, 16 bytes
    // (Decimal128 / LargeInt) -> 2, 32 bytes (Decimal256) -> 4. Variable-length or non-scalar /
    // unknown types (negative or missing slot size) fall back to 1.
    private static long widthMultiplier(Expr expr) {
        Type type = expr.getType();
        if (type == null || !type.isScalarType()) {
            return 1;
        }
        try {
            int bytes = type.getPrimitiveType().getSlotSize();
            return bytes <= 8 ? 1 : bytes / 8;
        } catch (RuntimeException e) {
            return 1;
        }
    }
}
