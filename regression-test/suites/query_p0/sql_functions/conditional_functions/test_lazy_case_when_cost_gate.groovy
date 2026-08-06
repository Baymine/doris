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

// Verifies the FE static cost gate that auto-enables lazy CASE WHEN evaluation
// (ShortCircuitCaseExpr on the BE) when the THEN/ELSE branches are expensive -- the
// Decimal256-division hotspot -- while cheap CASEs stay eager. The gate must never change
// results: lazy and eager paths are asserted identical for every query below.
suite("test_lazy_case_when_cost_gate") {
    sql "set batch_size = 4096;"

    sql "DROP TABLE IF EXISTS test_lazy_case_when;"
    sql """
        CREATE TABLE IF NOT EXISTS test_lazy_case_when (
            id INT,
            k INT NULL,
            a DECIMAL(76, 6) NULL,
            b DECIMAL(76, 6) NULL,
            c DECIMAL(76, 6) NULL,
            d DECIMAL(76, 6) NULL,
            e DECIMAL(76, 6) NULL,
            f DECIMAL(76, 6) NULL,
            s VARCHAR(50) NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """

    // Mix of matching branches, NULL divisors (Decimal divide returns NULL, never throws),
    // NULL numerators, and rows falling through to ELSE.
    sql """
        INSERT INTO test_lazy_case_when VALUES
            (1, 1, 100.5, 4.0, 7.0, 2.0, 9.0, 3.0, 'x'),
            (2, 2, 20.0, 8.0, 60.6, 5.0, 1.0, 4.0, 'y'),
            (3, 3, 30.0, 2.0, 12.0, 6.0, 8.0, 2.0, 'z'),
            (4, 1, 4.0, NULL, 9.0, 3.0, 7.0, 1.0, NULL),
            (5, 2, NULL, 5.0, NULL, 2.0, 6.0, 3.0, 'p'),
            (6, 9, 55.0, 5.0, 40.0, 8.0, 2.0, NULL, 'q'),
            (7, 1, 77.0, 7.0, 30.0, 6.0, 10.0, 5.0, 'r'),
            (8, 3, 88.0, 0.0, 24.0, 4.0, 3.0, 3.0, 's'),
            (9, 2, 9.0, 9.0, 45.0, 5.0, NULL, 2.0, NULL),
            (10, 1, 100.0, 10.0, 50.0, 2.0, 4.0, 4.0, 't');
    """

    // Run a query under three settings and assert identical results:
    //   (1) default          -> auto cost-gate decides (expensive CASE -> lazy)
    //   (2) forced eager      -> enable_cost_based_short_circuit=false, short_circuit_evaluation=false
    //   (3) forced lazy       -> short_circuit_evaluation=true (explicit override)
    def assert_lazy_eager_equal = { String tag, String query ->
        sql "set enable_cost_based_short_circuit = true;"
        sql "set short_circuit_evaluation = false;"
        sql "set short_circuit_cost_threshold = 30;"
        def resultDefault = sql query

        sql "set enable_cost_based_short_circuit = false;"
        sql "set short_circuit_evaluation = false;"
        def resultEager = sql query

        sql "set enable_cost_based_short_circuit = true;"
        sql "set short_circuit_evaluation = true;"
        def resultLazy = sql query

        assertEquals(resultEager, resultDefault, "default (auto-gate) != eager for ${tag}")
        assertEquals(resultEager, resultLazy, "forced-lazy != eager for ${tag}")
    }

    // Expensive: every THEN/ELSE is a Decimal256 divide -> gate flips this to lazy by default.
    assert_lazy_eager_equal("dec256_divide_case", """
        select id,
            case when k = 1 then a / b
                 when k = 2 then c / d
                 else e / f
            end as result
        from test_lazy_case_when order by id
    """)

    // Nested expensive CASE inside a THEN.
    assert_lazy_eager_equal("dec256_nested_case", """
        select id,
            case when k = 1 then
                     case when a > 50 then a / b else b / a end
                 when k = 2 then c / d
                 else e / f
            end as result
        from test_lazy_case_when order by id
    """)

    // CASE without ELSE: unmatched rows produce NULL; must still match across paths.
    assert_lazy_eager_equal("dec256_no_else", """
        select id,
            case when k = 1 then a / b
                 when k = 2 then c / d
            end as result
        from test_lazy_case_when order by id
    """)

    // Cheap CASE: THEN branches are plain literals -> gate keeps this eager by default.
    // Still must return correctly and match the forced-lazy path.
    assert_lazy_eager_equal("cheap_case", """
        select id,
            case when k = 1 then 'one'
                 when k = 2 then 'two'
                 else 'other'
            end as result
        from test_lazy_case_when order by id
    """)

    // A single-WHEN CASE is rewritten to if() by CaseWhenToIf before reaching the CaseExpr gate,
    // so the gate must also cover if(). Expensive Decimal256 value branches -> lazy by default.
    // (Written as if() directly to exercise the FunctionCallExpr path regardless of rewrite.)
    assert_lazy_eager_equal("if_dec256_divide", """
        select id, if(k = 1, a / b, c / d) as result
        from test_lazy_case_when order by id
    """)

    // coalesce over Decimal256 divides: value branches are args after the first -> lazy by default.
    assert_lazy_eager_equal("coalesce_dec256", """
        select id, coalesce(a / b, c / d, e / f) as result
        from test_lazy_case_when order by id
    """)

    sql "DROP TABLE IF EXISTS test_lazy_case_when;"

    // reset to defaults
    sql "set enable_cost_based_short_circuit = true;"
    sql "set short_circuit_evaluation = false;"
    sql "set short_circuit_cost_threshold = 30;"
}
