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

package org.apache.doris.nereids.trees.expressions.functions.table;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.jdbc.client.JdbcClientException;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class QueryTest {
    private static class ThrowingTableValuedFunctionIf extends TableValuedFunctionIf {
        private final RuntimeException toThrow;

        ThrowingTableValuedFunctionIf(RuntimeException toThrow) {
            this.toThrow = toThrow;
        }

        @Override
        public String getTableName() {
            return "query";
        }

        @Override
        public List<Column> getTableColumns() throws AnalysisException {
            throw toThrow;
        }

        @Override
        public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
            return null;
        }
    }

    private static class TestQuery extends Query {
        private final RuntimeException toThrow;

        TestQuery(Properties properties, RuntimeException toThrow) {
            super(properties);
            this.toThrow = toThrow;
        }

        @Override
        protected TableValuedFunctionIf toCatalogFunction() {
            return new ThrowingTableValuedFunctionIf(toThrow);
        }
    }

    private Properties queryProperties() {
        Map<String, String> map = Maps.newHashMap();
        map.put("catalog", "jdbc");
        map.put("query", "select 1");
        return new Properties(map);
    }

    @Test
    public void testQueryErrorMessageWithCause() {
        TestQuery query = new TestQuery(queryProperties(),
                new JdbcClientException("test error: %s", new SQLException("test jdbc error"), "test"));
        try {
            query.getTable();
            Assertions.fail("jdbc query should throw exception");
        } catch (Exception e) {
            Assertions.assertEquals("Can not build FunctionGenTable 'query'. error: JdbcClientException: test error: test\n"
                    + "  Caused by: SQLException: test jdbc error", e.getMessage());
        }
    }

    @Test
    public void testQueryErrorMessageWithoutCause() {
        TestQuery query = new TestQuery(queryProperties(),
                new JdbcClientException("test error without cause"));
        try {
            query.getTable();
            Assertions.fail("jdbc query should throw exception");
        } catch (Exception e) {
            Assertions.assertEquals(
                    "Can not build FunctionGenTable 'query'. error: JdbcClientException: test error without cause",
                    e.getMessage());
        }
    }
}
