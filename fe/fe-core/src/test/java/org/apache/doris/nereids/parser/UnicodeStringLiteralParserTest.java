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

import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UnicodeStringLiteralParserTest extends TestWithFeService {

    private String parseValue(String sql) {
        NereidsParser parser = new NereidsParser();
        Plan plan = parser.parseSingle(sql);
        LogicalPlan child = (LogicalPlan) plan.child(0);
        UnboundOneRowRelation one = (UnboundOneRowRelation) child;
        Expression expr = one.getProjects().get(0).child(0);
        Assertions.assertInstanceOf(StringLikeLiteral.class, expr);
        return ((StringLikeLiteral) expr).getStringValue();
    }

    @Test
    public void testBmpEscape() {
        Assertions.assertEquals("A", parseValue("SELECT U&'\\0041'"));
    }

    @Test
    public void testSupplementaryEscape() {
        Assertions.assertEquals(new String(Character.toChars(0x1F600)), parseValue("SELECT U&'\\+01F600'"));
    }

    @Test
    public void testLiteralBackslash() {
        Assertions.assertEquals("\\", parseValue("SELECT U&'\\\\'"));
    }

    @Test
    public void testMixedEscapeAndPlainText() {
        Assertions.assertEquals("aAb", parseValue("SELECT U&'a\\0041b'"));
    }

    @Test
    public void testDoubledQuoteCollapsed() {
        Assertions.assertEquals("it's", parseValue("SELECT U&'it''s'"));
    }

    @Test
    public void testLowerCaseHexDigits() {
        Assertions.assertEquals("j", parseValue("SELECT U&'\\006a'"));
        Assertions.assertEquals(new String(Character.toChars(0x00E9)), parseValue("SELECT U&'\\00e9'"));
    }

    @Test
    public void testEmpty() {
        Assertions.assertEquals("", parseValue("SELECT U&''"));
    }

    @Test
    public void testInvalidHexDigit() {
        NereidsParser parser = new NereidsParser();
        Assertions.assertThrows(ParseException.class, () -> parser.parseSingle("SELECT U&'\\XYZ0'"));
    }

    @Test
    public void testIncompleteSequence() {
        NereidsParser parser = new NereidsParser();
        Assertions.assertThrows(ParseException.class, () -> parser.parseSingle("SELECT U&'\\004'"));
    }

    @Test
    public void testLoneSurrogate() {
        NereidsParser parser = new NereidsParser();
        Assertions.assertThrows(ParseException.class, () -> parser.parseSingle("SELECT U&'\\D800'"));
    }
}
