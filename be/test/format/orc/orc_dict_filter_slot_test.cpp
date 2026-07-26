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

#include <gtest/gtest.h>

#include <memory>
#include <set>

#include "exprs/vexpr.h"
#include "exprs/vslot_ref.h"
#include "format/orc/vorc_reader.h"

namespace doris {

// Non-slot expr node that can hold children, used to stand in for a function like
// concat(...) that references several slots. _collect_slot_ids only reads node_type
// and children, so execute_column_impl/expr_name are just stubs to satisfy the interface.
class FakeParentExpr final : public VExpr {
public:
    FakeParentExpr() { _node_type = TExprNodeType::FUNCTION_CALL; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::OK();
    }

    const std::string& expr_name() const override { return _name; }

private:
    std::string _name = "FakeParentExpr";
};

static VExprSPtr make_slot(int slot_id) {
    auto slot = std::make_shared<VSlotRef>();
    slot->set_slot_id(slot_id);
    return slot;
}

static VExprSPtr make_parent(const std::vector<VExprSPtr>& children) {
    auto parent = std::make_shared<FakeParentExpr>();
    for (const auto& c : children) {
        parent->add_child(c);
    }
    return parent;
}

class OrcCollectSlotIdsTest : public testing::Test {
protected:
    static std::set<int> collect(const VExprSPtr& expr) {
        std::set<int> ids;
        OrcReader::_collect_slot_ids(expr, ids);
        return ids;
    }
};

TEST_F(OrcCollectSlotIdsTest, SingleSlot) {
    EXPECT_EQ((std::set<int> {5}), collect(make_slot(5)));
}

TEST_F(OrcCollectSlotIdsTest, MultiSlotFunction) {
    // e.g. concat(slot5, slot8): both slots must be collected so both are excluded
    // from dict filtering.
    EXPECT_EQ((std::set<int> {5, 8}), collect(make_parent({make_slot(5), make_slot(8)})));
}

TEST_F(OrcCollectSlotIdsTest, NestedFunctions) {
    // e.g. concat(upper(slot3), slot7) -> {3, 7}
    auto inner = make_parent({make_slot(3)});
    EXPECT_EQ((std::set<int> {3, 7}), collect(make_parent({inner, make_slot(7)})));
}

TEST_F(OrcCollectSlotIdsTest, DuplicateSlotDeduped) {
    EXPECT_EQ((std::set<int> {5}), collect(make_parent({make_slot(5), make_slot(5)})));
}

TEST_F(OrcCollectSlotIdsTest, NoSlotRef) {
    EXPECT_TRUE(collect(make_parent({})).empty());
}

TEST_F(OrcCollectSlotIdsTest, DeeplyNested) {
    auto tree = make_parent({make_parent({make_parent({make_slot(9)})})});
    EXPECT_EQ((std::set<int> {9}), collect(tree));
}

} // namespace doris
