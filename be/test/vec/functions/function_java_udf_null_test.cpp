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

#include <gtest/gtest_pred_impl.h>

#include "function_test_util.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "vec/functions/function_java_udf.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class JavaUdfNullTest : public testing::Test {
protected:
    void SetUp() override {
        // Initialize test environment
        _runtime_state = nullptr;
        _function_context = nullptr;
    }

    void TearDown() override {
        // Clean up test environment
        if (_function_context != nullptr) {
            delete _function_context;
            _function_context = nullptr;
        }
        if (_runtime_state != nullptr) {
            delete _runtime_state;
            _runtime_state = nullptr;
        }
    }

    // Helper function to create a test block with nullable columns
    Block create_test_block_with_nulls(const DataTypePtr& input_type, const DataTypePtr& result_type,
                                     const std::vector<Field>& input_data,
                                     const std::vector<UInt8>& null_map) {
        Block block;

        auto col = input_type->create_column();
        EXPECT_EQ(input_data.size(), null_map.size());
        
        // Create nullable column
        auto nullable_col = ColumnNullable::create(std::move(col), ColumnUInt8::create());
        for (size_t i = 0; i < input_data.size(); ++i) {
            if (!null_map[i]) {
                nullable_col->insert(input_data[i]);
            } else {
                nullable_col->insert_default();
            }
        }

        block.insert({std::move(nullable_col), input_type, "input"});
        block.insert({result_type->create_column(), result_type, "result"});

        return block;
    }

    // Helper function to verify column values and null map
    template <typename T>
    void verify_column_values_and_nulls(const IColumn& column,
                                      const std::vector<T>& expected_values,
                                      const std::vector<UInt8>& expected_null_map) {
        ASSERT_EQ(column.size(), expected_values.size());
        ASSERT_EQ(column.size(), expected_null_map.size());

        const auto* nullable_column = typeid_cast<const ColumnNullable*>(&column);
        ASSERT_NE(nullable_column, nullptr);

        const auto& null_map = nullable_column->get_null_map_data();
        for (size_t i = 0; i < expected_null_map.size(); ++i) {
            EXPECT_EQ(null_map[i], expected_null_map[i]) << "at position " << i;
            if (!expected_null_map[i]) {
                Field field;
                column.get(i, field);
                EXPECT_EQ(field.get<T>(), expected_values[i]) << "at position " << i;
            }
        }
    }

    RuntimeState* _runtime_state;
    FunctionContext* _function_context;
};

// Test predicate UDF with null input values
TEST_F(JavaUdfNullTest, PredicateUdfNullInput) {
    // Create input and result types
    auto input_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());

    // Create test data with some null values
    std::vector<Field> input_data = {1, 2, 3, 4, 5};
    std::vector<UInt8> input_null_map = {0, 1, 0, 1, 0};  // 2nd and 4th values are null
    auto block = create_test_block_with_nulls(input_type, result_type, input_data, input_null_map);

    // Create and execute UDF
    JavaFunctionCall udf(/* ... UDF parameters ... */);
    ASSERT_TRUE(udf.execute_impl(_function_context, block, {0}, 1, input_data.size()).ok());

    // Verify results - nulls in input should result in nulls in output
    std::vector<UInt8> expected_values = {1, 0, 1, 0, 1};
    std::vector<UInt8> expected_null_map = {0, 1, 0, 1, 0};
    verify_column_values_and_nulls<UInt8>(*block.get_by_position(1).column,
                                         expected_values, expected_null_map);
}

// Test non-predicate UDF with null input values
TEST_F(JavaUdfNullTest, NonPredicateUdfNullInput) {
    // Create input and result types
    auto input_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    // Create test data with some null values
    std::vector<Field> input_data = {10, 20, 30, 40, 50};
    std::vector<UInt8> input_null_map = {0, 1, 0, 1, 0};  // 2nd and 4th values are null
    auto block = create_test_block_with_nulls(input_type, result_type, input_data, input_null_map);

    // Create and execute UDF
    JavaFunctionCall udf(/* ... UDF parameters ... */);
    ASSERT_TRUE(udf.execute_impl(_function_context, block, {0}, 1, input_data.size()).ok());

    // Verify results - nulls in input should result in nulls in output
    std::vector<Int32> expected_values = {20, 0, 60, 0, 100};  // Double the input values
    std::vector<UInt8> expected_null_map = {0, 1, 0, 1, 0};
    verify_column_values_and_nulls<Int32>(*block.get_by_position(1).column,
                                         expected_values, expected_null_map);
}

// Test predicate UDF with NPE handling
TEST_F(JavaUdfNullTest, PredicateUdfNPEHandling) {
    // Create input and result types
    auto input_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());

    // Create test data that will trigger NPE
    std::vector<Field> input_data = {1, 2, 3, 4, 5};
    std::vector<UInt8> input_null_map = {0, 0, 1, 0, 0};  // 3rd value is null
    auto block = create_test_block_with_nulls(input_type, result_type, input_data, input_null_map);

    // Create and execute UDF that will throw NPE
    JavaFunctionCall udf(/* ... UDF parameters ... */);
    ASSERT_TRUE(udf.execute_impl(_function_context, block, {0}, 1, input_data.size()).ok());

    // Verify results - NPE should result in all values being false
    std::vector<UInt8> expected_values(input_data.size(), 0);
    std::vector<UInt8> expected_null_map(input_data.size(), 0);
    verify_column_values_and_nulls<UInt8>(*block.get_by_position(1).column,
                                         expected_values, expected_null_map);
}

// Test filter size mismatch handling
TEST_F(JavaUdfNullTest, FilterSizeMismatchHandling) {
    // Create input and result types
    auto input_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());

    // Create test data
    std::vector<Field> input_data = {1, 2, 3, 4, 5};
    std::vector<UInt8> input_null_map = {0, 0, 0, 0, 0};  // No nulls
    auto block = create_test_block_with_nulls(input_type, result_type, input_data, input_null_map);

    // Create and execute UDF that will return wrong filter size
    JavaFunctionCall udf(/* ... UDF parameters ... */);
    ASSERT_TRUE(udf.execute_impl(_function_context, block, {0}, 1, input_data.size()).ok());

    // Verify results - size mismatch should result in all values being false
    std::vector<UInt8> expected_values(input_data.size(), 0);
    std::vector<UInt8> expected_null_map(input_data.size(), 0);
    verify_column_values_and_nulls<UInt8>(*block.get_by_position(1).column,
                                         expected_values, expected_null_map);
}

// Test multiple column UDF with null handling
TEST_F(JavaUdfNullTest, MultiColumnNullHandling) {
    // Create input and result types
    auto input_type1 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto input_type2 = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    // Create test data for first column
    std::vector<Field> input_data1 = {1, 2, 3, 4, 5};
    std::vector<UInt8> input_null_map1 = {0, 1, 0, 0, 0};

    // Create test data for second column
    std::vector<Field> input_data2 = {10, 20, 30, 40, 50};
    std::vector<UInt8> input_null_map2 = {0, 0, 1, 0, 0};

    // Create block with both columns
    Block block;
    auto col1 = input_type1->create_column();
    auto col2 = input_type2->create_column();

    // Create nullable columns
    auto nullable_col1 = ColumnNullable::create(std::move(col1), ColumnUInt8::create());
    auto nullable_col2 = ColumnNullable::create(std::move(col2), ColumnUInt8::create());

    // Insert data
    for (size_t i = 0; i < input_data1.size(); ++i) {
        if (!input_null_map1[i]) {
            nullable_col1->insert(input_data1[i]);
        } else {
            nullable_col1->insert_default();
        }
        if (!input_null_map2[i]) {
            nullable_col2->insert(input_data2[i]);
        } else {
            nullable_col2->insert_default();
        }
    }

    block.insert({std::move(nullable_col1), input_type1, "input1"});
    block.insert({std::move(nullable_col2), input_type2, "input2"});
    block.insert({result_type->create_column(), result_type, "result"});

    // Create and execute UDF
    JavaFunctionCall udf(/* ... UDF parameters ... */);
    ASSERT_TRUE(udf.execute_impl(_function_context, block, {0, 1}, 2, input_data1.size()).ok());

    // Verify results - if either input is null, result should be null
    std::vector<Int32> expected_values = {11, 0, 0, 44, 55};
    std::vector<UInt8> expected_null_map = {0, 1, 1, 0, 0};
    verify_column_values_and_nulls<Int32>(*block.get_by_position(2).column,
                                         expected_values, expected_null_map);
}

} // namespace doris::vectorized 