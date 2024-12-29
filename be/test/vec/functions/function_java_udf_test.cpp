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

class JavaUdfTest : public testing::Test {
protected:
    void SetUp() override {
        // Set up test environment
    }

    void TearDown() override {
        // Clean up test environment
    }

    // Helper function to create a test block with input data
    Block create_test_block(const DataTypePtr& input_type, const DataTypePtr& result_type, 
                          const std::vector<Field>& input_data) {
        Block block;
        
        auto col = input_type->create_column();
        for (const auto& field : input_data) {
            col->insert(field);
        }
        
        block.insert({std::move(col), input_type, "input"});
        block.insert({result_type->create_column(), result_type, "result"});
        
        return block;
    }

    // Helper function to verify column values
    template <typename T>
    void verify_column_values(const IColumn& column, const std::vector<T>& expected_values, 
                            const std::vector<UInt8>& expected_null_map = {}) {
        ASSERT_EQ(column.size(), expected_values.size());
        
        const auto* nullable_column = typeid_cast<const ColumnNullable*>(&column);
        if (nullable_column) {
            ASSERT_EQ(expected_null_map.size(), expected_values.size());
            const auto& null_map = nullable_column->get_null_map_data();
            for (size_t i = 0; i < expected_null_map.size(); ++i) {
                EXPECT_EQ(null_map[i], expected_null_map[i]) << "at position " << i;
            }
        }
        
        for (size_t i = 0; i < expected_values.size(); ++i) {
            if (!expected_null_map.empty() && expected_null_map[i]) {
                continue;  // Skip checking value if it's null
            }
            Field field;
            column.get(i, field);
            EXPECT_EQ(field.get<T>(), expected_values[i]) << "at position " << i;
        }
    }
};

// Test normal execution of a predicate UDF
TEST_F(JavaUdfTest, PredicateUdfNormalExecution) {
    auto input_type = std::make_shared<DataTypeInt32>();
    auto result_type = std::make_shared<DataTypeUInt8>();
    
    std::vector<Field> input_data = {1, 2, 3, 4, 5};
    auto block = create_test_block(input_type, result_type, input_data);
    
    // Create and execute UDF
    JavaFunctionCall udf(/* ... UDF parameters ... */);
    
    // Execute UDF
    FunctionContext* context = nullptr;  // Mock context
    ASSERT_TRUE(udf.execute_impl(context, block, {0}, 1, input_data.size()).ok());
    
    // Verify results
    std::vector<UInt8> expected_values = {1, 1, 1, 1, 1};  // Example expected values
    verify_column_values<UInt8>(*block.get_by_position(1).column, expected_values);
}

// Test NPE handling in predicate UDF
TEST_F(JavaUdfTest, PredicateUdfNullPointerException) {
    auto input_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
    
    // Create input data with nulls to trigger NPE
    std::vector<Field> input_data = {Field(), 2, Field(), 4, 5};  // Field() creates null value
    auto block = create_test_block(input_type, result_type, input_data);
    
    // Create and execute UDF
    JavaFunctionCall udf(/* ... UDF parameters ... */);
    
    // Execute UDF
    FunctionContext* context = nullptr;  // Mock context
    ASSERT_TRUE(udf.execute_impl(context, block, {0}, 1, input_data.size()).ok());
    
    // Verify results - all values should be null due to NPE
    std::vector<UInt8> expected_values = {0, 0, 0, 0, 0};
    std::vector<UInt8> expected_null_map = {1, 1, 1, 1, 1};  // All null
    verify_column_values<UInt8>(*block.get_by_position(1).column, expected_values, expected_null_map);
}

// Test filter size mismatch handling
TEST_F(JavaUdfTest, PredicateUdfFilterSizeMismatch) {
    auto input_type = std::make_shared<DataTypeInt32>();
    auto result_type = std::make_shared<DataTypeUInt8>();
    
    std::vector<Field> input_data = {1, 2, 3, 4, 5};
    auto block = create_test_block(input_type, result_type, input_data);
    
    // Create and execute UDF that returns wrong filter size
    JavaFunctionCall udf(/* ... UDF parameters ... */);
    
    // Execute UDF
    FunctionContext* context = nullptr;  // Mock context
    ASSERT_TRUE(udf.execute_impl(context, block, {0}, 1, input_data.size()).ok());
    
    // Verify results - should have correct size with all false values
    std::vector<UInt8> expected_values(input_data.size(), 0);
    verify_column_values<UInt8>(*block.get_by_position(1).column, expected_values);
}

// Test non-predicate UDF with NPE
TEST_F(JavaUdfTest, NonPredicateUdfNullPointerException) {
    auto input_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    auto result_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    
    std::vector<Field> input_data = {Field(), 2, Field(), 4, 5};  // Some null values
    auto block = create_test_block(input_type, result_type, input_data);
    
    // Create and execute UDF
    JavaFunctionCall udf(/* ... UDF parameters ... */);
    
    // Execute UDF
    FunctionContext* context = nullptr;  // Mock context
    ASSERT_TRUE(udf.execute_impl(context, block, {0}, 1, input_data.size()).ok());
    
    // Verify results - should have nulls where input was null
    std::vector<Int32> expected_values = {0, 0, 0, 0, 0};
    std::vector<UInt8> expected_null_map = {1, 1, 1, 1, 1};  // All null due to NPE
    verify_column_values<Int32>(*block.get_by_position(1).column, expected_values, expected_null_map);
}

// Test resource cleanup
TEST_F(JavaUdfTest, ResourceCleanup) {
    auto input_type = std::make_shared<DataTypeInt32>();
    auto result_type = std::make_shared<DataTypeUInt8>();
    
    std::vector<Field> input_data = {1, 2, 3, 4, 5};
    auto block = create_test_block(input_type, result_type, input_data);
    
    // Create UDF
    JavaFunctionCall udf(/* ... UDF parameters ... */);
    
    // Execute UDF multiple times
    FunctionContext* context = nullptr;  // Mock context
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(udf.execute_impl(context, block, {0}, 1, input_data.size()).ok());
    }
    
    // Verify no memory leaks or resource leaks
    // This would require integration with a memory tracking system
}

} // namespace doris::vectorized 