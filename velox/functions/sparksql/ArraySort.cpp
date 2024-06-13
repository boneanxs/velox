/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/functions/sparksql/ArraySort.h"
#include "velox/functions/sparksql/SimpleComparisonChecker.h"

namespace facebook::velox::functions::sparksql {
namespace {

core::CallTypedExprPtr asArraySortCall(
    const std::string& prefix,
    const core::TypedExprPtr& expr) {
  if (auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    if (call->name() == prefix + "array_sort") {
      return call;
    }
  }
  return nullptr;
}

} // namespace

std::shared_ptr<exec::VectorFunction> makeArraySortAsc(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  // If the second argument is present, it must be a lambda.
  if (inputArgs.size() == 2) {
    return makeArraySortLambdaFunction(name, inputArgs, config, true, false);
  }

  VELOX_CHECK_EQ(inputArgs.size(), 1);
  // Nulls are considered largest.
  return facebook::velox::functions::makeArraySort(
      name,
      inputArgs,
      config,
      /*ascending=*/true,
      /*nullsFirst=*/false,
      /*throwOnNestedNull=*/false);
}

std::shared_ptr<exec::VectorFunction> makeArraySortDesc(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  VELOX_CHECK_EQ(inputArgs.size(), 2);
  return makeArraySortLambdaFunction(name, inputArgs, config, false, false);
}

// Signatures:
//   array_sort_desc(array(T), function(T,U)) -> array(T)
std::vector<std::shared_ptr<exec::FunctionSignature>>
arraySortDescSignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .orderableTypeVariable("T")
          .orderableTypeVariable("U")
          .returnType("array(T)")
          .argumentType("array(T)")
          .constantArgumentType("function(T,U)")
          .build(),
  };
}

// Signatures:
//   sort_array(array(T)) -> array(T)
//   sort_array(array(T), boolean) -> array(T)
std::vector<std::shared_ptr<exec::FunctionSignature>> sortArraySignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .orderableTypeVariable("T")
          .argumentType("array(T)")
          .returnType("array(T)")
          .build(),
      exec::FunctionSignatureBuilder()
          .orderableTypeVariable("T")
          .argumentType("array(T)")
          .constantArgumentType("boolean")
          .returnType("array(T)")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeSortArray(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  VELOX_CHECK(
      inputArgs.size() == 1 || inputArgs.size() == 2,
      "Invalid number of arguments {}, expected 1 or 2",
      inputArgs.size());
  bool ascending = true;
  // Read optional sort ascending flag.
  if (inputArgs.size() == 2) {
    BaseVector* boolVector = inputArgs[1].constantValue.get();
    if (!boolVector || !boolVector->isConstantEncoding()) {
      VELOX_USER_FAIL(
          "{} requires a constant bool as the second argument.", name);
    }
    ascending = boolVector->as<ConstantVector<bool>>()->valueAt(0);
  }
  // Nulls are considered smallest.
  bool nullsFirst = ascending;
  return facebook::velox::functions::makeArraySort(
      name,
      inputArgs,
      config,
      /*ascending=*/ascending,
      /*nullsFirst=*/nullsFirst,
      /*throwOnNestedNull=*/false);
}

core::TypedExprPtr rewriteArraySortCall(
    const std::string& prefix,
    const core::TypedExprPtr& expr) {
  auto call = asArraySortCall(prefix, expr);
  if (call == nullptr || call->inputs().size() != 2) {
    return nullptr;
  }

  auto lambda =
      dynamic_cast<const core::LambdaTypedExpr*>(call->inputs()[1].get());
  VELOX_CHECK_NOT_NULL(lambda);

  // Extract 'transform' from the comparison lambda:
  //  (x, y) -> if(func(x) < func(y),...) ===> x -> func(x).
  if (lambda->signature()->size() != 2) {
    return nullptr;
  }

  static const std::string kNotSupported =
      "array_sort with comparator lambda that cannot be rewritten "
      "into a transform is not supported: {}";

  auto checker = std::make_unique<SparkSimpleComparisonChecker>();

  if (auto comparison = checker->isSimpleComparison(prefix, *lambda)) {
    std::string name = comparison->isLessThen ? prefix + "array_sort"
                                              : prefix + "array_sort_desc";

    if (!comparison->expr->type()->isOrderable()) {
      VELOX_USER_FAIL(kNotSupported, lambda->toString())
    }

    auto rewritten = std::make_shared<core::CallTypedExpr>(
        call->type(),
        std::vector<core::TypedExprPtr>{
            call->inputs()[0],
            std::make_shared<core::LambdaTypedExpr>(
                ROW({lambda->signature()->nameOf(0)},
                    {lambda->signature()->childAt(0)}),
                comparison->expr),
        },
        name);

    return rewritten;
  }

  VELOX_USER_FAIL(kNotSupported, lambda->toString())
}

} // namespace facebook::velox::functions::sparksql
