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
#pragma once

#include "velox/core/Expressions.h"
#include "velox/functions/lib/ArraySort.h"
#include "velox/functions/prestosql/SimpleComparisonChecker.h"

namespace facebook::velox::functions {

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
  if (inputArgs.size() == 2) {
    return makeArraySortLambdaFunction(name, inputArgs, config, true, true);
  }

  VELOX_CHECK_EQ(inputArgs.size(), 1);
  return makeArraySort(name, inputArgs, config, true, false, true);
}

std::shared_ptr<exec::VectorFunction> makeArraySortDesc(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  if (inputArgs.size() == 2) {
    return makeArraySortLambdaFunction(name, inputArgs, config, false, true);
  }

  VELOX_CHECK_EQ(inputArgs.size(), 1);
  return makeArraySort(name, inputArgs, config, false, false, true);
}

/// Analyzes array_sort(array, lambda) call to determine whether it can be
/// re-written into a simpler call that specifies sort-by expression.
///
/// For example, rewrites
///     array_sort(a, (x, y) -> if(length(x) < length(y), -1, if(length(x) >
///     length(y), 1, 0))
/// into
///     array_sort(a, x -> length(x))
///
/// Returns new expression or nullptr if rewrite is not possible.
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

  auto checker =
      std::make_unique<functions::prestosql::PrestoSimpleComparisonChecker>();

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

} // namespace facebook::velox::functions
