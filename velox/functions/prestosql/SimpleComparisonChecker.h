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

#include "velox/functions/lib/SimpleComparisonMatcher.h"

namespace facebook::velox::functions::prestosql {
using namespace facebook::velox::functions;

class PrestoSimpleComparisonChecker : public SimpleComparisonChecker {
 protected:
  MatcherPtr ifelse(
      const MatcherPtr& condition,
      const MatcherPtr& thenClause,
      const MatcherPtr& elseClause) override {
    return std::make_shared<IfMatcher>(
        std::vector<MatcherPtr>{condition, thenClause, elseClause});
  }

  MatcherPtr comparison(
      const std::string& prefix,
      const MatcherPtr& left,
      const MatcherPtr& right,
      std::string* op) override {
    return std::make_shared<ComparisonMatcher>(
        prefix, std::vector<MatcherPtr>{left, right}, op);
  }

  MatcherPtr anySingleInput(
      core::TypedExprPtr* expr,
      core::FieldAccessTypedExprPtr* input) override {
    return std::make_shared<AnySingleInputMatcher>(expr, input);
  }

  MatcherPtr comparisonConstant(int64_t* value) override {
    return std::make_shared<ComparisonConstantMatcher>(value);
  }

  std::string invert(const std::string& prefix, const std::string& op)
      override {
    return op == prefix + "lt" ? prefix + "gt" : prefix + "lt";
  }

  /// Returns true for a < b -> -1.
  bool isLessThen(
      const std::string& prefix,
      const std::string& operation,
      const core::FieldAccessTypedExprPtr& left,
      int64_t result,
      const std::string& inputLeft) override {
    std::string op =
        (left->name() == inputLeft) ? operation : invert(prefix, operation);

    if (op == prefix + "lt") {
      return result < 0;
    }

    return result > 0;
  }

  std::string eqName(const std::string& prefix) override {
    return prefix + "eq";
  }

 public:
  ~PrestoSimpleComparisonChecker() override = default;
};

} // namespace facebook::velox::functions::prestosql
