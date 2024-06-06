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

namespace facebook::velox::functions::sparksql {
using namespace facebook::velox::functions::lib;

class SparkComparisonMatcher : public Matcher {
 public:
  SparkComparisonMatcher(
      const std::string& prefix,
      std::vector<MatcherPtr> inputMatchers,
      std::string* op)
      : prefix_{prefix}, inputMatchers_{std::move(inputMatchers)}, op_{op} {
    VELOX_CHECK_EQ(2, inputMatchers_.size());
  }

  bool match(const core::TypedExprPtr& expr) override {
    if (auto call = dynamic_cast<const core::CallTypedExpr*>(expr.get())) {
      const auto& name = call->name();
      if (name == prefix_ + "equalto" || name == prefix_ + "lessthan" ||
          name == prefix_ + "greaterthan") {
        if (allMatch(call->inputs(), inputMatchers_)) {
          *op_ = name;
          return true;
        }
      }
    }
    return false;
  }

 private:
  const std::string prefix_;
  std::vector<MatcherPtr> inputMatchers_;
  std::string* op_;
};

class SparkSimpleComparisonChecker : public SimpleComparisonChecker {
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
    return std::make_shared<SparkComparisonMatcher>(
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
    return op == prefix + "lessthan" ? prefix + "greaterthan"
                                     : prefix + "lessthan";
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

    if (op == prefix + "lessthan") {
      return result < 0;
    }

    return result > 0;
  }

  std::string eqName(const std::string& prefix) override {
    return prefix + "equalto";
  }
};

} // namespace facebook::velox::functions::sparksql
