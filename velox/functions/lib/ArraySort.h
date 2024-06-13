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

#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {

/**
 * @param ascending If true, sort in ascending order; otherwise, sort in
 * descending order.
 * @param nullsFirst If true, nulls are placed first; otherwise, nulls are
 * placed last.
 * @param throwOnNestedNull If true, throw an exception if a nested null is
 * encountered.
 */
std::shared_ptr<exec::VectorFunction> makeArraySort(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config,
    bool ascending,
    bool nullsFirst,
    bool throwOnNestedNull);

/**
 * @param ascending If true, sort in ascending order; otherwise, sort in
 * descending order.
 * @param throwOnNestedNull If true, throw an exception if a nested null is
 * encountered.
 */
std::shared_ptr<exec::VectorFunction> makeArraySortLambdaFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config,
    bool ascending,
    bool throwOnNestedNull);

/**
 * @param withComparator If true, includes a signature for sorting with a
 * comparator function.
 */
std::vector<std::shared_ptr<exec::FunctionSignature>> arraySortSignatures(
    bool withComparator);

} // namespace facebook::velox::functions
