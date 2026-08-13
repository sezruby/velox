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

// Adapted from Apache Arrow.

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "velox/dwio/parquet/writer/arrow/Platform.h"

namespace facebook::velox::parquet::arrow {

class ColumnDescriptor;

/// Physical-size statistics for a column, as defined by the Parquet
/// SizeStatistics structure (PARQUET-2261). They let readers estimate the
/// memory needed to reconstruct the data and enable finer-grained filter
/// pushdown on nested columns.
struct PARQUET_EXPORT SizeStatistics {
  /// Number of physical bytes of BYTE_ARRAY data assuming no encoding,
  /// excluding the four-byte length prefix of each value. Only populated for
  /// columns whose physical type is BYTE_ARRAY.
  std::optional<int64_t> unencodedByteArrayDataBytes;

  /// Number of values observed at each repetition level, indexed by level, so
  /// its size is maxRepetitionLevel + 1. Empty when the column has no
  /// repetition levels.
  std::vector<int64_t> repetitionLevelHistogram;

  /// Number of values observed at each definition level, indexed by level, so
  /// its size is maxDefinitionLevel + 1. Empty when the column has no
  /// definition levels.
  std::vector<int64_t> definitionLevelHistogram;

  /// Returns whether any statistic is populated.
  bool isSet() const;

  /// Adds 'numBytes' to the unencoded BYTE_ARRAY byte count. Only valid when
  /// 'unencodedByteArrayDataBytes' is present.
  void incrementUnencodedByteArrayDataBytes(int64_t numBytes);

  /// Adds the statistics of 'other' into this instance element-wise. Throws if
  /// the two are not both sized for the same column.
  void merge(const SizeStatistics& other);

  /// Throws if the statistics are inconsistent with 'descr', i.e. the
  /// histogram sizes do not match the column levels or the byte count is set
  /// for a non-BYTE_ARRAY column.
  void validate(const ColumnDescriptor* descr) const;

  /// Zeroes all counts in place while keeping the current allocation and the
  /// presence of 'unencodedByteArrayDataBytes'.
  void reset();

  /// Creates an instance whose histograms and byte count are sized and enabled
  /// according to the levels and physical type of 'descr'.
  static std::unique_ptr<SizeStatistics> make(const ColumnDescriptor* descr);
};

/// Increments 'histogram[levels[i]]' for every level in 'levels'. 'histogram'
/// must be large enough to index the maximum level. A no-op when 'histogram'
/// is empty.
void updateLevelHistogram(
    const int16_t* levels,
    int64_t numLevels,
    std::vector<int64_t>& histogram);

} // namespace facebook::velox::parquet::arrow
