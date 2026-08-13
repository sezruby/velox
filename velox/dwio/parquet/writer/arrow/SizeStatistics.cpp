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

#include "velox/dwio/parquet/writer/arrow/SizeStatistics.h"

#include <algorithm>

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/parquet/writer/arrow/Exception.h"
#include "velox/dwio/parquet/writer/arrow/Schema.h"
#include "velox/dwio/parquet/writer/arrow/Types.h"

namespace facebook::velox::parquet::arrow {

namespace {

// Throws if the two histograms are not sized identically. An empty histogram
// matches any size so a producer that omits it does not fail the merge.
void checkMergeableHistogram(
    const std::vector<int64_t>& histogram,
    const std::vector<int64_t>& other) {
  if (histogram.size() != other.size()) {
    throw ParquetException("Cannot merge size statistics with different sizes");
  }
}

} // namespace

bool SizeStatistics::isSet() const {
  return unencodedByteArrayDataBytes.has_value() ||
      !repetitionLevelHistogram.empty() || !definitionLevelHistogram.empty();
}

void SizeStatistics::incrementUnencodedByteArrayDataBytes(int64_t numBytes) {
  VELOX_DCHECK(unencodedByteArrayDataBytes.has_value());
  unencodedByteArrayDataBytes = unencodedByteArrayDataBytes.value() + numBytes;
}

void SizeStatistics::merge(const SizeStatistics& other) {
  checkMergeableHistogram(
      repetitionLevelHistogram, other.repetitionLevelHistogram);
  checkMergeableHistogram(
      definitionLevelHistogram, other.definitionLevelHistogram);
  if (unencodedByteArrayDataBytes.has_value() !=
      other.unencodedByteArrayDataBytes.has_value()) {
    throw ParquetException(
        "Cannot merge size statistics with inconsistent unencoded byte array "
        "data bytes");
  }

  for (size_t i = 0; i < repetitionLevelHistogram.size(); ++i) {
    repetitionLevelHistogram[i] += other.repetitionLevelHistogram[i];
  }
  for (size_t i = 0; i < definitionLevelHistogram.size(); ++i) {
    definitionLevelHistogram[i] += other.definitionLevelHistogram[i];
  }
  if (unencodedByteArrayDataBytes.has_value()) {
    unencodedByteArrayDataBytes = unencodedByteArrayDataBytes.value() +
        other.unencodedByteArrayDataBytes.value();
  }
}

void SizeStatistics::validate(const ColumnDescriptor* descr) const {
  auto isValidSize = [](size_t size, int16_t maxLevel) {
    return size == 0 || size == static_cast<size_t>(maxLevel) + 1;
  };
  if (!isValidSize(
          repetitionLevelHistogram.size(), descr->maxRepetitionLevel())) {
    throw ParquetException("Repetition level histogram size mismatch");
  }
  if (!isValidSize(
          definitionLevelHistogram.size(), descr->maxDefinitionLevel())) {
    throw ParquetException("Definition level histogram size mismatch");
  }
  if (unencodedByteArrayDataBytes.has_value() &&
      descr->physicalType() != Type::kByteArray) {
    throw ParquetException(
        "Unencoded byte array data bytes must not be set for non-BYTE_ARRAY "
        "columns");
  }
}

void SizeStatistics::reset() {
  std::fill(
      repetitionLevelHistogram.begin(), repetitionLevelHistogram.end(), 0);
  std::fill(
      definitionLevelHistogram.begin(), definitionLevelHistogram.end(), 0);
  if (unencodedByteArrayDataBytes.has_value()) {
    unencodedByteArrayDataBytes = 0;
  }
}

std::unique_ptr<SizeStatistics> SizeStatistics::make(
    const ColumnDescriptor* descr) {
  auto sizeStats = std::make_unique<SizeStatistics>();
  // A histogram is only meaningful when the column has levels of that kind, so
  // a required or non-repeated column leaves the corresponding histogram empty.
  if (descr->maxDefinitionLevel() != 0) {
    sizeStats->definitionLevelHistogram.assign(
        descr->maxDefinitionLevel() + 1, 0);
  }
  if (descr->maxRepetitionLevel() != 0) {
    sizeStats->repetitionLevelHistogram.assign(
        descr->maxRepetitionLevel() + 1, 0);
  }
  if (descr->physicalType() == Type::kByteArray) {
    sizeStats->unencodedByteArrayDataBytes = 0;
  }
  return sizeStats;
}

void updateLevelHistogram(
    const int16_t* levels,
    int64_t numLevels,
    std::vector<int64_t>& histogram) {
  if (histogram.empty()) {
    return;
  }
  const auto maxLevel = static_cast<int16_t>(histogram.size() - 1);
  for (int64_t i = 0; i < numLevels; ++i) {
    const int16_t level = levels[i];
    VELOX_DCHECK_GE(level, 0);
    VELOX_DCHECK_LE(level, maxLevel);
    ++histogram[level];
  }
}

} // namespace facebook::velox::parquet::arrow
