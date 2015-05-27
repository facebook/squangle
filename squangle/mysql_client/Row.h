/*
 *  Copyright (c) 2015, Facebook, Inc.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree. An additional grant
 *  of patent rights can be found in the PATENTS file in the same directory.
 *
 */
//
// Classes representing rows and blocks of rows returned by a MySQL
// query.  Note a query can return multiple blocks.
//
// These classes make heavy use of StringPiece.  This means if you
// wish to store the data in the query, you must copy it out.  Once
// you lose your RowBlock, any Rows or StringPieces referencing it
// will be invalid.

#ifndef COMMON_ASYNC_MYSQL_ROW_H
#define COMMON_ASYNC_MYSQL_ROW_H

#include <chronos>
#include <vector>
#include <unordered_map>

#include <boost/iterator/iterator_facade.hpp>
#include <mysql.h>

#include <re2/re2.h>

#include "folly/Conv.h"
#include "folly/Format.h"
#include "folly/Hash.h"
#include "folly/Range.h"
#include "folly/experimental/StringKeyedUnorderedMap.h"

namespace facebook {
namespace common {
namespace mysql_client {

using folly::StringPiece;
using std::vector;
using std::string;

class RowBlock;

// A row of returned data.  This makes the columns available either
// positionally or by name, both via operator[].  In addition, the raw
// values are available via iteration.  A Row is only valid for as
// long as the RowBlock it belongs to is valid, so don't save these.
//
// Note that if multiple columns have the same name (as reported by
// the MySQL server when it returns rows), the column name access will
// return only one of them; to get all values, you should use the
// integer indexes in operator[] or iterate over the columns of the
// row directly.
class Row {
 public:
  Row(const RowBlock* row_block, size_t row_number);

  // L should be StringPiece, size_t, or convertible therefrom.  The
  // return value is converted with folly::to<T>.
  template <typename T, typename L>
  T get(const L& l) const;

  template <typename T, typename L>
  T getWithDefault(const L& l, const T d) const;

  // Vector-like and map-like access.  Note the above about ambiguity
  // for map access when column names conflict.
  size_t size() const;
  StringPiece operator[](size_t col) const;
  StringPiece operator[](StringPiece field) const;

  // Is the field nullable?
  bool isNull(size_t col) const;
  bool isNull(StringPiece field) const;

  // Our very simple iterator.  Just barely enough to support
  // range-based for loops.
  class Iterator
      : public boost::iterator_facade<Iterator,
                                      const StringPiece,
                                      boost::single_pass_traversal_tag,
                                      const StringPiece> {

   public:
    Iterator(const Row* row, size_t column_number)
        : row_(row), current_column_number_(column_number) {}

    void increment() { ++current_column_number_; }
    const StringPiece dereference() const {
      CHECK(current_column_number_ < row_->size());
      return row_->get<StringPiece>(current_column_number_);
    }
    bool equal(const Iterator& other) const {
      return (row_ == other.row_ &&
              current_column_number_ == other.current_column_number_);
    }

   private:
    const Row* row_;
    size_t current_column_number_;
  };

  Iterator begin() const;
  Iterator end() const;

 private:
  const RowBlock* row_block_; // unowned
  const size_t row_number_;
};

// RowFields encapsulates the data about the fields (name, flags, types).
class RowFields {
 public:
  RowFields(folly::StringKeyedUnorderedMap<int>&& field_name_map,
            std::vector<string>&& field_names,
            std::vector<uint64_t>&& mysql_field_flags,
            std::vector<enum_field_types>&& mysql_field_types)
      : num_fields_(field_names.size()),
        field_name_map_(std::move(field_name_map)),
        field_names_(std::move(field_names)),
        mysql_field_flags_(std::move(mysql_field_flags)),
        mysql_field_types_(std::move(mysql_field_types)) {}
  // Get the MySQL type of the field.
  enum_field_types getFieldType(size_t field_num) const {
    return mysql_field_types_[field_num];
  }

  // Ditto, but by name.
  enum_field_types getFieldType(StringPiece field_name) const {
    return mysql_field_types_[fieldIndex(field_name)];
  }

  // Get the MySQL flags of the field.
  uint64_t getFieldFlags(size_t field_num) const {
    return mysql_field_flags_[field_num];
  }

  // Ditto, but by name.
  uint64_t getFieldFlags(StringPiece field_name) const {
    return mysql_field_flags_[fieldIndex(field_name)];
  }

  // What is the name of the i'th column in the result set?
  StringPiece fieldName(size_t i) const { return field_names_[i]; }

  // How many fields and rows do we have?
  size_t numFields() const { return num_fields_; }

 private:
  size_t num_fields_;
  const folly::StringKeyedUnorderedMap<int> field_name_map_;
  const vector<string> field_names_;
  const std::vector<uint64_t> mysql_field_flags_;
  const std::vector<enum_field_types> mysql_field_types_;

  // Given a field_name, return the numeric column number, or die trying.
  size_t fieldIndex(StringPiece field_name) const {
    auto it = field_name_map_.find(field_name);
    if (it == field_name_map_.end()) {
      throw std::out_of_range(
          folly::format("Invalid field: {}", field_name).str());
    }
    return it->second;
  }

  friend class RowBlock;
};

std::chrono::system_clock::time_point parseDateTime(StringPiece datetime,
                                                    enum_field_types date_type);

std::chrono::microseconds parseTimeOnly(StringPiece mysql_time,
                                        enum_field_types field_type);

// A RowBlock holds the raw data from part of a MySQL result set.  It
// corresponds roughly to one set of rows (out of potentially many).
// The size of a block can vary based on the whims of the MySQL client
// and server, so don't count on how many you may get or how many rows
// are in each.
//
// Data layout tries to be efficient; values are packed into memory
// tightly and accessed via StringPieces and Rows that point into this
// block.  This prevents frequent allocations.  See data comments for
// details.
//
// Iterator access is provided as well, allowing for use cases like
//
// for (const auto& row : row_block) {
//   ...
// }
class RowBlock {
 public:
  class Iterator;

  explicit RowBlock(std::shared_ptr<RowFields>& row_fields)
      : row_fields_info_(row_fields) {}

  ~RowBlock() {}

  // Given a row N and column M, return a T corresponding to the Nth
  // row's Mth column.
  template <typename T>
  T getField(size_t row, size_t field_num) const;

  // Like above, but converting to the specified type T (using
  // folly::to<T>(StringPiece)).
  template <typename T>
  T getField(size_t row, StringPiece field_name) const;

  // Is this field NULL?
  bool isNull(size_t row, size_t field_num) const {
    return null_values_[row * row_fields_info_->numFields() + field_num];
  }

  // Ditto, but by name.
  bool isNull(size_t row, StringPiece field_name) const {
    return isNull(row, row_fields_info_->fieldIndex(field_name));
  }

  // Get the MySQL type of the field.
  enum_field_types getFieldType(size_t field_num) const {
    return row_fields_info_->getFieldType(field_num);
  }

  // Ditto, but by name.
  enum_field_types getFieldType(StringPiece field_name) const {
    return row_fields_info_->getFieldType(field_name);
  }

  // Get the MySQL flags of the field.
  uint64_t getFieldFlags(size_t field_num) const {
    return row_fields_info_->getFieldFlags(field_num);
  }

  // Ditto, but by name.
  uint64_t getFieldFlags(StringPiece field_name) const {
    return row_fields_info_->getFieldFlags(field_name);
  }

  // Access the Nth row of this row block as a Row object.
  Row getRow(size_t n) const { return Row(this, n); }

  RowFields* getRowFields() { return row_fields_info_.get(); }
  // What is the name of the i'th column in the result set?
  StringPiece fieldName(size_t i) const {
    return row_fields_info_->fieldName(i);
  }

  // Is our rowblock empty?
  bool empty() const { return field_offsets_.empty(); }

  // How many fields and rows do we have?
  size_t numFields() const { return row_fields_info_->numFields(); }

  // How many rows are in this RowBlock?
  size_t numRows() const {
    CHECK_EQ(0, field_offsets_.size() % row_fields_info_->numFields());
    return field_offsets_.size() / row_fields_info_->numFields();
  }

  // Iterator support.  Allows iteration over the rows in this block.
  // Like Row::Iterator, this is mainly for simple range-based for
  // iteration.
  class Iterator
      : public boost::iterator_facade<Iterator,
                                      const Row,
                                      boost::single_pass_traversal_tag,
                                      const Row> {

   public:
    Iterator(const RowBlock* row_block, size_t row_number)
        : row_block_(row_block), current_row_number_(row_number) {}

    void increment() { ++current_row_number_; }
    const Row dereference() const {
      return row_block_->getRow(current_row_number_);
    }
    bool equal(const Iterator& other) const {
      return (row_block_ == other.row_block_ &&
              current_row_number_ == other.current_row_number_);
    }

   private:
    const RowBlock* row_block_;
    size_t current_row_number_;
  };

  Iterator begin() const { return Iterator(this, 0); }

  Iterator end() const { return Iterator(this, numRows()); }

  // Functions called when building a RowBlock.  Not for general use.
  void startRow() {
    CHECK_EQ(0, field_offsets_.size() % row_fields_info_->numFields());
  }
  void finishRow() {
    CHECK_EQ(0, field_offsets_.size() % row_fields_info_->numFields());
  }
  void appendValue(const StringPiece value) {
    field_offsets_.push_back(buffer_.size());
    null_values_.push_back(false);
    buffer_.insert(buffer_.end(), value.begin(), value.end());
  }
  void appendNull() {
    field_offsets_.push_back(buffer_.size());
    null_values_.push_back(true);
  }

  // Let the compiler make our move operations.  We disallow copies below.
  RowBlock(RowBlock&&) = default;
  RowBlock& operator=(RowBlock&&) = default;

 private:
  time_t getDateField(size_t row, size_t field_num) const;

  bool isDate(size_t row, size_t field_num) const {
    switch (getFieldType(field_num)) {
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATE:
      return true;
    default:
      return false;
    }
  }

  // We represent the RowBlock as a vector of char's and offsets
  // inside of that vector.  The Nth row's Mth column's offset is
  // field_offsets_[N * num_fields + M] and extends to
  // field_offsets_[N * num_fields + M + 1] (or the end of the
  // buffer for the last row/column).
  vector<char> buffer_;
  vector<bool> null_values_;
  vector<size_t> field_offsets_;

  // field_name_map_ and field_names_ are owned by the RowFields shared between
  // RowBlocks of same query
  std::shared_ptr<RowFields> row_fields_info_;

  RowBlock(const RowBlock&) = delete;
  RowBlock& operator=(const RowBlock&) = delete;
};

// Declarations of specializations and trivial implementations.
template <>
StringPiece RowBlock::getField(size_t row, size_t field_num) const;

template <>
time_t RowBlock::getField(size_t row, size_t field_num) const;

template <>
std::chrono::system_clock::time_point RowBlock::getField(
    size_t row, size_t field_num) const;

template <>
std::chrono::microseconds RowBlock::getField(size_t row,
                                             size_t field_num) const;

template <typename T>
T RowBlock::getField(size_t row, size_t field_num) const {
  return folly::to<T>(getField<StringPiece>(row, field_num));
}

template <typename T>
T RowBlock::getField(size_t row, StringPiece field_name) const {
  return getField<T>(row, row_fields_info_->fieldIndex(field_name));
}

template <typename T, typename L>
T Row::get(const L& l) const {
  return row_block_->getField<T>(row_number_, l);
}

template <typename T, typename L>
T Row::getWithDefault(const L& l, const T d) const {
  if (isNull(l)) {
    return d;
  }
  return get<T>(l);
}
}
}
} // facebook::common::mysql_client

#endif // COMMON_ASYNC_MYSQL_ROW_H
