//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tuple.h
//
// Identification: src/include/storage/table/tuple.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "catalog/schema.h"
#include "common/rid.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Tuple format:
 * ---------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD |
 * ---------------------------------------------------------------------
 */
class Tuple {
  friend class TablePage;
  friend class TableHeap;
  friend class TableIterator;

 public:
  // Default constructor (to create a dummy tuple)
  Tuple() = default;

  // constructor for table heap tuple
  explicit Tuple(RID rid) : rid_(rid) {}

  // constructor for creating a new tuple based on input value
  Tuple(const std::vector<Value> &values, const Schema *schema);

  // copy constructor, deep copy
  Tuple(const Tuple &other);

  // assign operator, deep copy
  auto operator=(const Tuple &other) -> Tuple &;

  ~Tuple() {
    if (allocated_) {
      delete[] data_;
    }
    allocated_ = false;
    data_ = nullptr;
  }
  // serialize tuple data
  void SerializeTo(char *storage) const;

  // deserialize tuple data(deep copy)
  void DeserializeFrom(const char *storage);

  // return RID of current tuple
  inline auto GetRid() const -> RID { return rid_; }

  // Get the address of this tuple in the table's backing store
  inline auto GetData() const -> char * { return data_; }

  // Get length of the tuple, including varchar legth
  inline auto GetLength() const -> uint32_t { return size_; }

  // Get the value of a specified column (const)
  // checks the schema to see how to return the Value.
  auto GetValue(const Schema *schema, uint32_t column_idx) const -> Value;

  // Generates a key tuple given schemas and attributes
  auto KeyFromTuple(const Schema &schema, const Schema &key_schema, const std::vector<uint32_t> &key_attrs) -> Tuple;

  // Is the column value null ?
  inline auto IsNull(const Schema *schema, uint32_t column_idx) const -> bool {
    Value value = GetValue(schema, column_idx);
    return value.IsNull();
  }
  inline auto IsAllocated() -> bool { return allocated_; }

  auto ToString(const Schema *schema) const -> std::string;

  static auto Join(const Tuple *left_tuple, const Schema &left_schema, const Tuple *right_tuple, const Schema &right_schema, const Schema &schema) -> Tuple {
    // Tuple tuple;
    std::vector<Value> values;
    uint32_t left_column_count = left_schema.GetColumnCount();
    uint32_t right_column_count = right_schema.GetColumnCount();
    uint32_t column_count = left_column_count + right_column_count;
    values.reserve(column_count);
    for(uint32_t i = 0; i < left_column_count; i++){
      values.emplace_back(left_tuple->GetValue(&left_schema, i));
    }

    if(right_tuple != nullptr) {
      for(uint32_t i = 0; i < right_column_count; i++){
        values.emplace_back(right_tuple->GetValue(&right_schema, i));
      }
    } else {
      for(uint32_t i = 0; i < right_column_count; i++){
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }
    }
    
    return {values, &schema};
  }

 private:
  // Get the starting storage address of specific column
  auto GetDataPtr(const Schema *schema, uint32_t column_idx) const -> const char *;

  bool allocated_{false};  // is allocated?
  RID rid_{};              // if pointing to the table heap, the rid is valid
  uint32_t size_{0};
  char *data_{nullptr};
};

}  // namespace bustub
