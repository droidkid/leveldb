// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "mod/learned_merger.h"
#include "mod/plr.h"
#include "mod/config.h"

#include <cmath>

namespace leveldb {

namespace {
class LearnedMergingIterator : public Iterator {
 public:
  LearnedMergingIterator(const Comparator* comparator, Iterator** children,
                         int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        keys_data_(std::vector<std::vector<std::string>>()),
        keys_segments_(std::vector<std::vector<Segment>>()),
        keys_consumed_(std::vector<uint64_t>()),
        n_(n),
        current_(nullptr) {

    stats_.num_items = 0;
    stats_.cdf_abs_error = 0;
    stats_.comp_count = 0;

    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);

      // TODO: Write out line_segments and data to a file.
      keys_data_.push_back(std::vector<std::string>());
      keys_consumed_.push_back(0);
      children_[i].SeekToFirst();
      while(children_[i].Valid()) {
        keys_data_[i].push_back(children_[i].key().ToString());
        children_[i].Next();
        stats_.num_items++;
      }
      children_[i].SeekToFirst();
      PLR plr = PLR(PLR_ERROR);
      std::vector<Segment> segs = plr.train(keys_data_[i]);
      keys_segments_.push_back(segs);
    }
  }
  
  ~LearnedMergingIterator() override { delete[] children_; }

  bool Valid() const override { 
    return (current_ != nullptr); 
  }

  void SeekToFirst() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    current_ = nullptr;
    FindSmallest();
  }

  void SeekToLast() override {
    assert(false);  // Not supported
  }

  void Seek(const Slice& target) override {
    assert(false);  // Not supported
  }

  void Next() override {
    assert(Valid());
    current_->Next();
    keys_consumed_[current_iterator_index_]++;
    FindSmallest();
  }

  void Prev() override {
    assert(false);  // Not supported
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

  MergerStats get_merger_stats() override {
    return stats_;
  }

 private:
  void FindSmallest();
  uint64_t GuessPositionFromPLR(
    const std::string target_key,
    const int iterator_index);

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  std::vector<std::vector<std::string>> keys_data_;
  std::vector<std::vector<Segment>> keys_segments_;
  std::vector<uint64_t> keys_consumed_;
  IteratorWrapper* current_;
  int current_iterator_index_;
  uint64_t current_key_limit_index_;
  MergerStats stats_;
};


uint64_t LearnedMergingIterator::GuessPositionFromPLR(
    const std::string target_key,
    const int iterator_index){

  const std::vector<Segment> segments = keys_segments_[iterator_index];
  std::string min_key = keys_data_[iterator_index].front();
  std::string max_key = keys_data_[iterator_index].back();
  uint64_t size = keys_data_[iterator_index].size();

  // check if the key is within the model bounds
  if (comparator_->Compare(target_key, max_key) > 0) return size;
  if (comparator_->Compare(target_key, min_key) < 0) return 0;

  uint64_t target_int = LdbKeyToInteger(target_key);
  
  // binary search between segments
  uint32_t left = 0, right = (uint32_t)segments.size() - 1;
  while (left != right - 1) {
    uint32_t mid = (right + left) / 2;
    //auto& str = keys_segments[mid];
    if (target_int < segments[mid].x)
      right = mid;
    else
      left = mid;
  }

  double result =
      target_int * segments[left].k + segments[left].b;
  return floor(result);
}

void LearnedMergingIterator::FindSmallest() {
  int smallest_iterator_index = 0;
  IteratorWrapper* smallest = nullptr;
  IteratorWrapper* second_smallest = nullptr;

  if (current_ != nullptr &&
      (current_->Valid()) &&
      (keys_consumed_[current_iterator_index_] < current_key_limit_index_)) {
        // TODO: Should this comparision be counted?
        return;
  }

  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
        smallest_iterator_index = i;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        stats_.comp_count+=1;
        second_smallest = smallest;
        smallest = child;
        smallest_iterator_index = i;
      } else if (second_smallest == nullptr ||
                 comparator_->Compare(child->key(), second_smallest->key()) <
                     0) {
        stats_.comp_count+=2;
        second_smallest = child;
      }
    }
  }

  current_ = smallest;
  current_iterator_index_ = smallest_iterator_index;

  if (smallest == nullptr) {
    return;
  }

  if (second_smallest == nullptr) {
    current_key_limit_index_ = keys_data_[smallest_iterator_index].size();
    return;
  }

  std::string target_key = second_smallest->key().ToString();
  uint64_t approx_pos = GuessPositionFromPLR(target_key, smallest_iterator_index);

  // Correcting error for approx_pos to be first key greater than target_int
  if (approx_pos < 0){
      approx_pos = 0;
  }
  if (approx_pos >= keys_data_[smallest_iterator_index].size()){
      approx_pos = keys_data_[smallest_iterator_index].size()-1;
  }

  // Move approx_pos to be at first value lesser than or equal to target.
  while(approx_pos >= 0 && 
    comparator_->Compare(
        Slice(keys_data_[smallest_iterator_index][approx_pos]), 
        Slice(target_key)
      ) > 0) {
      stats_.cdf_abs_error++;
      approx_pos--;
  }

  // Move approx_pos to be at first value greater than target.
  while(approx_pos < keys_data_[smallest_iterator_index].size() && 
   comparator_->Compare(
    Slice(keys_data_[smallest_iterator_index][approx_pos]), Slice(target_key)) < 0){
      stats_.cdf_abs_error++;
      approx_pos++;
  }

  current_key_limit_index_ = approx_pos;
}
}  // namespace

Iterator* NewLearnedMergingIterator(const Comparator* comparator,
                                    Iterator** children, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new LearnedMergingIterator(comparator, children, n);
  }
}

}  // namespace leveldb
