// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/learned_merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "mod/plr.h"
#include <iostream>
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
        keys_bounds_(std::vector<std::pair<uint64_t, uint64_t>>()),
        n_(n),
        current_(nullptr) {
    assert(false);
    for (int i = 0; i < n; i++) {

      children_[i].Set(children[i]);
      keys_data_.push_back(std::vector<std::string>());
      children_[i].SeekToFirst();
      // Handle case when children_[i] is empty.
      uint64_t prev_key = SliceToInteger(children_[i].key());
      keys_bounds_.push_back(
        std::pair<uint64_t, uint64_t>(
                  SliceToInteger(children_[i].key()), 0));

      while(children_[i].Valid()) {
        keys_data_[i].push_back(children_[i].key().ToString());
        keys_bounds_[i].second = (SliceToInteger(children_[i].key()));
        children_[i].Next();
        
        uint64_t key = SliceToInteger(children_[i].key());
        assert(key >= prev_key);
        prev_key = key;
      }
      children_[i].SeekToFirst();
      PLR plr = PLR(10);   //error=10
      std::vector<Segment> segs = plr.train(keys_data_[i]);
      keys_segments_.push_back(segs);
      for (auto& str : keys_segments_[i]) {
        std::cout<<str.x<<" "<<str.k<<" "<<str.b;
      }
      // TODO: train once, instead of every constructor call. For now, we just want something working.        
    }
  }
  
  ~LearnedMergingIterator() override { delete[] children_; }

  bool Valid() const override { return (current_ != nullptr); }

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

 private:
  void FindSmallest();
  bool GuessPosition(IteratorWrapper* iter, const Slice& guess_key,
                     const Comparator& comparator, std::string& limit);
  uint64_t GuessPositionFromPLR(
    const Slice& target_x, 
    const int iterator_index);
  uint64_t SliceToInteger(const Slice& slice);

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  std::vector<std::vector<std::string>> keys_data_;
  std::vector<std::vector<Segment>> keys_segments_;
  std::vector<std::pair<uint64_t, uint64_t>> keys_bounds_;
  int n_;
  IteratorWrapper* current_;
  // State variables to keep track of current segment.
  std::string limit_;
  bool is_last_segment_;

};

uint64_t LearnedMergingIterator::SliceToInteger(const Slice& slice) {
    const char* data = slice.data();
    size_t size = slice.size();
    uint64_t num = 0;
    bool leading_zeros = true;

    for (int i = 0; i < size; ++i) {
        int temp = data[i];
        if (leading_zeros && temp == '0') continue;
        leading_zeros = false;
        num = (num << 3) + (num << 1) + temp - 48;
    }
    return num;
}


uint64_t LearnedMergingIterator::GuessPositionFromPLR(
    const Slice& target_x, 
    const int iterator_index){

  const std::vector<Segment> segments = keys_segments_[iterator_index];
  uint64_t min_key = keys_bounds_[iterator_index].first; // TODO: fill the right value
  uint64_t max_key = keys_bounds_[iterator_index].second; // TODO: fill the right value
  uint64_t size = keys_data_[iterator_index].size();
  // check if the key is within the model bounds
  uint64_t target_int = SliceToInteger(target_x);
  if (target_int > max_key) return size;
  if (target_int < min_key) return 0;
  
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

  // calculate the interval according to the selected key segment
  double result =
      target_int * segments[left].k + segments[left].b;
  return floor(result);
}

bool LearnedMergingIterator::GuessPosition(IteratorWrapper* iter,
                                           const Slice& guess_key,
                                           const Comparator& comparator,
                                           std::string& limit) {
  std::string start_key(iter->key().ToString());
  iter->Seek(guess_key);
  while (iter->Valid() &&
         comparator.Compare(iter->key(), Slice(guess_key)) == 0) {
    iter->Next();
  }

  if (iter->Valid()) {
    limit.clear();
    limit.append(iter->key().ToString());
    iter->Seek(Slice(start_key));
    return true;
  } else {
    iter->Seek(Slice(start_key));
    assert(comparator.Compare(iter->key(), Slice(start_key)) == 0);
    return false;
  }
}

void LearnedMergingIterator::FindSmallest() {
  int smallest_iterator_index = 0;
  IteratorWrapper* smallest = nullptr;
  IteratorWrapper* second_smallest = nullptr;

  if (current_ != nullptr &&
      (current_->Valid()) &&
      (is_last_segment_ || comparator_->Compare(current_->key(), Slice(limit_)) < 0)) {
        return;
  }

  // Done with the current segment,
  // Now to find the next distinct range.
  is_last_segment_ = false;

  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
        smallest_iterator_index = i;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        second_smallest = smallest;
        smallest = child;
        smallest_iterator_index = i;
      } else if (second_smallest == nullptr ||
                 comparator_->Compare(child->key(), second_smallest->key()) <
                     0) {
        second_smallest = child;
      }
    }
  }

  current_ = smallest;

  if (smallest == nullptr) {
    return;  // no more ranges - not valid
  }

  // TODO: Find second_smallest()->key position in smallest using MLModel.Guess
  if (second_smallest == nullptr) {
    is_last_segment_ = true;
    return;
  }
  // TODO: Guard these with pound signs
  bool hasStrictlyGreaterKey =
      GuessPosition(smallest, second_smallest->key(), *comparator_, limit_);
  is_last_segment_ = !hasStrictlyGreaterKey;

  uint64_t approx_pos = GuessPositionFromPLR(second_smallest->key(),smallest_iterator_index);
  std::cout<<approx_pos<<"\n";
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
