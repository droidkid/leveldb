// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>
#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward),
        oracle_savings_(0),
        is_last_segment_(false)
         {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  ~MergingIterator() override { delete[] children_; }

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  void SeekToLast() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  void Seek(const Slice& target) override {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    // std::cout<<"Next now points to: "<<current_->key().ToString()<<std::endl;
    std::string before_key = std::string(current_->key().ToString());
    current_->Next(); // Consume the element.

    FindSmallest(); // Set current to next smallest iterator.
    
    if (Valid()) {
      assert(comparator_->Compare(current_->key(), Slice(before_key)) >= 0);
    }
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
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

  int64_t get_oracle_savings() override {
    return oracle_savings_;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  IteratorWrapper* current_;
  Direction direction_;
  int64_t oracle_savings_;
  // Add some state variables
  std::string limit_; 
  bool is_last_segment_;
};

void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  IteratorWrapper* second_smallest = nullptr;

  // TODO: If current is still smallest, just return current.
  if (current_ != nullptr && 
      (current_->Valid()) &&
      (is_last_segment_ || comparator_->Compare(current_->key(), Slice(limit_)) < 0)) {
        oracle_savings_ += (n_-1);
    return;
  }
  // We're done with our range, now we want to find the next distinct range.
  is_last_segment_ = false; // We don't know yet if we're in the last_segment_

  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        second_smallest = smallest;
        smallest = child;
      } else if (second_smallest==nullptr || comparator_->Compare(child->key(), second_smallest->key()) < 0) {
        second_smallest = child;
      }
    }
  }

  /* TODO: Remove the stat in Compaction Stats
  if (current_ == smallest) {
    // These are n comparisions that we could have skipped!
    oracle_savings_ += (n_-1);
  }
  */
  current_ = smallest;
  if(smallest == nullptr){
      return; //no more ranges - not valid
  }

  // TODO: Find second_smallest()->key position in smallest using MLModel.Guess
  // This is our range for which current is smallest.
  // Option 1 -> assume guess is always correct
   // limit = current_->guess(second_smallest->key());
  
  if (second_smallest == nullptr) {
    is_last_segment_ = true;
    return;
  }
  std::string second_smallest_key = second_smallest->key().ToString();
  bool found_range_limit = smallest->guess(second_smallest_key, *comparator_, limit_);
  is_last_segment_ = !found_range_limit;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

}  // namespace leveldb
