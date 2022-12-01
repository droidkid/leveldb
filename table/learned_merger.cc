// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/learned_merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "mod/plr.h"

namespace leveldb {

namespace {
class LearnedMergingIterator : public Iterator {
 public:
  LearnedMergingIterator(const Comparator* comparator, Iterator** children,
                         int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        keys_data_(std::vector<std::vector<std::string>>()),
        n_(n),
        current_(nullptr) {
    for (int i = 0; i < n; i++) {

      children_[i].Set(children[i]);
      keys_data_.push_back(std::vector<std::string>());
      children_[i].SeekToFirst();
      while(children_[i].Valid()) {
        keys_data_[i].push_back(children_[i].key().ToString());
        children_[i].Next();
      }
      children_[i].SeekToFirst();

      // TODO: train once, instead of every constructor call. For now, we just want something working.
      // <INSERT PLR TRAINING HERE>
        
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

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  std::vector<std::vector<std::string>> keys_data_;
  int n_;
  IteratorWrapper* current_;
  // State variables to keep track of current segment.
  std::string limit_;
  bool is_last_segment_;
};

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
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        second_smallest = smallest;
        smallest = child;
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
  bool hasStrictlyGreaterKey =
      GuessPosition(smallest, second_smallest->key(), *comparator_, limit_);
  is_last_segment_ = !hasStrictlyGreaterKey;
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
