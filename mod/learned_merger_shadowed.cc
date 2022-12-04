// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/merger.h"
#include "mod/learned_merger.h"

#include <fstream>
#include <iostream>

namespace leveldb {

namespace {
// TODO: Verify this is a good name.
class LearnedMergingWithShadowIterator : public Iterator {

 public:
  LearnedMergingWithShadowIterator(const Comparator* comparator,
                                   Iterator** children,
                                   Iterator** shadow_children, int n)
      : mergingIterator_(NewMergingIterator(comparator, shadow_children, n)),
        learnedMergingIterator_(
            NewLearnedMergingIterator(comparator, children, n)) {}

  ~LearnedMergingWithShadowIterator() override {
    delete mergingIterator_;
    delete learnedMergingIterator_;
  }

  bool Valid() const override {
    assert(learnedMergingIterator_->Valid() == mergingIterator_->Valid());
    return mergingIterator_->Valid();
  }

  void SeekToFirst() override {
    mergingIterator_->SeekToFirst();
    learnedMergingIterator_->SeekToFirst();
  }

  void SeekToLast() override {
    assert(false);  // Not supported
  }

  void Seek(const Slice& target) override {
    assert(false);  // Not supported
  }

  void Next() override {
    mergingIterator_->Next();
    learnedMergingIterator_->Next();
  }

  void Prev() override {
    assert(false);  // Not supported
  }

  MergerStats get_merger_stats() override {
    MergerStats lm = learnedMergingIterator_->get_merger_stats();
    MergerStats m = mergingIterator_->get_merger_stats();
    assert(m.num_items == lm.num_items);

    std::ofstream stats;
    stats.open("stats.csv", std::ofstream::app);
    stats << lm.num_items<<",";
    stats << m.comp_count<<",";
    stats << lm.comp_count<<",";
    stats << lm.cdf_abs_error<<"\n";
    stats.close();

    return m;
  }

  Slice key() const override {
    if(mergingIterator_->key().compare(learnedMergingIterator_->key()) !=
           0) {
            std::cout<<mergingIterator_->key().ToString()<<std::endl;
            std::cout<<learnedMergingIterator_->key().ToString()<<std::endl;
            assert(false);
      }
    return mergingIterator_->key();
  }

  Slice value() const override {
    assert(mergingIterator_->value().compare(
               learnedMergingIterator_->value()) == 0);
    return mergingIterator_->value();
  }

  Status status() const override { return mergingIterator_->status(); }

 private:
  Iterator* mergingIterator_;
  Iterator* learnedMergingIterator_;
};
}  // namespace

Iterator* NewShadowedLearnedMergingIterator(const Comparator* comparator,
                                            Iterator** children,
                                            Iterator** shadow_children, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new LearnedMergingWithShadowIterator(comparator, children,
                                                shadow_children, n);
  }
}

}  // namespace leveldb
