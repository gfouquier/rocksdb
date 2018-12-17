//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::CompactionFilterFactory.

#ifndef ROCKSDB_ASSOCIATIVE_MERGE_OPERATOR_JNICALLBACK_H
#define ROCKSDB_ASSOCIATIVE_MERGE_OPERATOR_JNICALLBACK_H

#include <jni.h>
#include <memory>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksjni/jnicallback.h"
#include "rocksjni/portal.h"
#include "util/logging.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

class AssociativeMergeOperatorJniCallback : public JniCallback {
  public:
    AssociativeMergeOperatorJniCallback(
        JNIEnv* env, jobject jassociative_merge_operator);

    //virtual bool Merge(const Slice &key,
    //                   const Slice *existing_value,
    //                   const Slice &value,
    //                   std::string *new_value,
    //                   Logger */*logger*/) const;

    virtual std::unique_ptr<AssociativeMergeOperator> CreateAssociativeMergeOperator();

    virtual const char *Name() const override;
  private:
    std::unique_ptr<char[]> m_name;
    jmethodID m_jcreate_associative_merge_operator_methodid;
  };

}  //namespace rocksdb

#endif //ROCKSDB_ASSOCIATIVE_MERGE_OPERATOR_JNICALLBACK_H

