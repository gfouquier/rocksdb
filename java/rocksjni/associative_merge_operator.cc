//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// rocksdb::AssociativeMergeOperator.

#include <jni.h>
#include <memory>


#include "include/org_rocksdb_AbstractAssociativeMergeOperatorFactory.h"
#include "rocksjni/associative_merge_operator_jnicallback.h"

/*
 * Class:     org_rocksdb_AbstractAssociativeMergeOperator
 * Method:    createNewAssociativeNergeOperator
 * Signature: ()J
 */
jlong Java_org_rocksdb_AbstractAssociativeMergeOperator_createNewAssociativeMergeOperator(JNIEnv* env, jobject jobj) {
  auto* amo = new rocksdb::AssociativeMergeOperatorJniCallback(env, jobj);
  auto* ptr_sptr_amo =
      new std::shared_ptr<rocksdb::AssociativeMergeOperatorJniCallback>(amo);
  return reinterpret_cast<jlong>(ptr_sptr_amo);
}

/*
 * Class:     org_rocksdb_AbstractAssociativeMergeOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractAssociativeMergeOperator_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* ptr_sptr_amo = reinterpret_cast<
      std::shared_ptr<rocksdb::AssociativeMergeOperatorJniCallback>*>(jhandle);
  delete ptr_sptr_amo;
}

