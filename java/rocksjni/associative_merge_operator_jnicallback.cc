//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::CompactionFilterFactory.

#include "rocksjni/associative_merge_operator_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {

  AssociativeMergeOperatorJniCallback::AssociativeMergeOperatorJniCallback(
      JNIEnv* env, jobject jabstract_associative_merge_operator)
      : JniCallback(env, jabstract_associative_merge_operator) {

    // Note: The name of a AssociativeMergeOperatorFactory will not change during
    // it's lifetime, so we cache it in a global var
    jmethodID jname_method_id = AbstractAssociativeMergeOperatorFactoryJni::getNameMethodId(env);

    if(jname_method_id == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return;
    }

    jstring jname = (jstring)env->CallObjectMethod(m_jcallback_obj, jname_method_id);
    if(env->ExceptionCheck()) {
      // exception thrown
      return;
    }

    jboolean has_exception = JNI_FALSE;
    m_name = JniUtil::copyString(env, jname, &has_exception);  // also releases jname
    if (has_exception == JNI_TRUE) {
      // exception thrown
      return;
    }

    m_jcreate_compaction_filter_methodid =
        AbstractAssociativeMergeOperatorFactoryJni::getCreateCompactionFilterMethodId(env);
    if(m_jcreate_compaction_filter_methodid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return;
    }
  }

  const char* CompactionFilterFactoryJniCallback::Name() const {
    return m_name.get();
  }

  std::unique_ptr<AssociativeMergeOperator> AssociativeMergeOperatorFactoryJniCallback::CreateAssociativeMergeOperator() {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv* env = getJniEnv(&attached_thread);
    assert(env != nullptr);

    jlong associative_merge_operator = env->CallLongMethod(m_jcallback_obj,
                                                       m_jcreate_associative_merge_operator_methodid);

    if(env->ExceptionCheck()) {
      // exception thrown from CallLongMethod
      env->ExceptionDescribe();  // print out exception to stderr
      releaseJniEnv(attached_thread);
      return nullptr;
    }

    auto* cff = reinterpret_cast<AssociativeMergeOperator*>(associative_merge_operator);

    releaseJniEnv(attached_thread);

    return std::unique_ptr<AssociativeMergeOperator>(cff);
  }

  bool AssociativeMergeOperatorJniCallback::Merge(const Slice &key,
                                                  const Slice *existing_value,
                                                  const Slice &value,
                                                  std::string *new_value,
                                                  Logger *logger) const {
    jboolean attached_thread = JNI_FALSE;
    JNIEnv* env = getJniEnv(&attached_thread);
    if (env == NULL)
      return false;

    jbyteArray jb0, jb1, jb2;
    jbyte *buf0;
    jbyte *buf1;
    jbyte *buf2;

    size_t s0 = key.size() * sizeof(char);
    buf0 = (jbyte *) key.data();
    jb0 = env->NewByteArray(s0);
    env->SetByteArrayRegion(jb0, 0, s0, buf0);

    if (existing_value != NULL) {
      size_t s1 = existing_value->size() * sizeof(char);
      buf1 = (jbyte *) existing_value->data();
      jb1 = env->NewByteArray(s1);
      env->SetByteArrayRegion(jb1, 0, s1, buf1);
    } else {
      buf1 = NULL;
      jb1 = NULL;
    }

    size_t s2 = value.size() * sizeof(char);
    buf2 = (jbyte *) value.data();
    jb2 = env->NewByteArray(s2);
    env->SetByteArrayRegion(jb2, 0, s2, buf2);

    jobject rtobject = AbstractAssociativeMergeOperatorJni::getReturnTypeObject(env);
    jbyteArray jresult = AbstractAssociativeMergeOperatorJni::callObjectMethod(env, obj, jb0, jb1, jb2, rtobject);

    jthrowable ex = env->ExceptionOccurred();
    rocksdb::catchAndLog(env);
    env->DeleteLocalRef(jb0);
    rocksdb::catchAndLog(env);

    if (jb1 != NULL)
      env->DeleteLocalRef(jb1);
    env->DeleteLocalRef(jb2);
    rocksdb::catchAndLog(env);
    jboolean rtr = AbstractAssociativeMergeOperatorJni::getReturnTypeField(env, rtobject);
    env->DeleteLocalRef(rtobject);

    if (ex) {
      if (jresult != nullptr) {
        char *result = (char *) env->GetByteArrayElements(jresult, 0);
        env->ReleaseByteArrayElements(jresult, (jbyte *) result, rtr ? JNI_COMMIT : JNI_ABORT);
      }
      env->Throw(ex);
      releaseJniEnv(attached_thread);
      return false;
    } else {
      int len = env->GetArrayLength(jresult) / sizeof(char);
      char *result = (char *) env->GetByteArrayElements(jresult, 0);
      new_value->clear();
      new_value->assign(result, len);
      env->ReleaseByteArrayElements(jresult, (jbyte *) result, rtr ? JNI_COMMIT : JNI_ABORT);
      releaseJniEnv(attached_thread);
      return true;
    }
  }

}  // namespace rocksdb
