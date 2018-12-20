/*
 * @author cristian.lorenzetto@gmail.com
 * */

#include "include/org_rocksdb_XORNioMergeOperator.h"

#include <assert.h>
#include <iostream>
#include <jni.h>
#include <memory>
#include <rocksjni/portal.h>
#include <rocksjni/init.h>
#include <string>
#include <chrono>


typedef std::chrono::high_resolution_clock  hires_clock;
typedef hires_clock::duration duration;
typedef std::chrono::nanoseconds nanoseconds;
typedef std::chrono::milliseconds milliseconds;
typedef std::chrono::seconds seconds;

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksjni/portal.h"
#include "rocksjni/init.h"
#include "util/logging.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

    namespace JNIXORNioMergeOperator {

        class JNIMergeOperator : public AssociativeMergeOperator {
        public:
            virtual bool Merge(const Slice &,
                               const Slice *existing_value,
                               const Slice &value,
                               std::string *new_value,
                               Logger */*logger*/) const override {
                if (existing_value == nullptr) {
                     new_value->clear();
                     new_value->assign(value.data(), value.size());
                     return true;
                }

                if (existing_value->size() != value.size())
                    return false;

                new_value->clear();
                new_value->resize(value.size());
                const char* e = existing_value->data();
                const char* v = value.data();
                for( unsigned int i = 0; i < value.size(); i++) {
                    (*new_value)[i] = e[i] ^ v[i];
                }
                return true;
            }

            virtual const char *Name() const override {
                return "JNIXORNioMergeOperator";
            }

            void destroy(JNIEnv *env) {
                env->DeleteGlobalRef(obj);
            }

            void init(JNIEnv *e, jobject hook) {
                obj = e->NewGlobalRef(hook);
            }

            private:
                jobject obj;
        };// end of class
    }
}

jboolean Java_org_rocksdb_XORNioMergeOperator_initOperator(
        JNIEnv *env, jobject hook, jlong jhandle) {
    std::shared_ptr <rocksdb::JNIXORNioMergeOperator::JNIMergeOperator> *op =
            reinterpret_cast<std::shared_ptr <rocksdb::JNIXORNioMergeOperator::JNIMergeOperator> *>(jhandle);
    op->get()->init(env, hook);
    return true;
}

jlong Java_org_rocksdb_XORNioMergeOperator_newOperator(
        JNIEnv * /*env*/, jclass /*jclazz*/) {
    std::shared_ptr <rocksdb::MergeOperator> p = std::make_shared<rocksdb::JNIXORNioMergeOperator::JNIMergeOperator>();
    auto *op = new std::shared_ptr<rocksdb::MergeOperator>(p);
    return reinterpret_cast<jlong>(op);
}

void Java_org_rocksdb_XORNioMergeOperator_disposeInternal(
        JNIEnv *env, jobject /*obj*/, jlong jhandle) {
    std::shared_ptr <rocksdb::JNIXORNioMergeOperator::JNIMergeOperator> *op =
            reinterpret_cast<std::shared_ptr <rocksdb::JNIXORNioMergeOperator::JNIMergeOperator> *>(jhandle);
    op->get()->destroy(env);
    delete op;
}
