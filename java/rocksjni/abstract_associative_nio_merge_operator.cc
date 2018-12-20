/*
 * @author cristian.lorenzetto@gmail.com
 * */

#include "include/org_rocksdb_AbstractAssociativeNioMergeOperator.h"

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

    namespace JNIAbstractAssociativeNioMergeOperator {

        static jmethodID method;
        static jclass rtClass;
        static jfieldID rtField;
        static jmethodID rtConstructor;

        class JNIMergeOperator : public AssociativeMergeOperator {
        public:
            virtual bool Merge(const Slice &key,
                               const Slice *existing_value,
                               const Slice &value,
                               std::string *new_value,
                               Logger */*logger*/) const override {
                if (rocksdb::closing)
                    return false;
                JNIEnv *env = rocksdb::getEnv();

                if (env == NULL)
                    return false;

                jobject jkey, jexistingValue, jvalue;

                jkey = env->NewDirectByteBuffer(const_cast<void*>(reinterpret_cast<const void*>(key.data())),
                    static_cast<jlong>(key.size()));
                if (jkey == nullptr) {
                    std::cout << "Cannot allocate memory 1" << std::endl;
                    return false;
                }
                if (existing_value != nullptr) {
                    jexistingValue = env->NewDirectByteBuffer(const_cast<void*>(reinterpret_cast<const void*>(existing_value->data())),
                        static_cast<jlong>(existing_value->size()));
                    if (jexistingValue == nullptr) {
                        std::cout << "Cannot allocate memory 2" << std::endl;
                        env->DeleteLocalRef(jkey);
                        return false;
                    }

                } else
                    jexistingValue = nullptr;
                jvalue = env->NewDirectByteBuffer(const_cast<void*>(reinterpret_cast<const void*>(value.data())),
                    static_cast<jlong>(value.size()));
                if (jvalue == nullptr) {
                    std::cout << "Cannot allocate memory 3" << std::endl;
                    return false;
                }

                jobject rtobject = env->NewObject(rtClass, rtConstructor);

                jobject jresult = (jbyteArray) env->CallObjectMethod(obj,
                                                                        rocksdb::JNIAbstractAssociativeNioMergeOperator::method,
                                                                        jkey, jexistingValue, jvalue, rtobject);

                jthrowable ex = env->ExceptionOccurred();

                env->DeleteLocalRef(jkey);
                if (jexistingValue != nullptr)
                    env->DeleteLocalRef(jexistingValue);
                env->DeleteLocalRef(jvalue);

                jboolean rtr = env->GetBooleanField(rtobject, rtField);
                env->DeleteLocalRef(rtobject);

                bool returnValue = false;
                if (ex) {
                    if (jresult != nullptr) {
                        env->DeleteLocalRef(jresult);
                    }
                    env->Throw(ex);
                    returnValue = false;
                } else {
                    if (rtr == false) {
                        int len = env->GetArrayLength((jbyteArray)jresult) / sizeof(char);
                        new_value->clear();
                        char *result = (char *) env->GetPrimitiveArrayCritical((jbyteArray)jresult, 0);
                        new_value->assign(result, len);
                        env->ReleasePrimitiveArrayCritical((jbyteArray)jresult, (jbyte*) result, rtr ? JNI_COMMIT : JNI_ABORT);
                        env->DeleteLocalRef(jresult);
                        returnValue = true;
                        } else {

                        int len = env->GetDirectBufferCapacity(jresult) / sizeof(char);
                        new_value->clear();
                        char *result = (char*) env->GetDirectBufferAddress(jresult);
                        new_value->assign(result, len);
                        env->DeleteLocalRef(jresult);
                        returnValue = true;
                        }
                }
                return returnValue;
            }

            virtual const char *Name() const override {
                return "JNIAbstractAssociativeNioMergeOperator";
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

        static void staticDestroy(JNIEnv *env) {
            env->DeleteGlobalRef(reinterpret_cast<jobject>(JNIAbstractAssociativeNioMergeOperator::rtClass));
        }

        static void staticInit(JNIEnv *env) {
            jclass cls = env->FindClass("org/rocksdb/AbstractAssociativeNioMergeOperator");
            if (cls == nullptr)
                rocksdb::throwJavaLangError(env, "unable to find AbstractAssociativeNioMergeOperator");

            method = env->GetMethodID(cls, "merge", "(Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Lorg/rocksdb/ReturnType;)Ljava/lang/Object;");
            if (method == 0)
                rocksdb::throwJavaLangError(env, "unable to find method merge");

            jclass a = env->FindClass("org/rocksdb/ReturnType");
            if (a == 0)
                rocksdb::throwJavaLangError(env, "unable to find object org.rocksdb.ReturnType");
            else
                JNIAbstractAssociativeNioMergeOperator::rtClass = reinterpret_cast<jclass>(env->NewGlobalRef(a));
            rtConstructor = env->GetMethodID(rtClass, "<init>", "()V");
            if (rtConstructor == 0)
                rocksdb::throwJavaLangError(env, "unable to find field ReturnType.<init>");

            JNIAbstractAssociativeNioMergeOperator::rtField = env->GetFieldID(rtClass, "isArgumentReference", "Z");
            if (JNIAbstractAssociativeNioMergeOperator::rtField == 0)
                rocksdb::throwJavaLangError(env, "unable to find field ReturnType.isArgumentReference");
            rocksdb::catchAndLog(env);
        }
    }
}

initialize {
rocksdb::JNIInitializer::getInstance()->setLoader(&rocksdb::JNIAbstractAssociativeNioMergeOperator::staticInit);
rocksdb::JNIInitializer::getInstance()->setUnloader(&rocksdb::JNIAbstractAssociativeNioMergeOperator::staticDestroy);
}

jboolean Java_org_rocksdb_AbstractAssociativeNioMergeOperator_initOperator(
        JNIEnv *env, jobject hook, jlong jhandle) {
    std::shared_ptr <rocksdb::JNIAbstractAssociativeNioMergeOperator::JNIMergeOperator> *op =
            reinterpret_cast<std::shared_ptr <rocksdb::JNIAbstractAssociativeNioMergeOperator::JNIMergeOperator> *>(jhandle);
    op->get()->init(env, hook);
    return true;
}

jlong Java_org_rocksdb_AbstractAssociativeNioMergeOperator_newOperator(
        JNIEnv * /*env*/, jclass /*jclazz*/) {
    std::shared_ptr <rocksdb::MergeOperator> p = std::make_shared<rocksdb::JNIAbstractAssociativeNioMergeOperator::JNIMergeOperator>();
    auto *op = new std::shared_ptr<rocksdb::MergeOperator>(p);
    return reinterpret_cast<jlong>(op);
}

void Java_org_rocksdb_AbstractAssociativeNioMergeOperator_disposeInternal(
        JNIEnv *env, jobject /*obj*/, jlong jhandle) {
    std::shared_ptr <rocksdb::JNIAbstractAssociativeNioMergeOperator::JNIMergeOperator> *op =
            reinterpret_cast<std::shared_ptr <rocksdb::JNIAbstractAssociativeNioMergeOperator::JNIMergeOperator> *>(jhandle);
    op->get()->destroy(env);
    delete op;
}
