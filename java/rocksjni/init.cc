//
// Created by cristian on 27/01/18.
//
#include "include/org_rocksdb_util_Environment.h"

#include <assert.h>
#include <iostream>
#include <jni.h>
#include <pthread.h>
#include <rocksjni/init.h>
#include <vector>

#include "init.h"

namespace rocksdb {
    JavaVM *JVM;
#ifdef JNI_VERSION_1_10
    jint jvmVersion=JNI_VERSION_1_10;
#elif defined   JNI_VERSION_1_9
    jint jvmVersion=JNI_VERSION_1_9;
#elif defined   JNI_VERSION_1_8
    jint jvmVersion=JNI_VERSION_1_8;
#elif defined   JNI_VERSION_1_7
    jint jvmVersion=JNI_VERSION_1_7;
#elif defined   JNI_VERSION_1_6
    jint jvmVersion=JNI_VERSION_1_6;
#endif

    /**
     * Definitions for JNIInitializer
     */
    JNIInitializer * JNIInitializer::instance = nullptr;
    JNIInitializer::JNIInitializer() {
        loaders = new std::vector<callback> ();
        unloaders = new std::vector<callback> ();
    }

    void JNIInitializer::setLoader(callback func) {
        this->loaders->push_back(func);
    }

    void JNIInitializer::setUnloader(callback func) {
        this->unloaders->push_back(func);
    }

    /**
     * Global variables (!) FIXME : maybe in a JNIManager or something ?
     */
    int dbInstances = 0;

    pthread_key_t CURRENT_ATTACHED_ENV;

    struct JNI_CONTEXT {
        JNIEnv *env;
        bool attached;
    };

    /**
     * Helper for throwing a java.lang.Error exception
     * @param env
     * @param message
     */

    void throwJavaLangError(JNIEnv * env, const char * message) {
        jclass cls = env->FindClass("java/lang/Error");
        if (cls != nullptr)
            env->ThrowNew(cls, message);
    }

    void catchAndLog(JNIEnv *env) {
        jboolean hasException = env->ExceptionCheck();
        if (hasException == JNI_TRUE) {
            env->ExceptionDescribe();
            env->ExceptionClear();
        }
    }

    void _detachCurrentThread(void *addr) {
        std::cout<< "detaching threadid"<< pthread_self()<<" "<<addr <<" "<<static_cast<JNI_CONTEXT*>(addr)<<"\n";
        JNI_CONTEXT *context;
        context = static_cast<JNI_CONTEXT *>(addr);
        std::cout<< "context="<< context<<"\n";

        if (context != nullptr) {
            std::cout<< "detaching 1"<<context<<"\n";
            if (context->attached) {
                std::cout<< "deta execution"<<rocksdb::JVM<<"\n";
                assert(rocksdb::JVM != nullptr);
                rocksdb::JVM->DetachCurrentThread();
            }
            delete context;
        }
    }

    void detachCurrentThread() {
        JNI_CONTEXT *context = static_cast<JNI_CONTEXT *>(pthread_getspecific(rocksdb::CURRENT_ATTACHED_ENV));
        pthread_setspecific(rocksdb::CURRENT_ATTACHED_ENV, nullptr);
        _detachCurrentThread(context);
    }

    void destroyJNIContext() {
        std::cout << "destroy jni context\n";
        JNIEnv *env = rocksdb::getEnv();
        rocksdb::callback func;
        for (unsigned i = rocksdb::JNIInitializer::getInstance()->unloaders->size(); i-- > 0;) {
            func = (*rocksdb::JNIInitializer::getInstance()->unloaders)[i];
            (*func)(env);
        }
        rocksdb::JNIInitializer::getInstance()->unloaders->clear();
        rocksdb::detachCurrentThread();
    }

    JNIEnv *getEnv() {
        JNI_CONTEXT *context = (JNI_CONTEXT *) pthread_getspecific(rocksdb::CURRENT_ATTACHED_ENV);
        std::cout << "get env: threadid " << pthread_self() << "\n";
        if (context == NULL) {
            JNIEnv *env;
            int getEnvStat = rocksdb::JVM->GetEnv((void **) &env, rocksdb::jvmVersion);
            if (env == nullptr) {
                std::cerr << "JVM->GetEnv returned a NULL env" << std::endl;
                return nullptr;
            }
            rocksdb::catchAndLog(env);
            if (getEnvStat == JNI_EDETACHED) {
                // expensive operation : do not detach for every invocation
                if (rocksdb::JVM->AttachCurrentThread((void **) &env, NULL) != JNI_OK) {
                    rocksdb::throwJavaLangError(env, "unable to attach JNIEnv");
                    return nullptr;
                } else {
                    rocksdb::catchAndLog(env);
                    context = new JNI_CONTEXT();
                    std::cout << "attached" << context << "\n";
                    context->attached = true;
                    context->env = env;
                    pthread_setspecific(rocksdb::CURRENT_ATTACHED_ENV, context);
                    // atexit([] { detach_current_thread(NULL); });
                    //  std::cout << "4"<<context<< "\n";
                    return env;
                }
            } else if (getEnvStat == JNI_EVERSION) {
                rocksdb::throwJavaLangError(env, "unable to create JNIEnv:version not supported");
                return NULL;
            } else {//already attached
                rocksdb::catchAndLog(env);
                std::cout << " already attached\n";
                context = new JNI_CONTEXT();
                context->attached = false;
                context->env = env;
                pthread_setspecific(rocksdb::CURRENT_ATTACHED_ENV, context);
                //   atexit([] { detach_current_thread(NULL); });
                // std::cout << "4b "<<context<<"\n";
                std::cout << "threadid" << pthread_self() << "\n";
                return env;
            }
        } else {
            return context->env;
        }
    }
}

JNIEXPORT void JNICALL
Java_org_rocksdb_util_Environment_detachCurrentThreadIfPossible(JNIEnv
* /*env*/, jclass /*clazz*/) {
rocksdb::detachCurrentThread();
}

jint JNI_OnLoad(JavaVM *vm, void * /*reserved*/) {
    rocksdb::JVM = vm;
    pthread_key_create(&rocksdb::CURRENT_ATTACHED_ENV, rocksdb::_detachCurrentThread);
    JNIEnv * env = rocksdb::getEnv();
    std::cout << "########### nb loaders: " << rocksdb::JNIInitializer::getInstance()->loaders->size() << std::endl;
    std::cout << "########### nb unloaders: " << rocksdb::JNIInitializer::getInstance()->unloaders->size() << std::endl;
    for (auto const &func : *rocksdb::JNIInitializer::getInstance()->loaders) {
        (*func)(env);
        rocksdb::catchAndLog(env);
    }
    std::cout << "########### Clearing and detaching " << std::endl;
    rocksdb::JNIInitializer::getInstance()->loaders->clear();
    rocksdb::detachCurrentThread();
    return rocksdb::jvmVersion;
}

/*
 * fixed unload bug: this api is not called
 *
 * */

void JNI_OnUnload(JavaVM * /*vm*/, void * /*reserved*/) {
    rocksdb::JVM = nullptr;
}

