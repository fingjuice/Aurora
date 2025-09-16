#include "jni/sgx_spark_jni.h"
#include "enclave/sgx_spark_enclave.h"
#include "utils/sgx_utils.h"
#include <jni.h>
#include <iostream>
#include <vector>
#include <string>

// Global enclave instance
SGXSparkEnclave* g_enclave = nullptr;

JNIEXPORT jboolean JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeInitialize(JNIEnv *env, jclass clazz) {
    try {
        if (g_enclave == nullptr) {
            g_enclave = &SGXSparkEnclave::getInstance();
        }
        
        int result = g_enclave->initialize();
        return (result == 0) ? JNI_TRUE : JNI_FALSE;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("JNI Initialize failed: " + std::string(e.what()));
        return JNI_FALSE;
    }
}

// Helper function to convert Java List to C++ vector
std::vector<std::string> javaListToVector(JNIEnv *env, jobject list) {
    std::vector<std::string> result;
    
    jclass listClass = env->GetObjectClass(list);
    jmethodID sizeMethod = env->GetMethodID(listClass, "size", "()I");
    jmethodID getMethod = env->GetMethodID(listClass, "get", "(I)Ljava/lang/Object;");
    
    jint size = env->CallIntMethod(list, sizeMethod);
    result.reserve(size);
    
    for (jint i = 0; i < size; i++) {
        jobject obj = env->CallObjectMethod(list, getMethod, i);
        jstring str = (jstring)obj;
        const char* cStr = env->GetStringUTFChars(str, nullptr);
        result.push_back(std::string(cStr));
        env->ReleaseStringUTFChars(str, cStr);
        env->DeleteLocalRef(obj);
    }
    
    env->DeleteLocalRef(listClass);
    return result;
}

// Helper function to convert C++ vector to Java List
jobject vectorToJavaList(JNIEnv *env, const std::vector<std::string>& vec) {
    jclass listClass = env->FindClass("java/util/ArrayList");
    jmethodID listConstructor = env->GetMethodID(listClass, "<init>", "()V");
    jmethodID addMethod = env->GetMethodID(listClass, "add", "(Ljava/lang/Object;)Z");
    
    jobject list = env->NewObject(listClass, listConstructor);
    
    for (const auto& str : vec) {
        jstring jStr = env->NewStringUTF(str.c_str());
        env->CallBooleanMethod(list, addMethod, jStr);
        env->DeleteLocalRef(jStr);
    }
    
    env->DeleteLocalRef(listClass);
    return list;
}

// Helper function to convert Java String to C++ string
std::string javaStringToString(JNIEnv *env, jstring jStr) {
    if (jStr == nullptr) {
        return "";
    }
    const char* cStr = env->GetStringUTFChars(jStr, nullptr);
    std::string result(cStr);
    env->ReleaseStringUTFChars(jStr, cStr);
    return result;
}
