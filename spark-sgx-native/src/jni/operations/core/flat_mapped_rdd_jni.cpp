#include "jni/sgx_spark_jni.h"
#include "enclave/sgx_spark_enclave.h"
#include "utils/sgx_utils.h"
#include <jni.h>
#include <iostream>
#include <vector>
#include <string>

// External helper functions
extern std::vector<std::string> javaListToVector(JNIEnv *env, jobject list);
extern jobject vectorToJavaList(JNIEnv *env, const std::vector<std::string>& vec);
extern std::string javaStringToString(JNIEnv *env, jstring jStr);
extern SGXSparkEnclave* g_enclave;

// FlatMappedRDD JNI implementation
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteFlatMappedRDD(
    JNIEnv *env, jclass clazz, jobject inputData, jstring operationData) {
    
    try {
        if (g_enclave == nullptr || !g_enclave->isInitialized()) {
            throw std::runtime_error("Enclave not initialized");
        }
        
        std::vector<std::string> input = javaListToVector(env, inputData);
        std::string operation = javaStringToString(env, operationData);
        
        std::vector<std::string> result = g_enclave->executeCompute("FlatMappedRDD", input, operation);
        
        return vectorToJavaList(env, result);
        
    } catch (const std::exception& e) {
        SGXUtils::logError("JNI FlatMappedRDD failed: " + std::string(e.what()));
        return nullptr;
    }
}
