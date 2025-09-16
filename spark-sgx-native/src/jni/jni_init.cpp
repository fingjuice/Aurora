#include "jni/sgx_spark_jni.h"
#include "enclave/sgx_spark_enclave.h"
#include "utils/sgx_utils.h"
#include <iostream>

extern "C" {

JNIEXPORT jint JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeInitialize(JNIEnv* env, jclass clazz) {
    try {
        SGXUtils::logInfo("Initializing SGX Spark JNI wrapper");
        return SGXSparkJNI::getInstance().initialize();
    } catch (const std::exception& e) {
        SGXUtils::logError("Failed to initialize SGX Spark JNI: " + std::string(e.what()));
        return -1;
    }
}

JNIEXPORT void JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeCleanup(JNIEnv* env, jclass clazz) {
    try {
        SGXUtils::logInfo("Cleaning up SGX Spark JNI wrapper");
        SGXSparkJNI::getInstance().cleanup();
    } catch (const std::exception& e) {
        SGXUtils::logError("Cleanup failed: " + std::string(e.what()));
    }
}

} // extern "C"
