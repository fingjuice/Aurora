#ifndef SGX_SPARK_JNI_H
#define SGX_SPARK_JNI_H

#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

// JNI initialization
JNIEXPORT jboolean JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeInitialize(JNIEnv *env, jclass clazz);

// RDD compute operations
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteMapPartitionsRDD(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteFilteredRDD(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteFlatMappedRDD(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteUnionRDD(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteIntersectionRDD(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteDistinctRDD(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteGroupedRDD(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteReducedRDD(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteJoinedRDD(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);

// Action operations
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteCollectAction(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteCountAction(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteFirstAction(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteTakeAction(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteForeachAction(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteReduceAction(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);

// Transformation operations
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteMapValues(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteFlatMapValues(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteSample(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteSortBy(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteCoalesce(JNIEnv *env, jclass clazz, jobject inputData, jstring operationData);

#ifdef __cplusplus
}
#endif

#endif // SGX_SPARK_JNI_H