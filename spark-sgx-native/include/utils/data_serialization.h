#ifndef DATA_SERIALIZATION_H
#define DATA_SERIALIZATION_H

#include <string>
#include <vector>
#include <jni.h>

/**
 * Data serialization utilities for JNI communication
 */
namespace DataSerialization {
    
    // Convert Java List to C++ vector
    std::vector<jobject> javaListToVector(JNIEnv* env, jobject javaList);
    
    // Convert C++ vector to Java List
    jobject vectorToJavaList(JNIEnv* env, const std::vector<jobject>& data);
    
    // Convert Java object to string representation
    std::string javaObjectToString(JNIEnv* env, jobject obj);
    
    // Convert string to Java object (simplified)
    jobject stringToJavaObject(JNIEnv* env, const std::string& str);
    
    // Convert Java String to C++ string
    std::string javaStringToString(JNIEnv* env, jstring javaString);
    
    // Convert C++ string to Java String
    jstring stringToJavaString(JNIEnv* env, const std::string& str);
    
    // Serialize data for SGX processing
    std::string serializeData(const std::string& data);
    
    // Deserialize data from SGX processing
    std::string deserializeData(const std::string& serializedData);
    
    // Helper functions for type conversion
    bool isInteger(const std::string& str);
    bool isDouble(const std::string& str);
    bool isBoolean(const std::string& str);
    
    // Convert string to appropriate Java type
    jobject stringToTypedJavaObject(JNIEnv* env, const std::string& str);
}

#endif // DATA_SERIALIZATION_H

