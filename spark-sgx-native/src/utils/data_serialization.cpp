#include "utils/data_serialization.h"
#include "utils/sgx_utils.h"
#include <jni.h>
#include <sstream>
#include <algorithm>

namespace DataSerialization {

std::vector<jobject> javaListToVector(JNIEnv* env, jobject javaList) {
    std::vector<jobject> result;
    
    if (!javaList) {
        return result;
    }
    
    // Get List class and methods
    jclass listClass = env->GetObjectClass(javaList);
    jmethodID sizeMethod = env->GetMethodID(listClass, "size", "()I");
    jmethodID getMethod = env->GetMethodID(listClass, "get", "(I)Ljava/lang/Object;");
    
    if (!sizeMethod || !getMethod) {
        SGXUtils::logError("Failed to get List methods");
        return result;
    }
    
    // Get list size
    jint size = env->CallIntMethod(javaList, sizeMethod);
    
    // Extract elements
    for (jint i = 0; i < size; ++i) {
        jobject element = env->CallObjectMethod(javaList, getMethod, i);
        if (element) {
            result.push_back(element);
        }
    }
    
    return result;
}

jobject vectorToJavaList(JNIEnv* env, const std::vector<jobject>& data) {
    // Create ArrayList
    jclass arrayListClass = env->FindClass("java/util/ArrayList");
    jmethodID constructor = env->GetMethodID(arrayListClass, "<init>", "()V");
    jmethodID addMethod = env->GetMethodID(arrayListClass, "add", "(Ljava/lang/Object;)Z");
    
    if (!arrayListClass || !constructor || !addMethod) {
        SGXUtils::logError("Failed to get ArrayList methods");
        return nullptr;
    }
    
    jobject list = env->NewObject(arrayListClass, constructor);
    if (!list) {
        SGXUtils::logError("Failed to create ArrayList");
        return nullptr;
    }
    
    // Add elements
    for (const auto& element : data) {
        env->CallBooleanMethod(list, addMethod, element);
    }
    
    return list;
}

std::string javaObjectToString(JNIEnv* env, jobject obj) {
    if (!obj) {
        return "";
    }
    
    // Get object class
    jclass objClass = env->GetObjectClass(obj);
    
    // Try to get toString method
    jmethodID toStringMethod = env->GetMethodID(objClass, "toString", "()Ljava/lang/String;");
    if (toStringMethod) {
        jstring str = (jstring)env->CallObjectMethod(obj, toStringMethod);
        if (str) {
            return javaStringToString(env, str);
        }
    }
    
    // Fallback to class name
    jmethodID getClassMethod = env->GetMethodID(objClass, "getClass", "()Ljava/lang/Class;");
    if (getClassMethod) {
        jobject classObj = env->CallObjectMethod(obj, getClassMethod);
        if (classObj) {
            jclass classClass = env->GetObjectClass(classObj);
            jmethodID getNameMethod = env->GetMethodID(classClass, "getName", "()Ljava/lang/String;");
            if (getNameMethod) {
                jstring className = (jstring)env->CallObjectMethod(classObj, getNameMethod);
                if (className) {
                    return javaStringToString(env, className);
                }
            }
        }
    }
    
    return "Unknown";
}

jobject stringToJavaObject(JNIEnv* env, const std::string& str) {
    // Create a String object
    jstring javaString = stringToJavaString(env, str);
    return javaString;
}

std::string javaStringToString(JNIEnv* env, jstring javaString) {
    if (!javaString) {
        return "";
    }
    
    const char* str = env->GetStringUTFChars(javaString, nullptr);
    if (!str) {
        return "";
    }
    
    std::string result(str);
    env->ReleaseStringUTFChars(javaString, str);
    
    return result;
}

jstring stringToJavaString(JNIEnv* env, const std::string& str) {
    return env->NewStringUTF(str.c_str());
}

std::string serializeData(const std::string& data) {
    // Simple serialization - in practice, you might want to use more sophisticated methods
    return data;
}

std::string deserializeData(const std::string& serializedData) {
    // Simple deserialization - in practice, you might want to use more sophisticated methods
    return serializedData;
}

bool isInteger(const std::string& str) {
    if (str.empty()) {
        return false;
    }
    
    size_t start = 0;
    if (str[0] == '-' || str[0] == '+') {
        start = 1;
    }
    
    if (start >= str.length()) {
        return false;
    }
    
    return std::all_of(str.begin() + start, str.end(), ::isdigit);
}

bool isDouble(const std::string& str) {
    if (str.empty()) {
        return false;
    }
    
    size_t start = 0;
    if (str[0] == '-' || str[0] == '+') {
        start = 1;
    }
    
    if (start >= str.length()) {
        return false;
    }
    
    bool hasDot = false;
    for (size_t i = start; i < str.length(); ++i) {
        if (str[i] == '.') {
            if (hasDot) {
                return false; // Multiple dots
            }
            hasDot = true;
        } else if (!std::isdigit(str[i])) {
            return false;
        }
    }
    
    return true;
}

bool isBoolean(const std::string& str) {
    return str == "true" || str == "false" || str == "TRUE" || str == "FALSE";
}

jobject stringToTypedJavaObject(JNIEnv* env, const std::string& str) {
    // Try to determine the type and create appropriate Java object
    
    if (isInteger(str)) {
        // Create Integer object
        jclass integerClass = env->FindClass("java/lang/Integer");
        jmethodID constructor = env->GetMethodID(integerClass, "<init>", "(I)V");
        
        if (integerClass && constructor) {
            int value = std::stoi(str);
            return env->NewObject(integerClass, constructor, value);
        }
    } else if (isDouble(str)) {
        // Create Double object
        jclass doubleClass = env->FindClass("java/lang/Double");
        jmethodID constructor = env->GetMethodID(doubleClass, "<init>", "(D)V");
        
        if (doubleClass && constructor) {
            double value = std::stod(str);
            return env->NewObject(doubleClass, constructor, value);
        }
    } else if (isBoolean(str)) {
        // Create Boolean object
        jclass booleanClass = env->FindClass("java/lang/Boolean");
        jmethodID constructor = env->GetMethodID(booleanClass, "<init>", "(Z)V");
        
        if (booleanClass && constructor) {
            bool value = (str == "true" || str == "TRUE");
            return env->NewObject(booleanClass, constructor, value);
        }
    }
    
    // Default to String
    return stringToJavaString(env, str);
}

} // namespace DataSerialization

