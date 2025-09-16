#ifndef SGX_UTILS_H
#define SGX_UTILS_H

#include <string>

/**
 * SGX utility functions
 */
class SGXUtils {
public:
    // SGX initialization
    static int initializeSGX();
    static void* createEnclave(const std::string& enclavePath);
    static void destroyEnclave(void* enclaveId);
    
    // Logging
    static void logInfo(const std::string& message);
    static void logWarning(const std::string& message);
    static void logError(const std::string& message);
    static void logDebug(const std::string& message);
};

#endif // SGX_UTILS_H