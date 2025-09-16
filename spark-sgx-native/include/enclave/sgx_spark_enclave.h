#ifndef SGX_SPARK_ENCLAVE_H
#define SGX_SPARK_ENCLAVE_H

#include <vector>
#include <string>
#include <memory>
#include "operations/rdd_compute.h"

/**
 * SGX Enclave interface for Spark RDD operations
 */
class SGXSparkEnclave {
public:
    static SGXSparkEnclave& getInstance();
    
    // Enclave lifecycle
    int initialize();
    void cleanup();
    bool isInitialized() const;
    
    // Main compute method - entry point for all RDD operations
    std::vector<std::string> executeCompute(
        const std::string& rddType,
        const std::vector<std::string>& inputData,
        const std::string& operationData = "");

private:
    SGXSparkEnclave() = default;
    ~SGXSparkEnclave() = default;
    SGXSparkEnclave(const SGXSparkEnclave&) = delete;
    SGXSparkEnclave& operator=(const SGXSparkEnclave&) = delete;
    
    bool m_initialized = false;
    void* m_enclaveId = nullptr;
};

#endif // SGX_SPARK_ENCLAVE_H