#include "enclave/sgx_spark_enclave.h"
#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>

SGXSparkEnclave& SGXSparkEnclave::getInstance() {
    static SGXSparkEnclave instance;
    return instance;
}

int SGXSparkEnclave::initialize() {
    if (m_initialized) {
        return 0;
    }
    
    try {
        // Initialize SGX utilities
        int result = SGXUtils::initializeSGX();
        if (result != 0) {
            SGXUtils::logError("Failed to initialize SGX utilities in enclave");
            return result;
        }
        
        // Create enclave (simplified - in real implementation, you would load an actual enclave)
        m_enclaveId = SGXUtils::createEnclave("spark_enclave.signed.so");
        if (!m_enclaveId) {
            SGXUtils::logError("Failed to create SGX enclave");
            return -1;
        }
        
        m_initialized = true;
        SGXUtils::logInfo("SGX Spark Enclave initialized successfully");
        return 0;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("Exception during enclave initialization: " + std::string(e.what()));
        return -1;
    }
}

void SGXSparkEnclave::cleanup() {
    if (!m_initialized) {
        return;
    }
    
    try {
        if (m_enclaveId) {
            SGXUtils::destroyEnclave(m_enclaveId);
            m_enclaveId = nullptr;
        }
        
        m_initialized = false;
        SGXUtils::logInfo("SGX Spark Enclave cleaned up successfully");
        
    } catch (const std::exception& e) {
        SGXUtils::logError("Exception during enclave cleanup: " + std::string(e.what()));
    }
}

bool SGXSparkEnclave::isInitialized() const {
    return m_initialized;
}

std::vector<std::string> SGXSparkEnclave::executeCompute(
    const std::string& rddType,
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    if (!m_initialized) {
        throw std::runtime_error("Enclave not initialized");
    }
    
    try {
        SGXUtils::logInfo("SGXSparkEnclave::executeCompute - Executing compute for RDD type: " + rddType + 
                         " with " + std::to_string(inputData.size()) + " input elements");
        
        // Create the appropriate compute implementation
        auto compute = spark_sgx::RDDComputeFactory::createCompute(rddType);
        if (!compute) {
            SGXUtils::logWarning("Unknown RDD type: " + rddType + ", returning original data");
            return inputData;
        }
        
        // Execute the compute operation
        std::vector<std::string> result = compute->compute(inputData, operationData);
        
        SGXUtils::logInfo("SGXSparkEnclave::executeCompute - Completed compute for RDD type: " + rddType + 
                         ", output size: " + std::to_string(result.size()));
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("Compute operation failed for RDD type " + rddType + ": " + std::string(e.what()));
        throw;
    }
}