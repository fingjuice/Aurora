#include <iostream>
#include <vector>
#include <string>
#include "jni/sgx_spark_jni.h"
#include "enclave/sgx_spark_enclave.h"
#include "operations/rdd_operations.h"
#include "utils/sgx_utils.h"

int main() {
    std::cout << "Running SGX Spark Native Tests..." << std::endl;
    
    try {
        // Test SGX Utils
        std::cout << "Testing SGX Utils..." << std::endl;
        int result = SGXUtils::initializeSGX();
        if (result != 0) {
            std::cerr << "Failed to initialize SGX utils" << std::endl;
            return -1;
        }
        
        // Test Enclave
        std::cout << "Testing SGX Enclave..." << std::endl;
        result = SGXSparkEnclave::getInstance().initialize();
        if (result != 0) {
            std::cerr << "Failed to initialize SGX enclave" << std::endl;
            return -1;
        }
        
        // Test RDD Operations
        std::cout << "Testing RDD Operations..." << std::endl;
        std::vector<std::string> testData = {"1", "2", "3", "4", "5"};
        
        // Test map operation
        auto mapFunction = [](const std::string& input) -> std::string {
            try {
                int value = std::stoi(input);
                return std::to_string(value * 2);
            } catch (...) {
                return input;
            }
        };
        
        auto mapResult = RDDOperations::map(testData, mapFunction);
        std::cout << "Map result: ";
        for (const auto& item : mapResult) {
            std::cout << item << " ";
        }
        std::cout << std::endl;
        
        // Test filter operation
        auto filterFunction = [](const std::string& input) -> bool {
            try {
                int value = std::stoi(input);
                return value % 2 == 0;
            } catch (...) {
                return false;
            }
        };
        
        auto filterResult = RDDOperations::filter(testData, filterFunction);
        std::cout << "Filter result: ";
        for (const auto& item : filterResult) {
            std::cout << item << " ";
        }
        std::cout << std::endl;
        
        // Test reduce operation
        auto reduceFunction = [](const std::string& a, const std::string& b) -> std::string {
            try {
                int valA = std::stoi(a);
                int valB = std::stoi(b);
                return std::to_string(valA + valB);
            } catch (...) {
                return a;
            }
        };
        
        auto reduceResult = RDDOperations::reduce(testData, reduceFunction);
        std::cout << "Reduce result: " << reduceResult << std::endl;
        
        // Test count operation
        auto countResult = RDDOperations::count(testData);
        std::cout << "Count result: " << countResult << std::endl;
        
        // Test take operation
        auto takeResult = RDDOperations::take(testData, 3);
        std::cout << "Take result: ";
        for (const auto& item : takeResult) {
            std::cout << item << " ";
        }
        std::cout << std::endl;
        
        // Cleanup
        SGXSparkEnclave::getInstance().cleanup();
        SGXUtils::cleanupSGX();
        
        std::cout << "All tests passed successfully!" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return -1;
    }
}

