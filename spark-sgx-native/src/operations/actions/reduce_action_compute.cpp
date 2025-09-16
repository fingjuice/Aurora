#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> ReduceActionCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("ReduceActionCompute::compute - Reducing " + std::to_string(inputData.size()) + " elements");
    
    std::vector<std::string> result;
    
    if (inputData.empty()) {
        SGXUtils::logWarning("ReduceActionCompute::compute - No elements to reduce");
        return result;
    }
    
    if (inputData.size() == 1) {
        result.push_back(inputData[0]);
        SGXUtils::logInfo("ReduceActionCompute::compute - Single element, returning as-is");
        return result;
    }
    
    // Simple reduce operation - concatenate all strings
    // In real implementation, this would use the provided reduce function
    std::string reduced = inputData[0];
    for (size_t i = 1; i < inputData.size(); ++i) {
        reduced += "_" + inputData[i];
    }
    
    result.push_back(reduced);
    
    SGXUtils::logInfo("ReduceActionCompute::compute - Reduced to: " + reduced);
    return result;
}

} // namespace spark_sgx
