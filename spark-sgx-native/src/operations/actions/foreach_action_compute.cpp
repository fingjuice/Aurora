#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> ForeachActionCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("ForeachActionCompute::compute - Processing " + std::to_string(inputData.size()) + " elements");
    
    // Foreach action processes each element but doesn't return data
    // Here we simulate the foreach operation by logging each element
    for (size_t i = 0; i < inputData.size(); ++i) {
        // Simulate processing each element
        SGXUtils::logDebug("ForeachActionCompute::compute - Processing element " + std::to_string(i) + ": " + inputData[i]);
        
        // Every 1000 elements, log progress
        if ((i + 1) % 1000 == 0) {
            SGXUtils::logInfo("ForeachActionCompute::compute - Processed " + std::to_string(i + 1) + " elements");
        }
    }
    
    // Foreach returns empty result
    std::vector<std::string> result;
    
    SGXUtils::logInfo("ForeachActionCompute::compute - Completed processing " + std::to_string(inputData.size()) + " elements");
    return result;
}

} // namespace spark_sgx
