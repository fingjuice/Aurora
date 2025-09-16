#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>

namespace spark_sgx {

std::vector<std::string> TakeActionCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("TakeActionCompute::compute - Taking elements from " + std::to_string(inputData.size()) + " elements");
    
    // Parse the number of elements to take from operationData
    int n = 10; // default
    if (!operationData.empty()) {
        try {
            n = std::stoi(operationData);
        } catch (const std::exception& e) {
            SGXUtils::logWarning("TakeActionCompute::compute - Invalid operation data, using default n=10");
        }
    }
    
    std::vector<std::string> result;
    int takeCount = std::min(n, static_cast<int>(inputData.size()));
    
    for (int i = 0; i < takeCount; ++i) {
        result.push_back(inputData[i]);
    }
    
    SGXUtils::logInfo("TakeActionCompute::compute - Took " + std::to_string(result.size()) + " elements");
    return result;
}

} // namespace spark_sgx
