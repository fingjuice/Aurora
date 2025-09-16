#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> FirstActionCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("FirstActionCompute::compute - Getting first element from " + std::to_string(inputData.size()) + " elements");
    
    std::vector<std::string> result;
    
    if (!inputData.empty()) {
        result.push_back(inputData[0]);
        SGXUtils::logInfo("FirstActionCompute::compute - First element: " + inputData[0]);
    } else {
        SGXUtils::logWarning("FirstActionCompute::compute - No elements to get first from");
    }
    
    return result;
}

} // namespace spark_sgx
