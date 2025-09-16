#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> CountActionCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("CountActionCompute::compute - Counting " + std::to_string(inputData.size()) + " elements");
    
    // Count action returns the count as a single element
    std::vector<std::string> result;
    result.push_back(std::to_string(inputData.size()));
    
    SGXUtils::logInfo("CountActionCompute::compute - Count result: " + std::to_string(inputData.size()));
    return result;
}

} // namespace spark_sgx
