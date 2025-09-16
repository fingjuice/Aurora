#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> CollectActionCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("CollectActionCompute::compute - Collecting " + std::to_string(inputData.size()) + " elements");
    
    // Collect action simply returns all data
    std::vector<std::string> result = inputData;
    
    SGXUtils::logInfo("CollectActionCompute::compute - Collected " + std::to_string(result.size()) + " elements");
    return result;
}

} // namespace spark_sgx
