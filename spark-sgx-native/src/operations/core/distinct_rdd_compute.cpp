#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <unordered_set>

namespace spark_sgx {

std::vector<std::string> DistinctRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("DistinctRDDCompute::compute - Processing " + std::to_string(inputData.size()) + " elements");
    
    std::vector<std::string> result;
    std::unordered_set<std::string> seen;
    
    for (const auto& item : inputData) {
        if (seen.find(item) == seen.end()) {
            seen.insert(item);
            result.push_back(item);
        }
    }
    
    SGXUtils::logInfo("DistinctRDDCompute::compute - Completed distinct operation: " + 
                     std::to_string(inputData.size()) + " -> " + std::to_string(result.size()) + " elements");
    return result;
}

} // namespace spark_sgx

