#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <set>

namespace spark_sgx {

std::vector<std::string> IntersectionRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("IntersectionRDDCompute::compute - Processing " + std::to_string(inputData.size()) + " elements");
    
    // 解析操作数据获取另一个RDD的数据
    std::vector<std::string> otherRDDData = parseOtherRDDData(operationData);
    
    std::vector<std::string> result;
    std::vector<std::string> inputCopy = inputData;
    std::vector<std::string> otherCopy = otherRDDData;
    std::sort(inputCopy.begin(), inputCopy.end());
    std::sort(otherCopy.begin(), otherCopy.end());
    
    std::set_intersection(inputCopy.begin(), inputCopy.end(),
                        otherCopy.begin(), otherCopy.end(),
                        std::back_inserter(result));
    
    SGXUtils::logInfo("IntersectionRDDCompute::compute - Completed intersection of " + std::to_string(inputData.size()) + 
                     " and " + std::to_string(otherRDDData.size()) + " elements = " + std::to_string(result.size()) + " elements");
    return result;
}

// Helper function to parse other RDD data
std::vector<std::string> IntersectionRDDCompute::parseOtherRDDData(const std::string& operationData) {
    std::vector<std::string> result;
    if (!operationData.empty()) {
        // Split by delimiter (simplified)
        std::istringstream iss(operationData);
        std::string item;
        while (std::getline(iss, item, ',')) {
            if (!item.empty()) {
                result.push_back(item);
            }
        }
    }
    return result;
}

} // namespace spark_sgx
