#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> UnionRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("UnionRDDCompute::compute - Processing " + std::to_string(inputData.size()) + " elements");
    
    // 解析操作数据获取另一个RDD的数据
    std::vector<std::string> otherRDDData = parseOtherRDDData(operationData);
    
    std::vector<std::string> result;
    result.reserve(inputData.size() + otherRDDData.size());
    
    // 添加第一个RDD的所有元素
    result.insert(result.end(), inputData.begin(), inputData.end());
    
    // 添加第二个RDD的所有元素
    result.insert(result.end(), otherRDDData.begin(), otherRDDData.end());
    
    SGXUtils::logInfo("UnionRDDCompute::compute - Completed union of " + std::to_string(inputData.size()) + 
                     " and " + std::to_string(otherRDDData.size()) + " elements = " + std::to_string(result.size()) + " elements");
    return result;
}

// Helper function to parse other RDD data
std::vector<std::string> UnionRDDCompute::parseOtherRDDData(const std::string& operationData) {
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

