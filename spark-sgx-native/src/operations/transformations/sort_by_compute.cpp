#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>

namespace spark_sgx {

std::vector<std::string> SortByCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("SortByCompute::compute - Sorting " + std::to_string(inputData.size()) + " elements");
    
    // 解析操作数据获取排序参数
    bool ascending = true; // 默认升序
    int numPartitions = 1; // 默认分区数
    
    if (!operationData.empty()) {
        try {
            // 解析格式: "ascending:numPartitions"
            std::stringstream ss(operationData);
            std::string token;
            if (std::getline(ss, token, ':')) {
                ascending = (token == "true");
            }
            if (std::getline(ss, token, ':')) {
                numPartitions = std::stoi(token);
            }
        } catch (const std::exception& e) {
            SGXUtils::logWarning("SortByCompute::compute - Invalid operation data, using defaults");
        }
    }
    
    std::vector<std::string> result = inputData;
    
    if (result.empty()) {
        SGXUtils::logInfo("SortByCompute::compute - No data to sort");
        return result;
    }
    
    // 简单的字符串排序
    // 在实际实现中，这里应该使用用户提供的排序函数
    std::sort(result.begin(), result.end(), [ascending](const std::string& a, const std::string& b) {
        if (ascending) {
            return a < b;
        } else {
            return a > b;
        }
    });
    
    SGXUtils::logInfo("SortByCompute::compute - Sorted " + std::to_string(result.size()) + 
                     " elements (ascending: " + (ascending ? "true" : "false") + ")");
    return result;
}

} // namespace spark_sgx
