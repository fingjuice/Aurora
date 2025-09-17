#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <unordered_set>

namespace spark_sgx {

std::vector<std::string> CountActionCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("CountActionCompute::compute - Starting count operation on " + std::to_string(inputData.size()) + " elements");
    
    try {
        // 解析操作数据获取计数参数
        bool countDistinct = false;
        bool countNonEmpty = false;
        std::string filterPattern = "";
        
        if (!operationData.empty()) {
            parseOperationData(operationData, countDistinct, countNonEmpty, filterPattern);
        }
        
        size_t count = 0;
        
        if (countDistinct) {
            // 计算唯一元素数量
            std::unordered_set<std::string> uniqueElements;
            for (const auto& item : inputData) {
                if (validateAndFilterItem(item, countNonEmpty, filterPattern)) {
                    uniqueElements.insert(item);
                }
            }
            count = uniqueElements.size();
            SGXUtils::logInfo("CountActionCompute::compute - Counted " + std::to_string(count) + " distinct elements");
        } else {
            // 计算总数量
            for (const auto& item : inputData) {
                if (validateAndFilterItem(item, countNonEmpty, filterPattern)) {
                    count++;
                }
            }
            SGXUtils::logInfo("CountActionCompute::compute - Counted " + std::to_string(count) + " total elements");
        }
        
        std::vector<std::string> result;
        result.push_back(std::to_string(count));
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("CountActionCompute::compute - Count operation completed in " + std::to_string(duration.count()) + "ms");
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("CountActionCompute::compute - Error during count operation: " + std::string(e.what()));
        throw;
    }
}

void CountActionCompute::parseOperationData(const std::string& operationData, 
                                          bool& countDistinct, 
                                          bool& countNonEmpty, 
                                          std::string& filterPattern) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "countDistinct:countNonEmpty:filterPattern"
        if (std::getline(iss, token, ':')) {
            countDistinct = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            countNonEmpty = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            filterPattern = token;
        }
    } catch (const std::exception& e) {
        SGXUtils::logWarning("CountActionCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

bool CountActionCompute::validateAndFilterItem(const std::string& item, 
                                             bool countNonEmpty, 
                                             const std::string& filterPattern) {
    // 基本验证
    if (item.empty()) {
        return !countNonEmpty; // 如果countNonEmpty为true，则跳过空项
    }
    
    // 模式过滤
    if (!filterPattern.empty()) {
        return item.find(filterPattern) != std::string::npos;
    }
    
    return true;
}

} // namespace spark_sgx
