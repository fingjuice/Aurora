#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <chrono>

namespace spark_sgx {

std::vector<std::string> CollectActionCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("CollectActionCompute::compute - Starting collection of " + std::to_string(inputData.size()) + " elements");
    
    try {
        // 验证输入数据
        if (inputData.empty()) {
            SGXUtils::logWarning("CollectActionCompute::compute - No data to collect");
            return std::vector<std::string>();
        }
        
        // 解析操作数据获取收集参数
        bool sortResults = false;
        bool removeDuplicates = false;
        size_t maxElements = SIZE_MAX;
        
        if (!operationData.empty()) {
            parseOperationData(operationData, sortResults, removeDuplicates, maxElements);
        }
        
        std::vector<std::string> result;
        result.reserve(std::min(inputData.size(), maxElements));
        
        // 收集数据
        size_t collected = 0;
        for (const auto& item : inputData) {
            if (collected >= maxElements) {
                break;
            }
            
            // 验证数据项
            if (validateDataItem(item)) {
                result.push_back(item);
                collected++;
                
                // 每处理10000个元素记录一次进度
                if (collected % 10000 == 0) {
                    SGXUtils::logInfo("CollectActionCompute::compute - Collected " + std::to_string(collected) + " elements");
                }
            } else {
                SGXUtils::logWarning("CollectActionCompute::compute - Invalid data item skipped: " + item);
            }
        }
        
        // 去重（如果需要）
        if (removeDuplicates) {
            auto originalSize = result.size();
            std::sort(result.begin(), result.end());
            result.erase(std::unique(result.begin(), result.end()), result.end());
            SGXUtils::logInfo("CollectActionCompute::compute - Removed " + std::to_string(originalSize - result.size()) + " duplicates");
        }
        
        // 排序（如果需要）
        if (sortResults) {
            std::sort(result.begin(), result.end());
            SGXUtils::logInfo("CollectActionCompute::compute - Sorted results");
        }
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("CollectActionCompute::compute - Successfully collected " + std::to_string(result.size()) + 
                         " elements in " + std::to_string(duration.count()) + "ms");
        
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("CollectActionCompute::compute - Error during collection: " + std::string(e.what()));
        throw;
    }
}

void CollectActionCompute::parseOperationData(const std::string& operationData, 
                                            bool& sortResults, 
                                            bool& removeDuplicates, 
                                            size_t& maxElements) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "sort:removeDuplicates:maxElements"
        if (std::getline(iss, token, ':')) {
            sortResults = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            removeDuplicates = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            maxElements = std::stoull(token);
        }
    } catch (const std::exception& e) {
        SGXUtils::logWarning("CollectActionCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

bool CollectActionCompute::validateDataItem(const std::string& item) {
    // 基本验证：非空且长度合理
    return !item.empty() && item.length() < 1024 * 1024; // 最大1MB
}

} // namespace spark_sgx
