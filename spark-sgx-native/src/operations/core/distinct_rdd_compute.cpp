#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <unordered_set>
#include <algorithm>
#include <chrono>
#include <functional>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> DistinctRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("DistinctRDDCompute::compute - Starting distinct operation on " + std::to_string(inputData.size()) + " elements");
    
    try {
        if (inputData.empty()) {
            SGXUtils::logInfo("DistinctRDDCompute::compute - No data to process");
            return std::vector<std::string>();
        }
        
        // 解析操作数据获取distinct配置
        bool caseSensitive = true;
        bool preserveOrder = true;
        bool validateData = true;
        size_t maxElements = 1000000; // 最大元素数限制
        std::string prefix = "";
        std::string suffix = "";
        bool trimWhitespace = false;
        bool normalizeCase = false;
        
        if (!operationData.empty()) {
            parseOperationData(operationData, caseSensitive, preserveOrder, validateData, maxElements, prefix, suffix, trimWhitespace, normalizeCase);
        }
        
        // 数据验证
        if (validateData) {
            validateInputData(inputData);
        }
        
        std::vector<std::string> result;
        result.reserve(inputData.size()); // 预分配内存
        
        size_t processedCount = 0;
        size_t duplicateCount = 0;
        size_t skippedCount = 0;
        
        if (preserveOrder) {
            // 保持顺序的去重
            std::unordered_set<std::string> seen;
            
            for (const auto& item : inputData) {
                if (processedCount >= maxElements) break;
                
                try {
                    std::string processedItem = processItem(item, prefix, suffix, trimWhitespace, normalizeCase, caseSensitive);
                    
                    if (validateData && !validateItem(processedItem)) {
                        SGXUtils::logWarning("DistinctRDDCompute::compute - Invalid item: " + item);
                        skippedCount++;
                        continue;
                    }
                    
                    if (seen.find(processedItem) == seen.end()) {
                        seen.insert(processedItem);
                        result.push_back(processedItem);
                        processedCount++;
                    } else {
                        duplicateCount++;
                    }
                    
                    // 每处理1000个元素记录一次进度
                    if (processedCount % 1000 == 0) {
                        SGXUtils::logInfo("DistinctRDDCompute::compute - Processed " + std::to_string(processedCount) + " unique elements");
                    }
                    
                } catch (const std::exception& e) {
                    SGXUtils::logError("DistinctRDDCompute::compute - Error processing item: " + std::string(e.what()));
                    skippedCount++;
                }
            }
        } else {
            // 不保持顺序，使用更高效的方法
            std::unordered_set<std::string> seen;
            std::vector<std::string> tempResult;
            tempResult.reserve(inputData.size());
            
            // 先处理所有数据
            for (const auto& item : inputData) {
                try {
                    std::string processedItem = processItem(item, prefix, suffix, trimWhitespace, normalizeCase, caseSensitive);
                    
                    if (validateData && !validateItem(processedItem)) {
                        SGXUtils::logWarning("DistinctRDDCompute::compute - Invalid item: " + item);
                        skippedCount++;
                        continue;
                    }
                    
                    if (seen.find(processedItem) == seen.end()) {
                        seen.insert(processedItem);
                        tempResult.push_back(processedItem);
                    } else {
                        duplicateCount++;
                    }
                    
                } catch (const std::exception& e) {
                    SGXUtils::logError("DistinctRDDCompute::compute - Error processing item: " + std::string(e.what()));
                    skippedCount++;
                }
            }
            
            // 如果需要排序，对结果进行排序
            if (preserveOrder) {
                std::sort(tempResult.begin(), tempResult.end());
            }
            
            result = std::move(tempResult);
            processedCount = result.size();
        }
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("DistinctRDDCompute::compute - Completed distinct operation: " + 
                         std::to_string(inputData.size()) + " -> " + std::to_string(result.size()) + 
                         " elements (processed: " + std::to_string(processedCount) + 
                         ", duplicates: " + std::to_string(duplicateCount) + 
                         ", skipped: " + std::to_string(skippedCount) + 
                         ") in " + std::to_string(duration.count()) + "ms");
        
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("DistinctRDDCompute::compute - Error during distinct operation: " + std::string(e.what()));
        throw;
    }
}

void DistinctRDDCompute::parseOperationData(const std::string& operationData, 
                                          bool& caseSensitive, 
                                          bool& preserveOrder, 
                                          bool& validateData, 
                                          size_t& maxElements, 
                                          std::string& prefix, 
                                          std::string& suffix, 
                                          bool& trimWhitespace, 
                                          bool& normalizeCase) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "caseSensitive:preserveOrder:validateData:maxElements:prefix:suffix:trimWhitespace:normalizeCase"
        if (std::getline(iss, token, ':')) {
            caseSensitive = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            preserveOrder = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            validateData = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            maxElements = std::stoull(token);
        }
        if (std::getline(iss, token, ':')) {
            prefix = token;
        }
        if (std::getline(iss, token, ':')) {
            suffix = token;
        }
        if (std::getline(iss, token, ':')) {
            trimWhitespace = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            normalizeCase = (token == "true");
        }
    } catch (const std::exception& e) {
        SGXUtils::logWarning("DistinctRDDCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

std::string DistinctRDDCompute::processItem(const std::string& item, 
                                          const std::string& prefix, 
                                          const std::string& suffix, 
                                          bool trimWhitespace, 
                                          bool normalizeCase, 
                                          bool caseSensitive) {
    std::string result = item;
    
    // 去除空白字符
    if (trimWhitespace) {
        result.erase(0, result.find_first_not_of(" \t\n\r"));
        result.erase(result.find_last_not_of(" \t\n\r") + 1);
    }
    
    // 大小写处理
    if (normalizeCase) {
        if (caseSensitive) {
            // 转换为小写
            std::transform(result.begin(), result.end(), result.begin(), ::tolower);
        } else {
            // 转换为大写
            std::transform(result.begin(), result.end(), result.begin(), ::toupper);
        }
    }
    
    // 添加前缀和后缀
    result = prefix + result + suffix;
    
    return result;
}

bool DistinctRDDCompute::validateItem(const std::string& item) {
    return !item.empty() && item.length() < 10000; // 基本验证
}

void DistinctRDDCompute::validateInputData(const std::vector<std::string>& data) {
    size_t invalidCount = 0;
    for (size_t i = 0; i < data.size(); ++i) {
        if (!validateItem(data[i])) {
            invalidCount++;
            if (invalidCount <= 5) { // 只记录前5个无效项
                SGXUtils::logWarning("DistinctRDDCompute::validateInputData - Invalid item at index " + 
                                   std::to_string(i) + ": " + data[i]);
            }
        }
    }
    
    if (invalidCount > 5) {
        SGXUtils::logWarning("DistinctRDDCompute::validateInputData - " + std::to_string(invalidCount) + 
                           " invalid items found");
    }
}

} // namespace spark_sgx

