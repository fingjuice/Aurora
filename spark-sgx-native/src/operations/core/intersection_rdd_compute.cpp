#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <set>
#include <unordered_set>
#include <chrono>

namespace spark_sgx {

std::vector<std::string> IntersectionRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("IntersectionRDDCompute::compute - Starting intersection operation on " + std::to_string(inputData.size()) + " elements");
    
    try {
        if (inputData.empty()) {
            SGXUtils::logInfo("IntersectionRDDCompute::compute - No input data to intersect");
            return std::vector<std::string>();
        }
        
        // 解析操作数据获取intersection配置
        std::string delimiter = ",";
        bool removeDuplicates = true;
        bool preserveOrder = false;
        bool validateData = true;
        size_t maxElements = 1000000; // 最大元素数限制
        std::string prefix = "";
        std::string suffix = "";
        bool caseSensitive = true;
        
        if (!operationData.empty()) {
            parseOperationData(operationData, delimiter, removeDuplicates, preserveOrder, validateData, maxElements, prefix, suffix, caseSensitive);
        }
        
        // 解析其他RDD数据
        std::vector<std::string> otherRDDData = parseOtherRDDData(operationData, delimiter);
        
        if (otherRDDData.empty()) {
            SGXUtils::logWarning("IntersectionRDDCompute::compute - No other RDD data to intersect with");
            return std::vector<std::string>();
        }
        
        // 数据验证
        if (validateData) {
            validateInputData(inputData, "first RDD");
            validateInputData(otherRDDData, "second RDD");
        }
        
        // 处理数据（添加前缀后缀，大小写转换等）
        std::vector<std::string> processedInput = processData(inputData, prefix, suffix, caseSensitive);
        std::vector<std::string> processedOther = processData(otherRDDData, prefix, suffix, caseSensitive);
        
        std::vector<std::string> result;
        result.reserve(std::min(processedInput.size(), processedOther.size()));
        
        size_t processedCount = 0;
        size_t duplicateCount = 0;
        size_t skippedCount = 0;
        
        // 使用set来优化查找性能
        std::unordered_set<std::string> otherSet(processedOther.begin(), processedOther.end());
        
        if (removeDuplicates) {
            // 需要去重，使用set来跟踪已处理的元素
            std::unordered_set<std::string> seen;
            
            for (const auto& item : processedInput) {
                if (processedCount >= maxElements) break;
                
                if (validateData && !validateItem(item)) {
                    SGXUtils::logWarning("IntersectionRDDCompute::compute - Invalid item in first RDD: " + item);
                    skippedCount++;
                    continue;
                }
                
                if (otherSet.find(item) != otherSet.end() && seen.find(item) == seen.end()) {
                    seen.insert(item);
                    result.push_back(item);
                    processedCount++;
                } else if (otherSet.find(item) != otherSet.end()) {
                    duplicateCount++;
                }
            }
        } else {
            // 不需要去重，直接查找
            for (const auto& item : processedInput) {
                if (processedCount >= maxElements) break;
                
                if (validateData && !validateItem(item)) {
                    SGXUtils::logWarning("IntersectionRDDCompute::compute - Invalid item in first RDD: " + item);
                    skippedCount++;
                    continue;
                }
                
                if (otherSet.find(item) != otherSet.end()) {
                    result.push_back(item);
                    processedCount++;
                }
            }
        }
        
        // 如果需要保持顺序，对结果进行排序
        if (preserveOrder) {
            std::sort(result.begin(), result.end());
        }
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("IntersectionRDDCompute::compute - Completed intersection: " + 
                         std::to_string(inputData.size()) + " ∩ " + std::to_string(otherRDDData.size()) + 
                         " -> " + std::to_string(result.size()) + " elements (processed: " + 
                         std::to_string(processedCount) + ", duplicates: " + std::to_string(duplicateCount) + 
                         ", skipped: " + std::to_string(skippedCount) + ") in " + 
                         std::to_string(duration.count()) + "ms");
        
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("IntersectionRDDCompute::compute - Error during intersection operation: " + std::string(e.what()));
        throw;
    }
}

void IntersectionRDDCompute::parseOperationData(const std::string& operationData, 
                                              std::string& delimiter, 
                                              bool& removeDuplicates, 
                                              bool& preserveOrder, 
                                              bool& validateData, 
                                              size_t& maxElements, 
                                              std::string& prefix, 
                                              std::string& suffix, 
                                              bool& caseSensitive) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "delimiter:removeDuplicates:preserveOrder:validateData:maxElements:prefix:suffix:caseSensitive"
        if (std::getline(iss, token, ':')) {
            delimiter = token;
        }
        if (std::getline(iss, token, ':')) {
            removeDuplicates = (token == "true");
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
            caseSensitive = (token == "true");
        }
    } catch (const std::exception& e) {
        SGXUtils::logWarning("IntersectionRDDCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

std::vector<std::string> IntersectionRDDCompute::parseOtherRDDData(const std::string& operationData, const std::string& delimiter) {
    std::vector<std::string> result;
    if (!operationData.empty()) {
        std::istringstream iss(operationData);
        std::string item;
        while (std::getline(iss, item, delimiter[0])) {
            if (!item.empty()) {
                result.push_back(item);
            }
        }
    }
    return result;
}

std::vector<std::string> IntersectionRDDCompute::processData(const std::vector<std::string>& data, 
                                                           const std::string& prefix, 
                                                           const std::string& suffix, 
                                                           bool caseSensitive) {
    std::vector<std::string> result;
    result.reserve(data.size());
    
    for (const auto& item : data) {
        std::string processedItem = prefix + item + suffix;
        
        if (!caseSensitive) {
            std::transform(processedItem.begin(), processedItem.end(), processedItem.begin(), ::tolower);
        }
        
        result.push_back(processedItem);
    }
    
    return result;
}

bool IntersectionRDDCompute::validateItem(const std::string& item) {
    return !item.empty() && item.length() < 10000; // 基本验证
}

void IntersectionRDDCompute::validateInputData(const std::vector<std::string>& data, const std::string& dataName) {
    size_t invalidCount = 0;
    for (size_t i = 0; i < data.size(); ++i) {
        if (!validateItem(data[i])) {
            invalidCount++;
            if (invalidCount <= 5) { // 只记录前5个无效项
                SGXUtils::logWarning("IntersectionRDDCompute::validateInputData - Invalid item in " + dataName + 
                                   " at index " + std::to_string(i) + ": " + data[i]);
            }
        }
    }
    
    if (invalidCount > 5) {
        SGXUtils::logWarning("IntersectionRDDCompute::validateInputData - " + std::to_string(invalidCount) + 
                           " invalid items found in " + dataName);
    }
}

} // namespace spark_sgx
