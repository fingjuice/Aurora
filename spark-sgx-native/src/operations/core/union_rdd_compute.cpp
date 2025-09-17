#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <unordered_set>

namespace spark_sgx {

std::vector<std::string> UnionRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("UnionRDDCompute::compute - Starting union operation on " + std::to_string(inputData.size()) + " elements");
    
    try {
        if (inputData.empty()) {
            SGXUtils::logInfo("UnionRDDCompute::compute - No input data to union");
            return std::vector<std::string>();
        }
        
        // 解析操作数据获取union配置
        std::string delimiter = ",";
        bool removeDuplicates = false;
        bool preserveOrder = true;
        bool validateData = true;
        size_t maxElements = 1000000; // 最大元素数限制
        std::string prefix = "";
        std::string suffix = "";
        
        if (!operationData.empty()) {
            parseOperationData(operationData, delimiter, removeDuplicates, preserveOrder, validateData, maxElements, prefix, suffix);
        }
        
        // 解析其他RDD数据
        std::vector<std::string> otherRDDData = parseOtherRDDData(operationData, delimiter);
        
        if (otherRDDData.empty()) {
            SGXUtils::logWarning("UnionRDDCompute::compute - No other RDD data to union with");
            return inputData;
        }
        
        // 数据验证
        if (validateData) {
            validateInputData(inputData, "first RDD");
            validateInputData(otherRDDData, "second RDD");
        }
        
        // 检查元素数量限制
        size_t totalElements = inputData.size() + otherRDDData.size();
        if (totalElements > maxElements) {
            SGXUtils::logWarning("UnionRDDCompute::compute - Total elements (" + std::to_string(totalElements) + 
                               ") exceeds limit (" + std::to_string(maxElements) + "), truncating");
        }
        
        std::vector<std::string> result;
        result.reserve(std::min(totalElements, maxElements));
        
        size_t processedCount = 0;
        size_t duplicateCount = 0;
        size_t skippedCount = 0;
        
        // 如果不需要去重，直接合并
        if (!removeDuplicates) {
            // 添加第一个RDD的所有元素
            for (const auto& item : inputData) {
                if (processedCount >= maxElements) break;
                
                std::string processedItem = processItem(item, prefix, suffix);
                if (validateData && !validateItem(processedItem)) {
                    SGXUtils::logWarning("UnionRDDCompute::compute - Invalid item in first RDD: " + item);
                    skippedCount++;
                    continue;
                }
                
                result.push_back(processedItem);
                processedCount++;
            }
            
            // 添加第二个RDD的所有元素
            for (const auto& item : otherRDDData) {
                if (processedCount >= maxElements) break;
                
                std::string processedItem = processItem(item, prefix, suffix);
                if (validateData && !validateItem(processedItem)) {
                    SGXUtils::logWarning("UnionRDDCompute::compute - Invalid item in second RDD: " + item);
                    skippedCount++;
                    continue;
                }
                
                result.push_back(processedItem);
                processedCount++;
            }
        } else {
            // 需要去重，使用set来跟踪已处理的元素
            std::unordered_set<std::string> seen;
            
            // 处理第一个RDD
            for (const auto& item : inputData) {
                if (processedCount >= maxElements) break;
                
                std::string processedItem = processItem(item, prefix, suffix);
                if (validateData && !validateItem(processedItem)) {
                    SGXUtils::logWarning("UnionRDDCompute::compute - Invalid item in first RDD: " + item);
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
            }
            
            // 处理第二个RDD
            for (const auto& item : otherRDDData) {
                if (processedCount >= maxElements) break;
                
                std::string processedItem = processItem(item, prefix, suffix);
                if (validateData && !validateItem(processedItem)) {
                    SGXUtils::logWarning("UnionRDDCompute::compute - Invalid item in second RDD: " + item);
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
            }
        }
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("UnionRDDCompute::compute - Completed union: " + 
                         std::to_string(inputData.size()) + " + " + std::to_string(otherRDDData.size()) + 
                         " -> " + std::to_string(result.size()) + " elements (processed: " + 
                         std::to_string(processedCount) + ", duplicates: " + std::to_string(duplicateCount) + 
                         ", skipped: " + std::to_string(skippedCount) + ") in " + 
                         std::to_string(duration.count()) + "ms");
        
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("UnionRDDCompute::compute - Error during union operation: " + std::string(e.what()));
        throw;
    }
}

void UnionRDDCompute::parseOperationData(const std::string& operationData, 
                                       std::string& delimiter, 
                                       bool& removeDuplicates, 
                                       bool& preserveOrder, 
                                       bool& validateData, 
                                       size_t& maxElements, 
                                       std::string& prefix, 
                                       std::string& suffix) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "delimiter:removeDuplicates:preserveOrder:validateData:maxElements:prefix:suffix"
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
    } catch (const std::exception& e) {
        SGXUtils::logWarning("UnionRDDCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

std::vector<std::string> UnionRDDCompute::parseOtherRDDData(const std::string& operationData, const std::string& delimiter) {
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

std::string UnionRDDCompute::processItem(const std::string& item, const std::string& prefix, const std::string& suffix) {
    return prefix + item + suffix;
}

bool UnionRDDCompute::validateItem(const std::string& item) {
    return !item.empty() && item.length() < 10000; // 基本验证
}

void UnionRDDCompute::validateInputData(const std::vector<std::string>& data, const std::string& dataName) {
    size_t invalidCount = 0;
    for (size_t i = 0; i < data.size(); ++i) {
        if (!validateItem(data[i])) {
            invalidCount++;
            if (invalidCount <= 5) { // 只记录前5个无效项
                SGXUtils::logWarning("UnionRDDCompute::validateInputData - Invalid item in " + dataName + 
                                   " at index " + std::to_string(i) + ": " + data[i]);
            }
        }
    }
    
    if (invalidCount > 5) {
        SGXUtils::logWarning("UnionRDDCompute::validateInputData - " + std::to_string(invalidCount) + 
                           " invalid items found in " + dataName);
    }
}

} // namespace spark_sgx

