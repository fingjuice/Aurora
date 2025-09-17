#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <functional>
#include <regex>

namespace spark_sgx {

std::vector<std::string> MapPartitionsRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("MapPartitionsRDDCompute::compute - Starting map operation on " + std::to_string(inputData.size()) + " elements");
    
    try {
        if (inputData.empty()) {
            SGXUtils::logInfo("MapPartitionsRDDCompute::compute - No data to map");
            return std::vector<std::string>();
        }
        
        // 解析操作数据获取map函数配置
        MapFunctionType functionType = MapFunctionType::IDENTITY;
        std::string customFunction = "";
        bool preserveOrder = true;
        bool validateInput = true;
        bool caseSensitive = true;
        std::string delimiter = ",";
        std::string prefix = "";
        std::string suffix = "";
        
        if (!operationData.empty()) {
            parseOperationData(operationData, functionType, customFunction, preserveOrder, validateInput, caseSensitive, delimiter, prefix, suffix);
        }
        
        // 获取map函数
        auto mapFunction = getMapFunction(functionType, customFunction, caseSensitive, delimiter, prefix, suffix);
        
        std::vector<std::string> result;
        result.reserve(inputData.size());
        
        size_t processedCount = 0;
        size_t errorCount = 0;
        size_t skippedCount = 0;
        
        // 对每个元素应用map函数
        for (size_t i = 0; i < inputData.size(); ++i) {
            try {
                const std::string& item = inputData[i];
                
                // 输入验证
                if (validateInput && !validateInputItem(item)) {
                    SGXUtils::logWarning("MapPartitionsRDDCompute::compute - Invalid input item at index " + std::to_string(i) + ": " + item);
                    skippedCount++;
                    continue;
                }
                
                std::string mappedItem = mapFunction(item);
                
                // 输出验证
                if (validateInput && !validateOutputItem(mappedItem)) {
                    SGXUtils::logWarning("MapPartitionsRDDCompute::compute - Invalid output item at index " + std::to_string(i) + ": " + mappedItem);
                    errorCount++;
                    continue;
                }
                
                result.push_back(mappedItem);
                processedCount++;
                
                // 每处理1000个元素记录一次进度
                if (processedCount % 1000 == 0) {
                    SGXUtils::logInfo("MapPartitionsRDDCompute::compute - Processed " + std::to_string(processedCount) + " elements");
                }
                
            } catch (const std::exception& e) {
                SGXUtils::logError("MapPartitionsRDDCompute::compute - Function failed for item " + std::to_string(i) + 
                                  ": " + std::string(e.what()));
                errorCount++;
                
                // 根据配置决定是否跳过错误项
                if (preserveOrder) {
                    result.push_back(inputData[i]); // 返回原始元素
                }
            }
        }
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("MapPartitionsRDDCompute::compute - Completed map operation: " + 
                         std::to_string(inputData.size()) + " -> " + std::to_string(result.size()) + 
                         " elements (processed: " + std::to_string(processedCount) + 
                         ", errors: " + std::to_string(errorCount) + 
                         ", skipped: " + std::to_string(skippedCount) + 
                         ") in " + std::to_string(duration.count()) + "ms");
        
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("MapPartitionsRDDCompute::compute - Error during map operation: " + std::string(e.what()));
        throw;
    }
}

void MapPartitionsRDDCompute::parseOperationData(const std::string& operationData, 
                                               MapFunctionType& functionType, 
                                               std::string& customFunction, 
                                               bool& preserveOrder, 
                                               bool& validateInput, 
                                               bool& caseSensitive, 
                                               std::string& delimiter, 
                                               std::string& prefix, 
                                               std::string& suffix) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "functionType:customFunction:preserveOrder:validateInput:caseSensitive:delimiter:prefix:suffix"
        if (std::getline(iss, token, ':')) {
            if (token == "IDENTITY") functionType = MapFunctionType::IDENTITY;
            else if (token == "UPPERCASE") functionType = MapFunctionType::UPPERCASE;
            else if (token == "LOWERCASE") functionType = MapFunctionType::LOWERCASE;
            else if (token == "REVERSE") functionType = MapFunctionType::REVERSE;
            else if (token == "LENGTH") functionType = MapFunctionType::LENGTH;
            else if (token == "REGEX_REPLACE") functionType = MapFunctionType::REGEX_REPLACE;
            else if (token == "CUSTOM") functionType = MapFunctionType::CUSTOM;
        }
        if (std::getline(iss, token, ':')) {
            customFunction = token;
        }
        if (std::getline(iss, token, ':')) {
            preserveOrder = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            validateInput = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            caseSensitive = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            delimiter = token;
        }
        if (std::getline(iss, token, ':')) {
            prefix = token;
        }
        if (std::getline(iss, token, ':')) {
            suffix = token;
        }
    } catch (const std::exception& e) {
        SGXUtils::logWarning("MapPartitionsRDDCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

std::function<std::string(const std::string&)> MapPartitionsRDDCompute::getMapFunction(
    MapFunctionType functionType, 
    const std::string& customFunction, 
    bool caseSensitive, 
    const std::string& delimiter, 
    const std::string& prefix, 
    const std::string& suffix) {
    
    switch (functionType) {
        case MapFunctionType::IDENTITY:
            return [](const std::string& item) -> std::string {
                return item;
            };
            
        case MapFunctionType::UPPERCASE:
            return [](const std::string& item) -> std::string {
                std::string result = item;
                std::transform(result.begin(), result.end(), result.begin(), ::toupper);
                return result;
            };
            
        case MapFunctionType::LOWERCASE:
            return [](const std::string& item) -> std::string {
                std::string result = item;
                std::transform(result.begin(), result.end(), result.begin(), ::tolower);
                return result;
            };
            
        case MapFunctionType::REVERSE:
            return [](const std::string& item) -> std::string {
                std::string result = item;
                std::reverse(result.begin(), result.end());
                return result;
            };
            
        case MapFunctionType::LENGTH:
            return [](const std::string& item) -> std::string {
                return std::to_string(item.length());
            };
            
        case MapFunctionType::REGEX_REPLACE:
            return [customFunction](const std::string& item) -> std::string {
                try {
                    // 解析自定义函数: "pattern:replacement"
                    size_t pos = customFunction.find(':');
                    if (pos != std::string::npos) {
                        std::string pattern = customFunction.substr(0, pos);
                        std::string replacement = customFunction.substr(pos + 1);
                        std::regex regexPattern(pattern);
                        return std::regex_replace(item, regexPattern, replacement);
                    }
                    return item;
                } catch (const std::exception& e) {
                    SGXUtils::logError("MapPartitionsRDDCompute::getMapFunction - Regex error: " + std::string(e.what()));
                    return item;
                }
            };
            
        case MapFunctionType::CUSTOM:
            return [customFunction, delimiter, prefix, suffix](const std::string& item) -> std::string {
                // 简单的自定义函数处理
                if (customFunction == "split") {
                    std::string result = item;
                    std::replace(result.begin(), result.end(), delimiter[0], ' ');
                    return result;
                } else if (customFunction == "join") {
                    return prefix + item + suffix;
                } else {
                    return customFunction + "_" + item;
                }
            };
            
        default:
            return [](const std::string& item) -> std::string {
                return item;
            };
    }
}

bool MapPartitionsRDDCompute::validateInputItem(const std::string& item) {
    return !item.empty() && item.length() < 10000; // 基本验证
}

bool MapPartitionsRDDCompute::validateOutputItem(const std::string& item) {
    return !item.empty() && item.length() < 10000; // 基本验证
}

} // namespace spark_sgx

