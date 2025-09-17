#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <functional>
#include <regex>

namespace spark_sgx {

std::vector<std::string> MapValuesCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("MapValuesCompute::compute - Starting mapValues operation on " + std::to_string(inputData.size()) + " key-value pairs");
    
    try {
        std::vector<std::string> result;
        result.reserve(inputData.size());
        
        if (inputData.empty()) {
            SGXUtils::logInfo("MapValuesCompute::compute - No data to process");
            return result;
        }
        
        // 解析操作数据获取map函数配置
        MapFunctionType functionType = MapFunctionType::STRING_TRANSFORM;
        std::string transformPattern = "";
        bool preserveKeys = true;
        bool validateInput = true;
        
        if (!operationData.empty()) {
            parseOperationData(operationData, functionType, transformPattern, preserveKeys, validateInput);
        }
        
        // 获取map函数
        auto mapFunction = getMapFunction(functionType, transformPattern);
        
        size_t processed = 0;
        size_t errors = 0;
        
        // 对每个key-value对应用map函数到value
        for (size_t i = 0; i < inputData.size(); ++i) {
            try {
                const std::string& item = inputData[i];
                
                // 验证输入格式
                if (validateInput && !validateKeyValueFormat(item)) {
                    SGXUtils::logWarning("MapValuesCompute::compute - Invalid key-value format: " + item);
                    errors++;
                    continue;
                }
                
                // 解析key-value对
                auto keyValue = parseKeyValuePair(item);
                const std::string& key = keyValue.first;
                const std::string& value = keyValue.second;
                
                // 对value应用map函数
                std::string mappedValue = mapFunction(value);
                
                // 构建结果
                if (preserveKeys && !key.empty()) {
                    result.push_back(key + ":" + mappedValue);
                } else {
                    result.push_back(mappedValue);
                }
                
                processed++;
                
                // 每处理10000个元素记录一次进度
                if (processed % 10000 == 0) {
                    SGXUtils::logInfo("MapValuesCompute::compute - Processed " + std::to_string(processed) + " elements");
                }
                
            } catch (const std::exception& e) {
                SGXUtils::logError("MapValuesCompute::compute - Function failed for item " + std::to_string(i) + 
                                  ": " + std::string(e.what()));
                errors++;
                result.push_back(inputData[i]); // 返回原始元素
            }
        }
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("MapValuesCompute::compute - Completed processing " + std::to_string(processed) + 
                         " elements with " + std::to_string(errors) + " errors in " + 
                         std::to_string(duration.count()) + "ms");
        
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("MapValuesCompute::compute - Error during mapValues operation: " + std::string(e.what()));
        throw;
    }
}

void MapValuesCompute::parseOperationData(const std::string& operationData, 
                                        MapFunctionType& functionType, 
                                        std::string& transformPattern, 
                                        bool& preserveKeys, 
                                        bool& validateInput) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "functionType:transformPattern:preserveKeys:validateInput"
        if (std::getline(iss, token, ':')) {
            if (token == "UPPERCASE") functionType = MapFunctionType::UPPERCASE;
            else if (token == "LOWERCASE") functionType = MapFunctionType::LOWERCASE;
            else if (token == "REVERSE") functionType = MapFunctionType::REVERSE;
            else if (token == "LENGTH") functionType = MapFunctionType::LENGTH;
            else if (token == "REGEX_REPLACE") functionType = MapFunctionType::REGEX_REPLACE;
            else if (token == "STRING_TRANSFORM") functionType = MapFunctionType::STRING_TRANSFORM;
        }
        if (std::getline(iss, token, ':')) {
            transformPattern = token;
        }
        if (std::getline(iss, token, ':')) {
            preserveKeys = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            validateInput = (token == "true");
        }
    } catch (const std::exception& e) {
        SGXUtils::logWarning("MapValuesCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

std::function<std::string(const std::string&)> 
MapValuesCompute::getMapFunction(MapFunctionType functionType, const std::string& transformPattern) {
    switch (functionType) {
        case MapFunctionType::UPPERCASE:
            return [](const std::string& value) -> std::string {
                std::string result = value;
                std::transform(result.begin(), result.end(), result.begin(), ::toupper);
                return result;
            };
        case MapFunctionType::LOWERCASE:
            return [](const std::string& value) -> std::string {
                std::string result = value;
                std::transform(result.begin(), result.end(), result.begin(), ::tolower);
                return result;
            };
        case MapFunctionType::REVERSE:
            return [](const std::string& value) -> std::string {
                std::string result = value;
                std::reverse(result.begin(), result.end());
                return result;
            };
        case MapFunctionType::LENGTH:
            return [](const std::string& value) -> std::string {
                return std::to_string(value.length());
            };
        case MapFunctionType::REGEX_REPLACE:
            return [transformPattern](const std::string& value) -> std::string {
                if (transformPattern.empty()) return value;
                try {
                    // 简单的字符串替换，实际实现中会使用真正的正则表达式
                    std::string result = value;
                    size_t pos = 0;
                    while ((pos = result.find(transformPattern, pos)) != std::string::npos) {
                        result.replace(pos, transformPattern.length(), "REPLACED");
                        pos += 8; // "REPLACED"的长度
                    }
                    return result;
                } catch (...) {
                    return value;
                }
            };
        case MapFunctionType::STRING_TRANSFORM:
        default:
            return [](const std::string& value) -> std::string {
                return "mapped_" + value;
            };
    }
}

bool MapValuesCompute::validateKeyValueFormat(const std::string& item) {
    // 检查是否包含冒号分隔符
    return item.find(':') != std::string::npos;
}

std::pair<std::string, std::string> MapValuesCompute::parseKeyValuePair(const std::string& item) {
    size_t delimiterPos = item.find(':');
    if (delimiterPos != std::string::npos) {
        return {item.substr(0, delimiterPos), item.substr(delimiterPos + 1)};
    }
    return {"", item}; // 如果没有分隔符，将整个字符串作为值
}

} // namespace spark_sgx
