#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <chrono>
#include <functional>
#include <regex>

namespace spark_sgx {

std::vector<std::string> FlatMapValuesCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("FlatMapValuesCompute::compute - Starting flatMapValues operation on " + std::to_string(inputData.size()) + " key-value pairs");
    
    try {
        std::vector<std::string> result;
        result.reserve(inputData.size() * 2); // 预估容量
        
        if (inputData.empty()) {
            SGXUtils::logInfo("FlatMapValuesCompute::compute - No data to process");
            return result;
        }
        
        // 解析操作数据获取flatMap函数配置
        FlatMapFunctionType functionType = FlatMapFunctionType::SPLIT_WORDS;
        std::string delimiter = " ";
        std::string prefix = "flatmapped_";
        bool preserveKeys = true;
        bool validateInput = true;
        size_t maxOutputPerInput = 1000; // 防止无限扩展
        
        if (!operationData.empty()) {
            parseOperationData(operationData, functionType, delimiter, prefix, preserveKeys, validateInput, maxOutputPerInput);
        }
        
        // 获取flatMap函数
        auto flatMapFunction = getFlatMapFunction(functionType, delimiter, prefix, maxOutputPerInput);
        
        size_t processed = 0;
        size_t errors = 0;
        size_t totalOutputs = 0;
        
        // 对每个key-value对应用flatMap函数到value
        for (size_t i = 0; i < inputData.size(); ++i) {
            try {
                const std::string& item = inputData[i];
                
                // 验证输入格式
                if (validateInput && !validateKeyValueFormat(item)) {
                    SGXUtils::logWarning("FlatMapValuesCompute::compute - Invalid key-value format: " + item);
                    errors++;
                    continue;
                }
                
                // 解析key-value对
                auto keyValue = parseKeyValuePair(item);
                const std::string& key = keyValue.first;
                const std::string& value = keyValue.second;
                
                // 对value应用flatMap函数
                std::vector<std::string> mappedValues = flatMapFunction(value);
                
                // 构建结果
                for (const auto& mappedValue : mappedValues) {
                    if (preserveKeys && !key.empty()) {
                        result.push_back(key + ":" + mappedValue);
                    } else {
                        result.push_back(mappedValue);
                    }
                    totalOutputs++;
                }
                
                processed++;
                
                // 每处理10000个元素记录一次进度
                if (processed % 10000 == 0) {
                    SGXUtils::logInfo("FlatMapValuesCompute::compute - Processed " + std::to_string(processed) + 
                                     " elements, generated " + std::to_string(totalOutputs) + " outputs");
                }
                
            } catch (const std::exception& e) {
                SGXUtils::logError("FlatMapValuesCompute::compute - Function failed for item " + std::to_string(i) + 
                                  ": " + std::string(e.what()));
                errors++;
                result.push_back(inputData[i]); // 返回原始元素
            }
        }
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("FlatMapValuesCompute::compute - Completed processing " + std::to_string(processed) + 
                         " elements with " + std::to_string(errors) + " errors, generated " + 
                         std::to_string(totalOutputs) + " outputs in " + std::to_string(duration.count()) + "ms");
        
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("FlatMapValuesCompute::compute - Error during flatMapValues operation: " + std::string(e.what()));
        throw;
    }
}

void FlatMapValuesCompute::parseOperationData(const std::string& operationData, 
                                            FlatMapFunctionType& functionType, 
                                            std::string& delimiter, 
                                            std::string& prefix, 
                                            bool& preserveKeys, 
                                            bool& validateInput, 
                                            size_t& maxOutputPerInput) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "functionType:delimiter:prefix:preserveKeys:validateInput:maxOutputPerInput"
        if (std::getline(iss, token, ':')) {
            if (token == "SPLIT_WORDS") functionType = FlatMapFunctionType::SPLIT_WORDS;
            else if (token == "SPLIT_CHARS") functionType = FlatMapFunctionType::SPLIT_CHARS;
            else if (token == "SPLIT_LINES") functionType = FlatMapFunctionType::SPLIT_LINES;
            else if (token == "SPLIT_CUSTOM") functionType = FlatMapFunctionType::SPLIT_CUSTOM;
            else if (token == "REGEX_SPLIT") functionType = FlatMapFunctionType::REGEX_SPLIT;
        }
        if (std::getline(iss, token, ':')) {
            delimiter = token;
        }
        if (std::getline(iss, token, ':')) {
            prefix = token;
        }
        if (std::getline(iss, token, ':')) {
            preserveKeys = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            validateInput = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            maxOutputPerInput = std::stoull(token);
        }
    } catch (const std::exception& e) {
        SGXUtils::logWarning("FlatMapValuesCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

std::function<std::vector<std::string>(const std::string&)> 
FlatMapValuesCompute::getFlatMapFunction(FlatMapFunctionType functionType, 
                                       const std::string& delimiter, 
                                       const std::string& prefix, 
                                       size_t maxOutputPerInput) {
    switch (functionType) {
        case FlatMapFunctionType::SPLIT_WORDS:
            return [prefix, maxOutputPerInput](const std::string& value) -> std::vector<std::string> {
                std::vector<std::string> result;
                std::stringstream ss(value);
                std::string item;
                size_t count = 0;
                while (std::getline(ss, item, ' ') && count < maxOutputPerInput) {
                    if (!item.empty()) {
                        result.push_back(prefix + item);
                        count++;
                    }
                }
                return result;
            };
        case FlatMapFunctionType::SPLIT_CHARS:
            return [prefix, maxOutputPerInput](const std::string& value) -> std::vector<std::string> {
                std::vector<std::string> result;
                size_t count = 0;
                for (char c : value) {
                    if (count >= maxOutputPerInput) break;
                    result.push_back(prefix + std::string(1, c));
                    count++;
                }
                return result;
            };
        case FlatMapFunctionType::SPLIT_LINES:
            return [prefix, maxOutputPerInput](const std::string& value) -> std::vector<std::string> {
                std::vector<std::string> result;
                std::stringstream ss(value);
                std::string item;
                size_t count = 0;
                while (std::getline(ss, item, '\n') && count < maxOutputPerInput) {
                    if (!item.empty()) {
                        result.push_back(prefix + item);
                        count++;
                    }
                }
                return result;
            };
        case FlatMapFunctionType::SPLIT_CUSTOM:
            return [delimiter, prefix, maxOutputPerInput](const std::string& value) -> std::vector<std::string> {
                std::vector<std::string> result;
                std::stringstream ss(value);
                std::string item;
                size_t count = 0;
                while (std::getline(ss, item, delimiter[0]) && count < maxOutputPerInput) {
                    if (!item.empty()) {
                        result.push_back(prefix + item);
                        count++;
                    }
                }
                return result;
            };
        case FlatMapFunctionType::REGEX_SPLIT:
            return [delimiter, prefix, maxOutputPerInput](const std::string& value) -> std::vector<std::string> {
                std::vector<std::string> result;
                try {
                    // 简单的字符串分割，实际实现中会使用真正的正则表达式
                    std::stringstream ss(value);
                    std::string item;
                    size_t count = 0;
                    while (std::getline(ss, item, delimiter[0]) && count < maxOutputPerInput) {
                        if (!item.empty()) {
                            result.push_back(prefix + item);
                            count++;
                        }
                    }
                } catch (...) {
                    result.push_back(prefix + value);
                }
                return result;
            };
        default:
            return [prefix](const std::string& value) -> std::vector<std::string> {
                return {prefix + value};
            };
    }
}

bool FlatMapValuesCompute::validateKeyValueFormat(const std::string& item) {
    return item.find(':') != std::string::npos;
}

std::pair<std::string, std::string> FlatMapValuesCompute::parseKeyValuePair(const std::string& item) {
    size_t delimiterPos = item.find(':');
    if (delimiterPos != std::string::npos) {
        return {item.substr(0, delimiterPos), item.substr(delimiterPos + 1)};
    }
    return {"", item};
}

} // namespace spark_sgx
