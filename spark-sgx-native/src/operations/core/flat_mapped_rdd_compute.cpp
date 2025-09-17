#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <functional>
#include <regex>

namespace spark_sgx {

std::vector<std::string> FlatMappedRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("FlatMappedRDDCompute::compute - Starting flatMap operation on " + std::to_string(inputData.size()) + " elements");
    
    try {
        if (inputData.empty()) {
            SGXUtils::logInfo("FlatMappedRDDCompute::compute - No data to flatMap");
            return std::vector<std::string>();
        }
        
        // 解析操作数据获取flatMap函数配置
        FlatMapFunctionType functionType = FlatMapFunctionType::SPLIT_WORDS;
        std::string customFunction = "";
        bool preserveOrder = true;
        bool validateInput = true;
        bool caseSensitive = true;
        std::string delimiter = " ";
        std::string prefix = "";
        std::string suffix = "";
        size_t maxOutputElements = 1000000; // 最大输出元素数限制
        
        if (!operationData.empty()) {
            parseOperationData(operationData, functionType, customFunction, preserveOrder, validateInput, caseSensitive, delimiter, prefix, suffix, maxOutputElements);
        }
        
        // 获取flatMap函数
        auto flatMapFunction = getFlatMapFunction(functionType, customFunction, caseSensitive, delimiter, prefix, suffix);
        
        std::vector<std::string> result;
        result.reserve(inputData.size() * 2); // 预估容量
        
        size_t processedCount = 0;
        size_t errorCount = 0;
        size_t skippedCount = 0;
        size_t totalOutputCount = 0;
        
        // 对每个元素应用flatMap函数
        for (size_t i = 0; i < inputData.size(); ++i) {
            try {
                const std::string& item = inputData[i];
                
                // 输入验证
                if (validateInput && !validateInputItem(item)) {
                    SGXUtils::logWarning("FlatMappedRDDCompute::compute - Invalid input item at index " + std::to_string(i) + ": " + item);
                    skippedCount++;
                    continue;
                }
                
                auto flatMappedItems = flatMapFunction(item);
                
                // 检查输出元素数量限制
                if (totalOutputCount + flatMappedItems.size() > maxOutputElements) {
                    SGXUtils::logWarning("FlatMappedRDDCompute::compute - Output elements limit reached, truncating");
                    break;
                }
                
                // 输出验证
                if (validateInput) {
                    for (const auto& outputItem : flatMappedItems) {
                        if (!validateOutputItem(outputItem)) {
                            SGXUtils::logWarning("FlatMappedRDDCompute::compute - Invalid output item: " + outputItem);
                            errorCount++;
                            continue;
                        }
                    }
                }
                
                result.insert(result.end(), flatMappedItems.begin(), flatMappedItems.end());
                totalOutputCount += flatMappedItems.size();
                processedCount++;
                
                // 每处理1000个元素记录一次进度
                if (processedCount % 1000 == 0) {
                    SGXUtils::logInfo("FlatMappedRDDCompute::compute - Processed " + std::to_string(processedCount) + 
                                    " elements, output " + std::to_string(totalOutputCount) + " elements");
                }
                
            } catch (const std::exception& e) {
                SGXUtils::logError("FlatMappedRDDCompute::compute - Function failed for item " + std::to_string(i) + 
                                  ": " + std::string(e.what()));
                errorCount++;
                
                // 根据配置决定是否跳过错误项
                if (preserveOrder) {
                    // 添加空结果以保持顺序
                    result.push_back("");
                }
            }
        }
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("FlatMappedRDDCompute::compute - Completed flatMap operation: " + 
                         std::to_string(inputData.size()) + " -> " + std::to_string(result.size()) + 
                         " elements (processed: " + std::to_string(processedCount) + 
                         ", errors: " + std::to_string(errorCount) + 
                         ", skipped: " + std::to_string(skippedCount) + 
                         ") in " + std::to_string(duration.count()) + "ms");
        
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("FlatMappedRDDCompute::compute - Error during flatMap operation: " + std::string(e.what()));
        throw;
    }
}

void FlatMappedRDDCompute::parseOperationData(const std::string& operationData, 
                                            FlatMapFunctionType& functionType, 
                                            std::string& customFunction, 
                                            bool& preserveOrder, 
                                            bool& validateInput, 
                                            bool& caseSensitive, 
                                            std::string& delimiter, 
                                            std::string& prefix, 
                                            std::string& suffix, 
                                            size_t& maxOutputElements) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "functionType:customFunction:preserveOrder:validateInput:caseSensitive:delimiter:prefix:suffix:maxOutputElements"
        if (std::getline(iss, token, ':')) {
            if (token == "SPLIT_WORDS") functionType = FlatMapFunctionType::SPLIT_WORDS;
            else if (token == "SPLIT_CHARS") functionType = FlatMapFunctionType::SPLIT_CHARS;
            else if (token == "SPLIT_LINES") functionType = FlatMapFunctionType::SPLIT_LINES;
            else if (token == "SPLIT_CUSTOM") functionType = FlatMapFunctionType::SPLIT_CUSTOM;
            else if (token == "REGEX_SPLIT") functionType = FlatMapFunctionType::REGEX_SPLIT;
            else if (token == "CUSTOM") functionType = FlatMapFunctionType::CUSTOM;
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
        if (std::getline(iss, token, ':')) {
            maxOutputElements = std::stoull(token);
        }
    } catch (const std::exception& e) {
        SGXUtils::logWarning("FlatMappedRDDCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

std::function<std::vector<std::string>(const std::string&)> FlatMappedRDDCompute::getFlatMapFunction(
    FlatMapFunctionType functionType, 
    const std::string& customFunction, 
    bool caseSensitive, 
    const std::string& delimiter, 
    const std::string& prefix, 
    const std::string& suffix) {
    
    switch (functionType) {
        case FlatMapFunctionType::SPLIT_WORDS:
            return [delimiter, prefix, suffix](const std::string& item) -> std::vector<std::string> {
                std::vector<std::string> result;
                std::istringstream iss(item);
                std::string word;
                while (std::getline(iss, word, delimiter[0])) {
                    if (!word.empty()) {
                        result.push_back(prefix + word + suffix);
                    }
                }
                return result;
            };
            
        case FlatMapFunctionType::SPLIT_CHARS:
            return [prefix, suffix](const std::string& item) -> std::vector<std::string> {
                std::vector<std::string> result;
                for (char c : item) {
                    result.push_back(prefix + std::string(1, c) + suffix);
                }
                return result;
            };
            
        case FlatMapFunctionType::SPLIT_LINES:
            return [prefix, suffix](const std::string& item) -> std::vector<std::string> {
                std::vector<std::string> result;
                std::istringstream iss(item);
                std::string line;
                while (std::getline(iss, line)) {
                    if (!line.empty()) {
                        result.push_back(prefix + line + suffix);
                    }
                }
                return result;
            };
            
        case FlatMapFunctionType::SPLIT_CUSTOM:
            return [customFunction, delimiter, prefix, suffix](const std::string& item) -> std::vector<std::string> {
                std::vector<std::string> result;
                std::istringstream iss(item);
                std::string token;
                while (std::getline(iss, token, delimiter[0])) {
                    if (!token.empty()) {
                        result.push_back(prefix + token + suffix);
                    }
                }
                return result;
            };
            
        case FlatMapFunctionType::REGEX_SPLIT:
            return [customFunction, prefix, suffix](const std::string& item) -> std::vector<std::string> {
                std::vector<std::string> result;
                try {
                    std::regex regexPattern(customFunction);
                    std::sregex_token_iterator iter(item.begin(), item.end(), regexPattern, -1);
                    std::sregex_token_iterator end;
                    
                    for (; iter != end; ++iter) {
                        std::string token = *iter;
                        if (!token.empty()) {
                            result.push_back(prefix + token + suffix);
                        }
                    }
                } catch (const std::exception& e) {
                    SGXUtils::logError("FlatMappedRDDCompute::getFlatMapFunction - Regex error: " + std::string(e.what()));
                    result.push_back(prefix + item + suffix);
                }
                return result;
            };
            
        case FlatMapFunctionType::CUSTOM:
            return [customFunction, delimiter, prefix, suffix](const std::string& item) -> std::vector<std::string> {
                // 简单的自定义函数处理
                if (customFunction == "double") {
                    return {prefix + item + suffix, prefix + item + suffix};
                } else if (customFunction == "reverse") {
                    std::string reversed = item;
                    std::reverse(reversed.begin(), reversed.end());
                    return {prefix + reversed + suffix};
                } else {
                    return {prefix + customFunction + "_" + item + suffix};
                }
            };
            
        default:
            return [](const std::string& item) -> std::vector<std::string> {
                return {item};
            };
    }
}

bool FlatMappedRDDCompute::validateInputItem(const std::string& item) {
    return !item.empty() && item.length() < 10000; // 基本验证
}

bool FlatMappedRDDCompute::validateOutputItem(const std::string& item) {
    return !item.empty() && item.length() < 10000; // 基本验证
}

} // namespace spark_sgx

