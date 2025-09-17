#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <algorithm>
#include <chrono>
#include <functional>
#include <regex>

namespace spark_sgx {

std::vector<std::string> FilteredRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("FilteredRDDCompute::compute - Starting filter operation on " + std::to_string(inputData.size()) + " elements");
    
    try {
        std::vector<std::string> result;
        result.reserve(inputData.size()); // 预分配最大可能容量
        
        if (inputData.empty()) {
            SGXUtils::logInfo("FilteredRDDCompute::compute - No data to filter");
            return result;
        }
        
        // 解析操作数据获取filter谓词配置
        FilterType filterType = FilterType::NON_EMPTY;
        std::string pattern = "";
        bool caseSensitive = true;
        bool invertMatch = false;
        size_t minLength = 0;
        size_t maxLength = SIZE_MAX;
        
        if (!operationData.empty()) {
            parseOperationData(operationData, filterType, pattern, caseSensitive, invertMatch, minLength, maxLength);
        }
        
        // 获取filter谓词
        auto filterPredicate = getFilterPredicate(filterType, pattern, caseSensitive, minLength, maxLength);
        
        size_t processed = 0;
        size_t passed = 0;
        size_t errors = 0;
        
        // 对每个元素应用filter谓词
        for (size_t i = 0; i < inputData.size(); ++i) {
            try {
                const std::string& item = inputData[i];
                
                bool shouldInclude = filterPredicate(item);
                if (invertMatch) {
                    shouldInclude = !shouldInclude;
                }
                
                if (shouldInclude) {
                    result.push_back(item);
                    passed++;
                }
                
                processed++;
                
                // 每处理10000个元素记录一次进度
                if (processed % 10000 == 0) {
                    SGXUtils::logInfo("FilteredRDDCompute::compute - Processed " + std::to_string(processed) + 
                                     " elements, passed " + std::to_string(passed));
                }
                
            } catch (const std::exception& e) {
                SGXUtils::logError("FilteredRDDCompute::compute - Predicate failed for item " + std::to_string(i) + 
                                  ": " + std::string(e.what()));
                errors++;
                // 继续处理其他元素，不中断整个操作
            }
        }
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("FilteredRDDCompute::compute - Completed filtering " + std::to_string(processed) + 
                         " elements to " + std::to_string(passed) + " elements with " + 
                         std::to_string(errors) + " errors in " + std::to_string(duration.count()) + "ms");
        
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("FilteredRDDCompute::compute - Error during filter operation: " + std::string(e.what()));
        throw;
    }
}

void FilteredRDDCompute::parseOperationData(const std::string& operationData, 
                                          FilterType& filterType, 
                                          std::string& pattern, 
                                          bool& caseSensitive, 
                                          bool& invertMatch, 
                                          size_t& minLength, 
                                          size_t& maxLength) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "filterType:pattern:caseSensitive:invertMatch:minLength:maxLength"
        if (std::getline(iss, token, ':')) {
            if (token == "NON_EMPTY") filterType = FilterType::NON_EMPTY;
            else if (token == "CONTAINS") filterType = FilterType::CONTAINS;
            else if (token == "STARTS_WITH") filterType = FilterType::STARTS_WITH;
            else if (token == "ENDS_WITH") filterType = FilterType::ENDS_WITH;
            else if (token == "REGEX_MATCH") filterType = FilterType::REGEX_MATCH;
            else if (token == "LENGTH_RANGE") filterType = FilterType::LENGTH_RANGE;
            else if (token == "NUMERIC") filterType = FilterType::NUMERIC;
        }
        if (std::getline(iss, token, ':')) {
            pattern = token;
        }
        if (std::getline(iss, token, ':')) {
            caseSensitive = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            invertMatch = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            minLength = std::stoull(token);
        }
        if (std::getline(iss, token, ':')) {
            maxLength = std::stoull(token);
        }
    } catch (const std::exception& e) {
        SGXUtils::logWarning("FilteredRDDCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

std::function<bool(const std::string&)> 
FilteredRDDCompute::getFilterPredicate(FilterType filterType, 
                                     const std::string& pattern, 
                                     bool caseSensitive, 
                                     size_t minLength, 
                                     size_t maxLength) {
    switch (filterType) {
        case FilterType::NON_EMPTY:
            return [](const std::string& item) -> bool {
                return !item.empty();
            };
        case FilterType::CONTAINS:
            return [pattern, caseSensitive](const std::string& item) -> bool {
                if (pattern.empty()) return true;
                if (caseSensitive) {
                    return item.find(pattern) != std::string::npos;
                } else {
                    std::string lowerItem = item;
                    std::string lowerPattern = pattern;
                    std::transform(lowerItem.begin(), lowerItem.end(), lowerItem.begin(), ::tolower);
                    std::transform(lowerPattern.begin(), lowerPattern.end(), lowerPattern.begin(), ::tolower);
                    return lowerItem.find(lowerPattern) != std::string::npos;
                }
            };
        case FilterType::STARTS_WITH:
            return [pattern, caseSensitive](const std::string& item) -> bool {
                if (pattern.empty()) return true;
                if (caseSensitive) {
                    return item.substr(0, pattern.length()) == pattern;
                } else {
                    std::string lowerItem = item.substr(0, pattern.length());
                    std::string lowerPattern = pattern;
                    std::transform(lowerItem.begin(), lowerItem.end(), lowerItem.begin(), ::tolower);
                    std::transform(lowerPattern.begin(), lowerPattern.end(), lowerPattern.begin(), ::tolower);
                    return lowerItem == lowerPattern;
                }
            };
        case FilterType::ENDS_WITH:
            return [pattern, caseSensitive](const std::string& item) -> bool {
                if (pattern.empty()) return true;
                if (item.length() < pattern.length()) return false;
                if (caseSensitive) {
                    return item.substr(item.length() - pattern.length()) == pattern;
                } else {
                    std::string lowerItem = item.substr(item.length() - pattern.length());
                    std::string lowerPattern = pattern;
                    std::transform(lowerItem.begin(), lowerItem.end(), lowerItem.begin(), ::tolower);
                    std::transform(lowerPattern.begin(), lowerPattern.end(), lowerPattern.begin(), ::tolower);
                    return lowerItem == lowerPattern;
                }
            };
        case FilterType::REGEX_MATCH:
            return [pattern](const std::string& item) -> bool {
                if (pattern.empty()) return true;
                try {
                    std::regex regexPattern(pattern);
                    return std::regex_search(item, regexPattern);
                } catch (...) {
                    return false;
                }
            };
        case FilterType::LENGTH_RANGE:
            return [minLength, maxLength](const std::string& item) -> bool {
                size_t length = item.length();
                return length >= minLength && length <= maxLength;
            };
        case FilterType::NUMERIC:
            return [](const std::string& item) -> bool {
                if (item.empty()) return false;
                try {
                    std::stod(item);
                    return true;
                } catch (...) {
                    return false;
                }
            };
        default:
            return [](const std::string& item) -> bool {
                return !item.empty();
            };
    }
}

} // namespace spark_sgx

