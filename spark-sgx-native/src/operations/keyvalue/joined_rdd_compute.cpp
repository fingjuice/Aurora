#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <unordered_map>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <functional>

namespace spark_sgx {

std::vector<std::string> JoinedRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("JoinedRDDCompute::compute - Starting join operation on " + std::to_string(inputData.size()) + " elements");
    
    try {
        if (inputData.empty()) {
            SGXUtils::logInfo("JoinedRDDCompute::compute - No data to join");
            return std::vector<std::string>();
        }
        
        // 解析操作数据获取join配置
        JoinType joinType = JoinType::INNER_JOIN;
        std::string delimiter = ",";
        std::string keyValueDelimiter = ":";
        std::string resultDelimiter = "|";
        bool validateKeys = true;
        bool caseSensitive = true;
        
        if (!operationData.empty()) {
            parseOperationData(operationData, joinType, delimiter, keyValueDelimiter, resultDelimiter, validateKeys, caseSensitive);
        }
        
        // 解析其他RDD数据
        std::vector<std::string> otherRDDData = parseOtherRDDData(operationData, delimiter);
        
        if (otherRDDData.empty()) {
            SGXUtils::logWarning("JoinedRDDCompute::compute - No other RDD data to join with");
            return std::vector<std::string>();
        }
        
        // 构建映射表
        std::unordered_map<std::string, std::vector<std::string>> leftMap, rightMap;
        
        // 构建左表映射
        size_t leftProcessed = 0;
        for (const auto& item : inputData) {
            try {
                auto keyValue = parseKeyValuePair(item, keyValueDelimiter);
                if (validateKeys && keyValue.first.empty()) {
                    SGXUtils::logWarning("JoinedRDDCompute::compute - Invalid key in left RDD: " + item);
                    continue;
                }
                
                std::string key = caseSensitive ? keyValue.first : toLowerCase(keyValue.first);
                leftMap[key].push_back(keyValue.second);
                leftProcessed++;
                
                if (leftProcessed % 10000 == 0) {
                    SGXUtils::logInfo("JoinedRDDCompute::compute - Processed " + std::to_string(leftProcessed) + " left items");
                }
            } catch (const std::exception& e) {
                SGXUtils::logError("JoinedRDDCompute::compute - Error processing left item: " + std::string(e.what()));
            }
        }
        
        // 构建右表映射
        size_t rightProcessed = 0;
        for (const auto& item : otherRDDData) {
            try {
                auto keyValue = parseKeyValuePair(item, keyValueDelimiter);
                if (validateKeys && keyValue.first.empty()) {
                    SGXUtils::logWarning("JoinedRDDCompute::compute - Invalid key in right RDD: " + item);
                    continue;
                }
                
                std::string key = caseSensitive ? keyValue.first : toLowerCase(keyValue.first);
                rightMap[key].push_back(keyValue.second);
                rightProcessed++;
                
                if (rightProcessed % 10000 == 0) {
                    SGXUtils::logInfo("JoinedRDDCompute::compute - Processed " + std::to_string(rightProcessed) + " right items");
                }
            } catch (const std::exception& e) {
                SGXUtils::logError("JoinedRDDCompute::compute - Error processing right item: " + std::string(e.what()));
            }
        }
        
        // 执行join操作
        std::vector<std::string> result;
        result.reserve(std::min(leftMap.size(), rightMap.size()) * 2); // 预估容量
        
        size_t joinCount = 0;
        
        switch (joinType) {
            case JoinType::INNER_JOIN:
                joinCount = performInnerJoin(leftMap, rightMap, result, resultDelimiter);
                break;
            case JoinType::LEFT_JOIN:
                joinCount = performLeftJoin(leftMap, rightMap, result, resultDelimiter);
                break;
            case JoinType::RIGHT_JOIN:
                joinCount = performRightJoin(leftMap, rightMap, result, resultDelimiter);
                break;
            case JoinType::FULL_OUTER_JOIN:
                joinCount = performFullOuterJoin(leftMap, rightMap, result, resultDelimiter);
                break;
        }
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("JoinedRDDCompute::compute - Completed " + getJoinTypeName(joinType) + 
                         ": " + std::to_string(leftProcessed) + " + " + std::to_string(rightProcessed) + 
                         " -> " + std::to_string(joinCount) + " results in " + 
                         std::to_string(duration.count()) + "ms");
        
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("JoinedRDDCompute::compute - Error during join operation: " + std::string(e.what()));
        throw;
    }
}

void JoinedRDDCompute::parseOperationData(const std::string& operationData, 
                                        JoinType& joinType, 
                                        std::string& delimiter, 
                                        std::string& keyValueDelimiter, 
                                        std::string& resultDelimiter, 
                                        bool& validateKeys, 
                                        bool& caseSensitive) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "joinType:delimiter:keyValueDelimiter:resultDelimiter:validateKeys:caseSensitive"
        if (std::getline(iss, token, ':')) {
            if (token == "INNER_JOIN") joinType = JoinType::INNER_JOIN;
            else if (token == "LEFT_JOIN") joinType = JoinType::LEFT_JOIN;
            else if (token == "RIGHT_JOIN") joinType = JoinType::RIGHT_JOIN;
            else if (token == "FULL_OUTER_JOIN") joinType = JoinType::FULL_OUTER_JOIN;
        }
        if (std::getline(iss, token, ':')) {
            delimiter = token;
        }
        if (std::getline(iss, token, ':')) {
            keyValueDelimiter = token;
        }
        if (std::getline(iss, token, ':')) {
            resultDelimiter = token;
        }
        if (std::getline(iss, token, ':')) {
            validateKeys = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            caseSensitive = (token == "true");
        }
    } catch (const std::exception& e) {
        SGXUtils::logWarning("JoinedRDDCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

std::vector<std::string> JoinedRDDCompute::parseOtherRDDData(const std::string& operationData, const std::string& delimiter) {
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

std::pair<std::string, std::string> JoinedRDDCompute::parseKeyValuePair(const std::string& item, const std::string& delimiter) {
    size_t pos = item.find(delimiter);
    if (pos != std::string::npos) {
        return {item.substr(0, pos), item.substr(pos + delimiter.length())};
    }
    return {item, ""};
}

std::string JoinedRDDCompute::serializeJoinResult(const std::string& key, const std::string& value1, const std::string& value2, const std::string& delimiter) {
    return key + ":" + value1 + delimiter + value2;
}

std::string JoinedRDDCompute::toLowerCase(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

size_t JoinedRDDCompute::performInnerJoin(const std::unordered_map<std::string, std::vector<std::string>>& leftMap,
                                        const std::unordered_map<std::string, std::vector<std::string>>& rightMap,
                                        std::vector<std::string>& result,
                                        const std::string& delimiter) {
    size_t count = 0;
    for (const auto& leftItem : leftMap) {
        auto rightIt = rightMap.find(leftItem.first);
        if (rightIt != rightMap.end()) {
            for (const auto& leftValue : leftItem.second) {
                for (const auto& rightValue : rightIt->second) {
                    result.push_back(serializeJoinResult(leftItem.first, leftValue, rightValue, delimiter));
                    count++;
                }
            }
        }
    }
    return count;
}

size_t JoinedRDDCompute::performLeftJoin(const std::unordered_map<std::string, std::vector<std::string>>& leftMap,
                                       const std::unordered_map<std::string, std::vector<std::string>>& rightMap,
                                       std::vector<std::string>& result,
                                       const std::string& delimiter) {
    size_t count = 0;
    for (const auto& leftItem : leftMap) {
        auto rightIt = rightMap.find(leftItem.first);
        if (rightIt != rightMap.end()) {
            for (const auto& leftValue : leftItem.second) {
                for (const auto& rightValue : rightIt->second) {
                    result.push_back(serializeJoinResult(leftItem.first, leftValue, rightValue, delimiter));
                    count++;
                }
            }
        } else {
            for (const auto& leftValue : leftItem.second) {
                result.push_back(serializeJoinResult(leftItem.first, leftValue, "", delimiter));
                count++;
            }
        }
    }
    return count;
}

size_t JoinedRDDCompute::performRightJoin(const std::unordered_map<std::string, std::vector<std::string>>& leftMap,
                                        const std::unordered_map<std::string, std::vector<std::string>>& rightMap,
                                        std::vector<std::string>& result,
                                        const std::string& delimiter) {
    size_t count = 0;
    for (const auto& rightItem : rightMap) {
        auto leftIt = leftMap.find(rightItem.first);
        if (leftIt != leftMap.end()) {
            for (const auto& rightValue : rightItem.second) {
                for (const auto& leftValue : leftIt->second) {
                    result.push_back(serializeJoinResult(rightItem.first, leftValue, rightValue, delimiter));
                    count++;
                }
            }
        } else {
            for (const auto& rightValue : rightItem.second) {
                result.push_back(serializeJoinResult(rightItem.first, "", rightValue, delimiter));
                count++;
            }
        }
    }
    return count;
}

size_t JoinedRDDCompute::performFullOuterJoin(const std::unordered_map<std::string, std::vector<std::string>>& leftMap,
                                            const std::unordered_map<std::string, std::vector<std::string>>& rightMap,
                                            std::vector<std::string>& result,
                                            const std::string& delimiter) {
    size_t count = 0;
    
    // 处理左表的所有键
    for (const auto& leftItem : leftMap) {
        auto rightIt = rightMap.find(leftItem.first);
        if (rightIt != rightMap.end()) {
            for (const auto& leftValue : leftItem.second) {
                for (const auto& rightValue : rightIt->second) {
                    result.push_back(serializeJoinResult(leftItem.first, leftValue, rightValue, delimiter));
                    count++;
                }
            }
        } else {
            for (const auto& leftValue : leftItem.second) {
                result.push_back(serializeJoinResult(leftItem.first, leftValue, "", delimiter));
                count++;
            }
        }
    }
    
    // 处理右表中左表没有的键
    for (const auto& rightItem : rightMap) {
        auto leftIt = leftMap.find(rightItem.first);
        if (leftIt == leftMap.end()) {
            for (const auto& rightValue : rightItem.second) {
                result.push_back(serializeJoinResult(rightItem.first, "", rightValue, delimiter));
                count++;
            }
        }
    }
    
    return count;
}

std::string JoinedRDDCompute::getJoinTypeName(JoinType joinType) {
    switch (joinType) {
        case JoinType::INNER_JOIN: return "INNER_JOIN";
        case JoinType::LEFT_JOIN: return "LEFT_JOIN";
        case JoinType::RIGHT_JOIN: return "RIGHT_JOIN";
        case JoinType::FULL_OUTER_JOIN: return "FULL_OUTER_JOIN";
        default: return "UNKNOWN";
    }
}

} // namespace spark_sgx

