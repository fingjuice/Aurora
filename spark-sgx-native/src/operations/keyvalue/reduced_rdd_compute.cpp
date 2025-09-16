#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <unordered_map>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> ReducedRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("ReducedRDDCompute::compute - Processing " + std::to_string(inputData.size()) + " elements");
    
    std::unordered_map<std::string, std::string> reduced;
    
    // 解析操作数据获取reduce函数
    // 这里简化实现，实际应该反序列化真正的函数
    auto reduceFunction = [](const std::string& value1, const std::string& value2) -> std::string {
        return value1 + "+" + value2; // 简单的字符串连接
    };
    
    for (const auto& item : inputData) {
        // Parse key-value pair
        auto keyValue = parseKeyValuePair(item);
        
        if (reduced.find(keyValue.first) == reduced.end()) {
            reduced[keyValue.first] = keyValue.second;
        } else {
            // Apply reduce function
            reduced[keyValue.first] = reduceFunction(reduced[keyValue.first], keyValue.second);
        }
    }
    
    std::vector<std::string> result;
    for (const auto& item : reduced) {
        result.push_back(serializeKeyValuePair(item.first, item.second));
    }
    
    SGXUtils::logInfo("ReducedRDDCompute::compute - Completed reduce: " + 
                     std::to_string(inputData.size()) + " -> " + std::to_string(result.size()) + " elements");
    return result;
}

// Helper function to parse key-value pair
std::pair<std::string, std::string> ReducedRDDCompute::parseKeyValuePair(const std::string& item) {
    size_t pos = item.find(':');
    if (pos != std::string::npos) {
        return {item.substr(0, pos), item.substr(pos + 1)};
    }
    return {item, ""};
}

// Helper function to serialize key-value pair
std::string ReducedRDDCompute::serializeKeyValuePair(const std::string& key, const std::string& value) {
    return key + ":" + value;
}

} // namespace spark_sgx

