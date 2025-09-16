#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <unordered_map>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> GroupedRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("GroupedRDDCompute::compute - Processing " + std::to_string(inputData.size()) + " elements");
    
    std::unordered_map<std::string, std::vector<std::string>> groups;
    
    for (const auto& item : inputData) {
        // Parse key-value pair (simplified)
        auto keyValue = parseKeyValuePair(item);
        groups[keyValue.first].push_back(keyValue.second);
    }
    
    std::vector<std::string> result;
    for (const auto& group : groups) {
        std::string groupData = serializeGroup(group.first, group.second);
        result.push_back(groupData);
    }
    
    SGXUtils::logInfo("GroupedRDDCompute::compute - Completed grouping: " + 
                     std::to_string(inputData.size()) + " -> " + std::to_string(result.size()) + " groups");
    return result;
}

// Helper function to parse key-value pair
std::pair<std::string, std::string> GroupedRDDCompute::parseKeyValuePair(const std::string& item) {
    size_t pos = item.find(':');
    if (pos != std::string::npos) {
        return {item.substr(0, pos), item.substr(pos + 1)};
    }
    return {item, ""};
}

// Helper function to serialize group
std::string GroupedRDDCompute::serializeGroup(const std::string& key, const std::vector<std::string>& values) {
    std::string result = key + ":";
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) result += ",";
        result += values[i];
    }
    return result;
}

} // namespace spark_sgx

