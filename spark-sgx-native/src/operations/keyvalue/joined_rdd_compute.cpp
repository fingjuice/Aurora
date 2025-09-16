#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <unordered_map>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> JoinedRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("JoinedRDDCompute::compute - Processing " + std::to_string(inputData.size()) + " elements");
    
    // Parse operation data to get the other RDD data
    std::vector<std::string> otherRDDData = parseOtherRDDData(operationData);
    
    std::unordered_map<std::string, std::string> leftMap, rightMap;
    
    // Build maps from both RDDs
    for (const auto& item : inputData) {
        auto keyValue = parseKeyValuePair(item);
        leftMap[keyValue.first] = keyValue.second;
    }
    
    for (const auto& item : otherRDDData) {
        auto keyValue = parseKeyValuePair(item);
        rightMap[keyValue.first] = keyValue.second;
    }
    
    std::vector<std::string> result;
    for (const auto& leftItem : leftMap) {
        if (rightMap.find(leftItem.first) != rightMap.end()) {
            result.push_back(serializeJoinResult(leftItem.first, leftItem.second, rightMap[leftItem.first]));
        }
    }
    
    SGXUtils::logInfo("JoinedRDDCompute::compute - Completed join: " + 
                     std::to_string(inputData.size()) + " + " + std::to_string(otherRDDData.size()) + 
                     " -> " + std::to_string(result.size()) + " elements");
    return result;
}

// Helper function to parse other RDD data
std::vector<std::string> JoinedRDDCompute::parseOtherRDDData(const std::string& operationData) {
    std::vector<std::string> result;
    if (!operationData.empty()) {
        // Split by delimiter (simplified)
        std::istringstream iss(operationData);
        std::string item;
        while (std::getline(iss, item, ',')) {
            if (!item.empty()) {
                result.push_back(item);
            }
        }
    }
    return result;
}

// Helper function to parse key-value pair
std::pair<std::string, std::string> JoinedRDDCompute::parseKeyValuePair(const std::string& item) {
    size_t pos = item.find(':');
    if (pos != std::string::npos) {
        return {item.substr(0, pos), item.substr(pos + 1)};
    }
    return {item, ""};
}

// Helper function to serialize join result
std::string JoinedRDDCompute::serializeJoinResult(const std::string& key, const std::string& value1, const std::string& value2) {
    return key + ":" + value1 + "|" + value2;
}

} // namespace spark_sgx

