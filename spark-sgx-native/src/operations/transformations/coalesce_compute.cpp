#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <random>
#include <algorithm>

namespace spark_sgx {

std::vector<std::string> CoalesceCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("CoalesceCompute::compute - Coalescing " + std::to_string(inputData.size()) + " elements");
    
    // 解析操作数据获取coalesce参数
    int numPartitions = 1; // 默认分区数
    bool shuffle = false; // 默认不shuffle
    
    if (!operationData.empty()) {
        try {
            // 解析格式: "numPartitions:shuffle"
            std::stringstream ss(operationData);
            std::string token;
            if (std::getline(ss, token, ':')) {
                numPartitions = std::stoi(token);
            }
            if (std::getline(ss, token, ':')) {
                shuffle = (token == "true");
            }
        } catch (const std::exception& e) {
            SGXUtils::logWarning("CoalesceCompute::compute - Invalid operation data, using defaults");
        }
    }
    
    std::vector<std::string> result = inputData;
    
    if (result.empty()) {
        SGXUtils::logInfo("CoalesceCompute::compute - No data to coalesce");
        return result;
    }
    
    // Coalesce操作主要是重新分区，这里简化实现
    // 在实际实现中，这里会涉及复杂的分区逻辑
    if (shuffle) {
        // 如果需要shuffle，随机打乱数据
        std::random_device rd;
        std::mt19937 gen(rd());
        std::shuffle(result.begin(), result.end(), gen);
        SGXUtils::logInfo("CoalesceCompute::compute - Shuffled data for coalescing");
    }
    
    // 计算每个分区的元素数量
    size_t elementsPerPartition = result.size() / numPartitions;
    if (elementsPerPartition == 0) {
        elementsPerPartition = 1;
    }
    
    SGXUtils::logInfo("CoalesceCompute::compute - Coalesced " + std::to_string(result.size()) + 
                     " elements into " + std::to_string(numPartitions) + " partitions " +
                     "(shuffle: " + (shuffle ? "true" : "false") + ")");
    return result;
}

} // namespace spark_sgx
