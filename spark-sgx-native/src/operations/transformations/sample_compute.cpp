#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <random>
#include <algorithm>
#include <numeric>

namespace spark_sgx {

std::vector<std::string> SampleCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("SampleCompute::compute - Sampling from " + std::to_string(inputData.size()) + " elements");
    
    // 解析操作数据获取采样参数
    double fraction = 0.1; // 默认采样比例
    bool withReplacement = false; // 默认无放回采样
    long seed = 42; // 默认随机种子
    
    if (!operationData.empty()) {
        try {
            // 解析格式: "fraction:withReplacement:seed"
            std::stringstream ss(operationData);
            std::string token;
            if (std::getline(ss, token, ':')) {
                fraction = std::stod(token);
            }
            if (std::getline(ss, token, ':')) {
                withReplacement = (token == "true");
            }
            if (std::getline(ss, token, ':')) {
                seed = std::stol(token);
            }
        } catch (const std::exception& e) {
            SGXUtils::logWarning("SampleCompute::compute - Invalid operation data, using defaults");
        }
    }
    
    std::vector<std::string> result;
    
    if (inputData.empty()) {
        SGXUtils::logInfo("SampleCompute::compute - No data to sample");
        return result;
    }
    
    // 计算采样数量
    size_t sampleSize = static_cast<size_t>(inputData.size() * fraction);
    sampleSize = std::max(sampleSize, size_t(1)); // 至少采样1个
    sampleSize = std::min(sampleSize, inputData.size()); // 不超过总数
    
    // 设置随机数生成器
    std::mt19937 gen(seed);
    std::uniform_real_distribution<> dis(0.0, 1.0);
    
    if (withReplacement) {
        // 有放回采样
        for (size_t i = 0; i < sampleSize; ++i) {
            size_t randomIndex = static_cast<size_t>(dis(gen) * inputData.size());
            result.push_back(inputData[randomIndex]);
        }
    } else {
        // 无放回采样
        std::vector<size_t> indices(inputData.size());
        std::iota(indices.begin(), indices.end(), 0);
        
        // 随机打乱
        std::shuffle(indices.begin(), indices.end(), gen);
        
        // 取前sampleSize个
        for (size_t i = 0; i < sampleSize; ++i) {
            result.push_back(inputData[indices[i]]);
        }
    }
    
    SGXUtils::logInfo("SampleCompute::compute - Sampled " + std::to_string(result.size()) + " elements from " + 
                     std::to_string(inputData.size()) + " (fraction: " + std::to_string(fraction) + ")");
    return result;
}

} // namespace spark_sgx
