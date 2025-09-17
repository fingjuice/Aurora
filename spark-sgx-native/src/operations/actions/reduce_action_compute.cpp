#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <functional>

namespace spark_sgx {

std::vector<std::string> ReduceActionCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    auto startTime = std::chrono::high_resolution_clock::now();
    SGXUtils::logInfo("ReduceActionCompute::compute - Starting reduce operation on " + std::to_string(inputData.size()) + " elements");
    
    try {
        std::vector<std::string> result;
        
        if (inputData.empty()) {
            SGXUtils::logWarning("ReduceActionCompute::compute - No elements to reduce");
            return result;
        }
        
        if (inputData.size() == 1) {
            result.push_back(inputData[0]);
            SGXUtils::logInfo("ReduceActionCompute::compute - Single element, returning as-is");
            return result;
        }
        
        // 解析操作数据获取reduce函数类型
        ReduceType reduceType = ReduceType::CONCATENATE;
        bool parallelReduce = false;
        size_t batchSize = 1000;
        
        if (!operationData.empty()) {
            parseOperationData(operationData, reduceType, parallelReduce, batchSize);
        }
        
        // 获取reduce函数
        auto reduceFunction = getReduceFunction(reduceType);
        
        std::string reduced;
        if (parallelReduce && inputData.size() > batchSize) {
            reduced = parallelReduceImpl(inputData, reduceFunction, batchSize);
        } else {
            reduced = sequentialReduceImpl(inputData, reduceFunction);
        }
        
        result.push_back(reduced);
        
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        
        SGXUtils::logInfo("ReduceActionCompute::compute - Reduced " + std::to_string(inputData.size()) + 
                         " elements to: " + reduced + " in " + std::to_string(duration.count()) + "ms");
        return result;
        
    } catch (const std::exception& e) {
        SGXUtils::logError("ReduceActionCompute::compute - Error during reduce operation: " + std::string(e.what()));
        throw;
    }
}

void ReduceActionCompute::parseOperationData(const std::string& operationData, 
                                           ReduceType& reduceType, 
                                           bool& parallelReduce, 
                                           size_t& batchSize) {
    try {
        std::istringstream iss(operationData);
        std::string token;
        
        // 解析格式: "reduceType:parallelReduce:batchSize"
        if (std::getline(iss, token, ':')) {
            if (token == "SUM") reduceType = ReduceType::SUM;
            else if (token == "PRODUCT") reduceType = ReduceType::PRODUCT;
            else if (token == "MIN") reduceType = ReduceType::MIN;
            else if (token == "MAX") reduceType = ReduceType::MAX;
            else if (token == "CONCATENATE") reduceType = ReduceType::CONCATENATE;
        }
        if (std::getline(iss, token, ':')) {
            parallelReduce = (token == "true");
        }
        if (std::getline(iss, token, ':')) {
            batchSize = std::stoull(token);
        }
    } catch (const std::exception& e) {
        SGXUtils::logWarning("ReduceActionCompute::parseOperationData - Invalid operation data: " + std::string(e.what()));
    }
}

std::function<std::string(const std::string&, const std::string&)> 
ReduceActionCompute::getReduceFunction(ReduceType reduceType) {
    switch (reduceType) {
        case ReduceType::SUM:
            return [](const std::string& a, const std::string& b) -> std::string {
                try {
                    double valA = std::stod(a);
                    double valB = std::stod(b);
                    return std::to_string(valA + valB);
                } catch (...) {
                    return a + "+" + b; // 回退到字符串连接
                }
            };
        case ReduceType::PRODUCT:
            return [](const std::string& a, const std::string& b) -> std::string {
                try {
                    double valA = std::stod(a);
                    double valB = std::stod(b);
                    return std::to_string(valA * valB);
                } catch (...) {
                    return a + "*" + b; // 回退到字符串连接
                }
            };
        case ReduceType::MIN:
            return [](const std::string& a, const std::string& b) -> std::string {
                return (a < b) ? a : b;
            };
        case ReduceType::MAX:
            return [](const std::string& a, const std::string& b) -> std::string {
                return (a > b) ? a : b;
            };
        case ReduceType::CONCATENATE:
        default:
            return [](const std::string& a, const std::string& b) -> std::string {
                return a + "_" + b;
            };
    }
}

std::string ReduceActionCompute::sequentialReduceImpl(
    const std::vector<std::string>& inputData,
    const std::function<std::string(const std::string&, const std::string&)>& reduceFunction) {
    
    std::string result = inputData[0];
    for (size_t i = 1; i < inputData.size(); ++i) {
        result = reduceFunction(result, inputData[i]);
        
        // 每处理1000个元素记录一次进度
        if (i % 1000 == 0) {
            SGXUtils::logInfo("ReduceActionCompute::sequentialReduceImpl - Processed " + std::to_string(i) + " elements");
        }
    }
    return result;
}

std::string ReduceActionCompute::parallelReduceImpl(
    const std::vector<std::string>& inputData,
    const std::function<std::string(const std::string&, const std::string&)>& reduceFunction,
    size_t batchSize) {
    
    // 简化的并行reduce实现
    // 在实际实现中，这里会使用真正的并行处理
    std::vector<std::string> batchResults;
    
    for (size_t i = 0; i < inputData.size(); i += batchSize) {
        size_t endIdx = std::min(i + batchSize, inputData.size());
        std::vector<std::string> batch(inputData.begin() + i, inputData.begin() + endIdx);
        
        if (batch.size() == 1) {
            batchResults.push_back(batch[0]);
        } else {
            std::string batchResult = batch[0];
            for (size_t j = 1; j < batch.size(); ++j) {
                batchResult = reduceFunction(batchResult, batch[j]);
            }
            batchResults.push_back(batchResult);
        }
    }
    
    // 合并批次结果
    if (batchResults.empty()) {
        return "";
    }
    
    std::string result = batchResults[0];
    for (size_t i = 1; i < batchResults.size(); ++i) {
        result = reduceFunction(result, batchResults[i]);
    }
    
    return result;
}

} // namespace spark_sgx
