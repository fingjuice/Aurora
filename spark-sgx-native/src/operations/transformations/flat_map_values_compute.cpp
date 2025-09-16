#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>
#include <vector>

namespace spark_sgx {

std::vector<std::string> FlatMapValuesCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("FlatMapValuesCompute::compute - Processing " + std::to_string(inputData.size()) + " key-value pairs");
    
    std::vector<std::string> result;
    
    // 解析操作数据获取flatMap函数
    // 这里简化实现，实际应该反序列化真正的函数
    auto flatMapFunction = [](const std::string& value) -> std::vector<std::string> {
        // 简单的分割实现
        std::vector<std::string> mappedValues;
        std::stringstream ss(value);
        std::string item;
        while (std::getline(ss, item, ' ')) {
            if (!item.empty()) {
                mappedValues.push_back("flatmapped_" + item);
            }
        }
        return mappedValues;
    };
    
    // 对每个key-value对应用flatMap函数到value
    for (size_t i = 0; i < inputData.size(); ++i) {
        try {
            const std::string& item = inputData[i];
            
            // 解析key-value对
            size_t delimiterPos = item.find(":");
            if (delimiterPos != std::string::npos) {
                std::string key = item.substr(0, delimiterPos);
                std::string value = item.substr(delimiterPos + 1);
                
                // 对value应用flatMap函数
                std::vector<std::string> mappedValues = flatMapFunction(value);
                for (const auto& mappedValue : mappedValues) {
                    result.push_back(key + ":" + mappedValue);
                }
            } else {
                // 如果不是key-value格式，直接处理
                std::vector<std::string> mappedValues = flatMapFunction(item);
                result.insert(result.end(), mappedValues.begin(), mappedValues.end());
            }
            
            // 每处理1000个元素记录一次进度
            if ((i + 1) % 1000 == 0) {
                SGXUtils::logInfo("FlatMapValuesCompute::compute - Processed " + std::to_string(i + 1) + " elements");
            }
        } catch (const std::exception& e) {
            SGXUtils::logError("FlatMapValuesCompute::compute - Function failed for item " + std::to_string(i) + 
                              ": " + std::string(e.what()));
            result.push_back(inputData[i]); // 返回原始元素
        }
    }
    
    SGXUtils::logInfo("FlatMapValuesCompute::compute - Completed processing " + std::to_string(result.size()) + " elements");
    return result;
}

} // namespace spark_sgx
