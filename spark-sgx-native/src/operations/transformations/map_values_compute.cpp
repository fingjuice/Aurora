#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> MapValuesCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("MapValuesCompute::compute - Processing " + std::to_string(inputData.size()) + " key-value pairs");
    
    std::vector<std::string> result;
    result.reserve(inputData.size());
    
    // 解析操作数据获取map函数
    // 这里简化实现，实际应该反序列化真正的函数
    auto mapFunction = [](const std::string& value) -> std::string {
        return "mapped_" + value;
    };
    
    // 对每个key-value对应用map函数到value
    for (size_t i = 0; i < inputData.size(); ++i) {
        try {
            const std::string& item = inputData[i];
            
            // 解析key-value对
            size_t delimiterPos = item.find(":");
            if (delimiterPos != std::string::npos) {
                std::string key = item.substr(0, delimiterPos);
                std::string value = item.substr(delimiterPos + 1);
                
                // 对value应用map函数
                std::string mappedValue = mapFunction(value);
                result.push_back(key + ":" + mappedValue);
            } else {
                // 如果不是key-value格式，直接处理
                result.push_back(mapFunction(item));
            }
            
            // 每处理1000个元素记录一次进度
            if ((i + 1) % 1000 == 0) {
                SGXUtils::logInfo("MapValuesCompute::compute - Processed " + std::to_string(i + 1) + " elements");
            }
        } catch (const std::exception& e) {
            SGXUtils::logError("MapValuesCompute::compute - Function failed for item " + std::to_string(i) + 
                              ": " + std::string(e.what()));
            result.push_back(inputData[i]); // 返回原始元素
        }
    }
    
    SGXUtils::logInfo("MapValuesCompute::compute - Completed processing " + std::to_string(result.size()) + " elements");
    return result;
}

} // namespace spark_sgx
