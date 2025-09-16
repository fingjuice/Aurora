#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>
#include <sstream>

namespace spark_sgx {

std::vector<std::string> MapPartitionsRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("MapPartitionsRDDCompute::compute - Processing " + std::to_string(inputData.size()) + " elements");
    
    std::vector<std::string> result;
    result.reserve(inputData.size());
    
    // 解析操作数据获取map函数
    // 这里简化实现，实际应该反序列化真正的函数
    auto mapFunction = [](const std::string& item) -> std::string {
        return "mapped_" + item;
    };
    
    // 对每个元素应用map函数
    for (size_t i = 0; i < inputData.size(); ++i) {
        try {
            const std::string& item = inputData[i];
            std::string mappedItem = mapFunction(item);
            result.push_back(mappedItem);
            
            // 每处理1000个元素记录一次进度
            if ((i + 1) % 1000 == 0) {
                SGXUtils::logInfo("MapPartitionsRDDCompute::compute - Processed " + std::to_string(i + 1) + " elements");
            }
        } catch (const std::exception& e) {
            SGXUtils::logError("MapPartitionsRDDCompute::compute - Function failed for item " + std::to_string(i) + 
                              ": " + std::string(e.what()));
            // 继续处理其他元素，不中断整个操作
            result.push_back(inputData[i]); // 返回原始元素
        }
    }
    
    SGXUtils::logInfo("MapPartitionsRDDCompute::compute - Completed processing " + std::to_string(result.size()) + " elements");
    return result;
}

} // namespace spark_sgx

