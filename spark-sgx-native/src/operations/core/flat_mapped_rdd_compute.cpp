#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>

namespace spark_sgx {

std::vector<std::string> FlatMappedRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("FlatMappedRDDCompute::compute - Processing " + std::to_string(inputData.size()) + " elements");
    
    std::vector<std::string> result;
    
    // 解析操作数据获取flatMap函数
    // 这里简化实现，实际应该反序列化真正的函数
    auto flatMapFunction = [](const std::string& item) -> std::vector<std::string> {
        return {"flatmapped_" + item};
    };
    
    // 对每个元素应用flatMap函数
    for (size_t i = 0; i < inputData.size(); ++i) {
        try {
            const std::string& item = inputData[i];
            auto flatMappedItems = flatMapFunction(item);
            result.insert(result.end(), flatMappedItems.begin(), flatMappedItems.end());
            
            // 每处理1000个元素记录一次进度
            if ((i + 1) % 1000 == 0) {
                SGXUtils::logInfo("FlatMappedRDDCompute::compute - Processed " + std::to_string(i + 1) + " elements");
            }
        } catch (const std::exception& e) {
            SGXUtils::logError("FlatMappedRDDCompute::compute - Function failed for item " + std::to_string(i) + 
                              ": " + std::string(e.what()));
            // 继续处理其他元素，不中断整个操作
        }
    }
    
    SGXUtils::logInfo("FlatMappedRDDCompute::compute - Completed processing " + std::to_string(inputData.size()) + 
                     " elements to " + std::to_string(result.size()) + " elements");
    return result;
}

} // namespace spark_sgx

