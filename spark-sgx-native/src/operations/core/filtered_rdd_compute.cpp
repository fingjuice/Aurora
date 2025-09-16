#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>

namespace spark_sgx {

std::vector<std::string> FilteredRDDCompute::compute(
    const std::vector<std::string>& inputData,
    const std::string& operationData) {
    
    SGXUtils::logInfo("FilteredRDDCompute::compute - Processing " + std::to_string(inputData.size()) + " elements");
    
    std::vector<std::string> result;
    result.reserve(inputData.size());
    
    // 解析操作数据获取filter谓词
    // 这里简化实现，实际应该反序列化真正的谓词
    auto filterPredicate = [](const std::string& item) -> bool {
        return !item.empty(); // 简单的非空检查
    };
    
    // 对每个元素应用filter谓词
    for (size_t i = 0; i < inputData.size(); ++i) {
        try {
            const std::string& item = inputData[i];
            if (filterPredicate(item)) {
                result.push_back(item);
            }
            
            // 每处理1000个元素记录一次进度
            if ((i + 1) % 1000 == 0) {
                SGXUtils::logInfo("FilteredRDDCompute::compute - Processed " + std::to_string(i + 1) + " elements");
            }
        } catch (const std::exception& e) {
            SGXUtils::logError("FilteredRDDCompute::compute - Predicate failed for item " + std::to_string(i) + 
                              ": " + std::string(e.what()));
            // 继续处理其他元素，不中断整个操作
        }
    }
    
    SGXUtils::logInfo("FilteredRDDCompute::compute - Completed filtering " + std::to_string(inputData.size()) + 
                     " elements to " + std::to_string(result.size()) + " elements");
    return result;
}

} // namespace spark_sgx

