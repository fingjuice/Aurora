#include "operations/rdd_compute.h"
#include "utils/sgx_utils.h"
#include <iostream>

namespace spark_sgx {

std::unique_ptr<RDDCompute> RDDComputeFactory::createCompute(const std::string& rddType) {
    SGXUtils::logInfo("RDDComputeFactory::createCompute - Creating compute for RDD type: " + rddType);
    
    if (rddType == "MapPartitionsRDD") {
        return std::make_unique<MapPartitionsRDDCompute>();
    } else if (rddType == "FilteredRDD") {
        return std::make_unique<FilteredRDDCompute>();
    } else if (rddType == "FlatMappedRDD") {
        return std::make_unique<FlatMappedRDDCompute>();
    } else if (rddType == "UnionRDD") {
        return std::make_unique<UnionRDDCompute>();
    } else if (rddType == "IntersectionRDD") {
        return std::make_unique<IntersectionRDDCompute>();
    } else if (rddType == "DistinctRDD") {
        return std::make_unique<DistinctRDDCompute>();
    } else if (rddType == "GroupedRDD") {
        return std::make_unique<GroupedRDDCompute>();
    } else if (rddType == "ReducedRDD") {
        return std::make_unique<ReducedRDDCompute>();
    } else if (rddType == "JoinedRDD") {
        return std::make_unique<JoinedRDDCompute>();
    } else if (rddType == "CollectAction") {
        return std::make_unique<CollectActionCompute>();
    } else if (rddType == "CountAction") {
        return std::make_unique<CountActionCompute>();
    } else if (rddType == "FirstAction") {
        return std::make_unique<FirstActionCompute>();
    } else if (rddType == "TakeAction") {
        return std::make_unique<TakeActionCompute>();
    } else if (rddType == "ForeachAction") {
        return std::make_unique<ForeachActionCompute>();
    } else if (rddType == "ReduceAction") {
        return std::make_unique<ReduceActionCompute>();
    } else if (rddType == "MapValues") {
        return std::make_unique<MapValuesCompute>();
    } else if (rddType == "FlatMapValues") {
        return std::make_unique<FlatMapValuesCompute>();
    } else if (rddType == "Sample") {
        return std::make_unique<SampleCompute>();
    } else if (rddType == "SortBy") {
        return std::make_unique<SortByCompute>();
    } else if (rddType == "Coalesce") {
        return std::make_unique<CoalesceCompute>();
    } else {
        SGXUtils::logWarning("RDDComputeFactory::createCompute - Unknown RDD type: " + rddType);
        return nullptr;
    }
}

} // namespace spark_sgx

