#ifndef RDD_COMPUTE_H
#define RDD_COMPUTE_H

#include <vector>
#include <string>
#include <functional>
#include <memory>

/**
 * RDD Compute Operations for SGX
 * 
 * This header defines the core compute operations that are executed within SGX enclaves.
 * Each RDD type has its own compute method that implements the actual computation logic.
 */

namespace spark_sgx {

/**
 * @brief Base class for RDD compute operations
 */
class RDDCompute {
public:
    virtual ~RDDCompute() = default;
    virtual std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) = 0;
};

/**
 * @brief MapPartitionsRDD compute implementation
 */
class MapPartitionsRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief FilteredRDD compute implementation
 */
class FilteredRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief FlatMappedRDD compute implementation
 */
class FlatMappedRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief UnionRDD compute implementation
 */
class UnionRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;

private:
    std::vector<std::string> parseOtherRDDData(const std::string& operationData);
};

/**
 * @brief IntersectionRDD compute implementation
 */
class IntersectionRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;

private:
    std::vector<std::string> parseOtherRDDData(const std::string& operationData);
};

/**
 * @brief DistinctRDD compute implementation
 */
class DistinctRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief GroupedRDD compute implementation
 */
class GroupedRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;

private:
    std::pair<std::string, std::string> parseKeyValuePair(const std::string& item);
    std::string serializeGroup(const std::string& key, const std::vector<std::string>& values);
};

/**
 * @brief ReducedRDD compute implementation
 */
class ReducedRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;

private:
    std::pair<std::string, std::string> parseKeyValuePair(const std::string& item);
    std::string serializeKeyValuePair(const std::string& key, const std::string& value);
};

/**
 * @brief JoinedRDD compute implementation
 */
class JoinedRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;

private:
    std::vector<std::string> parseOtherRDDData(const std::string& operationData);
    std::pair<std::string, std::string> parseKeyValuePair(const std::string& item);
    std::string serializeJoinResult(const std::string& key, const std::string& value1, const std::string& value2);
};

/**
 * @brief CollectAction compute implementation
 */
class CollectActionCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief CountAction compute implementation
 */
class CountActionCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief FirstAction compute implementation
 */
class FirstActionCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief TakeAction compute implementation
 */
class TakeActionCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief ForeachAction compute implementation
 */
class ForeachActionCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief ReduceAction compute implementation
 */
class ReduceActionCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief MapValues compute implementation
 */
class MapValuesCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief FlatMapValues compute implementation
 */
class FlatMapValuesCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief Sample compute implementation
 */
class SampleCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief SortBy compute implementation
 */
class SortByCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief Coalesce compute implementation
 */
class CoalesceCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
};

/**
 * @brief RDD Compute Factory
 */
class RDDComputeFactory {
public:
    static std::unique_ptr<RDDCompute> createCompute(const std::string& rddType);
};

} // namespace spark_sgx

#endif // RDD_COMPUTE_H
