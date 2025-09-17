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
 * @brief Reduce operation types
 */
enum class ReduceType {
    SUM,
    PRODUCT,
    MIN,
    MAX,
    CONCATENATE
};

/**
 * @brief Map function types for transformations
 */
enum class MapFunctionType {
    IDENTITY,
    UPPERCASE,
    LOWERCASE,
    REVERSE,
    LENGTH,
    REGEX_REPLACE,
    CUSTOM,
    STRING_TRANSFORM
};

/**
 * @brief FlatMap function types for transformations
 */
enum class FlatMapFunctionType {
    SPLIT_WORDS,
    SPLIT_CHARS,
    SPLIT_LINES,
    SPLIT_CUSTOM,
    REGEX_SPLIT,
    CUSTOM
};

/**
 * @brief Filter types for filtering operations
 */
enum class FilterType {
    NON_EMPTY,
    CONTAINS,
    STARTS_WITH,
    ENDS_WITH,
    REGEX_MATCH,
    LENGTH_RANGE,
    NUMERIC
};

/**
 * @brief Join types for join operations
 */
enum class JoinType {
    INNER_JOIN,
    LEFT_JOIN,
    RIGHT_JOIN,
    FULL_OUTER_JOIN
};

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

private:
    void parseOperationData(const std::string& operationData, 
                          MapFunctionType& functionType, 
                          std::string& customFunction, 
                          bool& preserveOrder, 
                          bool& validateInput, 
                          bool& caseSensitive, 
                          std::string& delimiter, 
                          std::string& prefix, 
                          std::string& suffix);
    std::function<std::string(const std::string&)> getMapFunction(
        MapFunctionType functionType, 
        const std::string& customFunction, 
        bool caseSensitive, 
        const std::string& delimiter, 
        const std::string& prefix, 
        const std::string& suffix);
    bool validateInputItem(const std::string& item);
    bool validateOutputItem(const std::string& item);
};

/**
 * @brief FilteredRDD compute implementation
 */
class FilteredRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
    
private:
    void parseOperationData(const std::string& operationData, 
                          FilterType& filterType, 
                          std::string& pattern, 
                          bool& caseSensitive, 
                          bool& invertMatch, 
                          size_t& minLength, 
                          size_t& maxLength);
    std::function<bool(const std::string&)> 
        getFilterPredicate(FilterType filterType, 
                         const std::string& pattern, 
                         bool caseSensitive, 
                         size_t minLength, 
                         size_t maxLength);
};

/**
 * @brief FlatMappedRDD compute implementation
 */
class FlatMappedRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;

private:
    void parseOperationData(const std::string& operationData, 
                          FlatMapFunctionType& functionType, 
                          std::string& customFunction, 
                          bool& preserveOrder, 
                          bool& validateInput, 
                          bool& caseSensitive, 
                          std::string& delimiter, 
                          std::string& prefix, 
                          std::string& suffix, 
                          size_t& maxOutputElements);
    std::function<std::vector<std::string>(const std::string&)> getFlatMapFunction(
        FlatMapFunctionType functionType, 
        const std::string& customFunction, 
        bool caseSensitive, 
        const std::string& delimiter, 
        const std::string& prefix, 
        const std::string& suffix);
    bool validateInputItem(const std::string& item);
    bool validateOutputItem(const std::string& item);
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
    void parseOperationData(const std::string& operationData, 
                          std::string& delimiter, 
                          bool& removeDuplicates, 
                          bool& preserveOrder, 
                          bool& validateData, 
                          size_t& maxElements, 
                          std::string& prefix, 
                          std::string& suffix);
    std::vector<std::string> parseOtherRDDData(const std::string& operationData, const std::string& delimiter);
    std::string processItem(const std::string& item, const std::string& prefix, const std::string& suffix);
    bool validateItem(const std::string& item);
    void validateInputData(const std::vector<std::string>& data, const std::string& dataName);
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
    void parseOperationData(const std::string& operationData, 
                          std::string& delimiter, 
                          bool& removeDuplicates, 
                          bool& preserveOrder, 
                          bool& validateData, 
                          size_t& maxElements, 
                          std::string& prefix, 
                          std::string& suffix, 
                          bool& caseSensitive);
    std::vector<std::string> parseOtherRDDData(const std::string& operationData, const std::string& delimiter);
    std::vector<std::string> processData(const std::vector<std::string>& data, 
                                       const std::string& prefix, 
                                       const std::string& suffix, 
                                       bool caseSensitive);
    bool validateItem(const std::string& item);
    void validateInputData(const std::vector<std::string>& data, const std::string& dataName);
};

/**
 * @brief DistinctRDD compute implementation
 */
class DistinctRDDCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;

private:
    void parseOperationData(const std::string& operationData, 
                          bool& caseSensitive, 
                          bool& preserveOrder, 
                          bool& validateData, 
                          size_t& maxElements, 
                          std::string& prefix, 
                          std::string& suffix, 
                          bool& trimWhitespace, 
                          bool& normalizeCase);
    std::string processItem(const std::string& item, 
                          const std::string& prefix, 
                          const std::string& suffix, 
                          bool trimWhitespace, 
                          bool normalizeCase, 
                          bool caseSensitive);
    bool validateItem(const std::string& item);
    void validateInputData(const std::vector<std::string>& data);
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
    void parseOperationData(const std::string& operationData, 
                          JoinType& joinType, 
                          std::string& delimiter, 
                          std::string& keyValueDelimiter, 
                          std::string& resultDelimiter, 
                          bool& validateKeys, 
                          bool& caseSensitive);
    std::vector<std::string> parseOtherRDDData(const std::string& operationData, const std::string& delimiter);
    std::pair<std::string, std::string> parseKeyValuePair(const std::string& item, const std::string& delimiter);
    std::string serializeJoinResult(const std::string& key, const std::string& value1, const std::string& value2, const std::string& delimiter);
    std::string toLowerCase(const std::string& str);
    size_t performInnerJoin(const std::unordered_map<std::string, std::vector<std::string>>& leftMap,
                          const std::unordered_map<std::string, std::vector<std::string>>& rightMap,
                          std::vector<std::string>& result,
                          const std::string& delimiter);
    size_t performLeftJoin(const std::unordered_map<std::string, std::vector<std::string>>& leftMap,
                         const std::unordered_map<std::string, std::vector<std::string>>& rightMap,
                         std::vector<std::string>& result,
                         const std::string& delimiter);
    size_t performRightJoin(const std::unordered_map<std::string, std::vector<std::string>>& leftMap,
                          const std::unordered_map<std::string, std::vector<std::string>>& rightMap,
                          std::vector<std::string>& result,
                          const std::string& delimiter);
    size_t performFullOuterJoin(const std::unordered_map<std::string, std::vector<std::string>>& leftMap,
                              const std::unordered_map<std::string, std::vector<std::string>>& rightMap,
                              std::vector<std::string>& result,
                              const std::string& delimiter);
    std::string getJoinTypeName(JoinType joinType);
};

/**
 * @brief CollectAction compute implementation
 */
class CollectActionCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
    
private:
    void parseOperationData(const std::string& operationData, 
                          bool& sortResults, 
                          bool& removeDuplicates, 
                          size_t& maxElements);
    bool validateDataItem(const std::string& item);
};

/**
 * @brief CountAction compute implementation
 */
class CountActionCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
    
private:
    void parseOperationData(const std::string& operationData, 
                          bool& countDistinct, 
                          bool& countNonEmpty, 
                          std::string& filterPattern);
    bool validateAndFilterItem(const std::string& item, 
                             bool countNonEmpty, 
                             const std::string& filterPattern);
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
    
private:
    void parseOperationData(const std::string& operationData, 
                          ReduceType& reduceType, 
                          bool& parallelReduce, 
                          size_t& batchSize);
    std::function<std::string(const std::string&, const std::string&)> 
        getReduceFunction(ReduceType reduceType);
    std::string sequentialReduceImpl(
        const std::vector<std::string>& inputData,
        const std::function<std::string(const std::string&, const std::string&)>& reduceFunction);
    std::string parallelReduceImpl(
        const std::vector<std::string>& inputData,
        const std::function<std::string(const std::string&, const std::string&)>& reduceFunction,
        size_t batchSize);
};

/**
 * @brief MapValues compute implementation
 */
class MapValuesCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
    
private:
    void parseOperationData(const std::string& operationData, 
                          MapFunctionType& functionType, 
                          std::string& transformPattern, 
                          bool& preserveKeys, 
                          bool& validateInput);
    std::function<std::string(const std::string&)> 
        getMapFunction(MapFunctionType functionType, const std::string& transformPattern);
    bool validateKeyValueFormat(const std::string& item);
    std::pair<std::string, std::string> parseKeyValuePair(const std::string& item);
};

/**
 * @brief FlatMapValues compute implementation
 */
class FlatMapValuesCompute : public RDDCompute {
public:
    std::vector<std::string> compute(
        const std::vector<std::string>& inputData,
        const std::string& operationData) override;
    
private:
    void parseOperationData(const std::string& operationData, 
                          FlatMapFunctionType& functionType, 
                          std::string& delimiter, 
                          std::string& prefix, 
                          bool& preserveKeys, 
                          bool& validateInput, 
                          size_t& maxOutputPerInput);
    std::function<std::vector<std::string>(const std::string&)> 
        getFlatMapFunction(FlatMapFunctionType functionType, 
                          const std::string& delimiter, 
                          const std::string& prefix, 
                          size_t maxOutputPerInput);
    bool validateKeyValueFormat(const std::string& item);
    std::pair<std::string, std::string> parseKeyValuePair(const std::string& item);
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
