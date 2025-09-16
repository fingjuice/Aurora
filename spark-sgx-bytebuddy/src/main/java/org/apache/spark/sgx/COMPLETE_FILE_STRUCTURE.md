# SGX Spark ByteBuddy 完整文件结构

## 实际拦截的RDD类型

根据Spark源码分析，我们拦截以下RDD类型的compute方法：

### 1. MapPartitionsRDD
- **对应操作**: `map()`, `mapPartitions()`
- **SGX实现**: `rdd_map.cpp`, `rdd_mappartitions.cpp`
- **Java文件**: 
  - `SGXMapPartitionsRDDAdvice.java` - 拦截器
  - `SGXMapPartitionsRDDFactory.java` - 工厂类

### 2. FilteredRDD
- **对应操作**: `filter()`
- **SGX实现**: `rdd_filter.cpp`
- **Java文件**: 
  - `SGXFilteredRDDAdvice.java` - 拦截器
  - `SGXFilteredRDDFactory.java` - 工厂类

### 3. FlatMappedRDD
- **对应操作**: `flatMap()`
- **SGX实现**: `rdd_flatmap.cpp`
- **Java文件**: 
  - `SGXFlatMappedRDDAdvice.java` - 拦截器
  - `SGXFlatMappedRDDFactory.java` - 工厂类

### 4. UnionRDD
- **对应操作**: `union()`
- **SGX实现**: `rdd_union.cpp`
- **Java文件**: 
  - `SGXUnionRDDAdvice.java` - 拦截器
  - `SGXUnionRDDFactory.java` - 工厂类

### 5. IntersectionRDD
- **对应操作**: `intersection()`
- **SGX实现**: `rdd_intersection.cpp`
- **Java文件**: 
  - `SGXIntersectionRDDAdvice.java` - 拦截器
  - `SGXIntersectionRDDFactory.java` - 工厂类

### 6. DistinctRDD
- **对应操作**: `distinct()`
- **SGX实现**: `rdd_distinct.cpp`
- **Java文件**: 
  - `SGXDistinctRDDAdvice.java` - 拦截器
  - `SGXDistinctRDDFactory.java` - 工厂类

### 7. GroupedRDD
- **对应操作**: `groupByKey()`
- **SGX实现**: `rdd_groupbykey.cpp`
- **Java文件**: 
  - `SGXGroupedRDDAdvice.java` - 拦截器
  - `SGXGroupedRDDFactory.java` - 工厂类

### 8. ReducedRDD
- **对应操作**: `reduceByKey()`
- **SGX实现**: `rdd_reducebykey.cpp`
- **Java文件**: 
  - `SGXReducedRDDAdvice.java` - 拦截器
  - `SGXReducedRDDFactory.java` - 工厂类

### 9. JoinedRDD
- **对应操作**: `join()`
- **SGX实现**: `rdd_join.cpp`
- **Java文件**: 
  - `SGXJoinedRDDAdvice.java` - 拦截器
  - `SGXJoinedRDDFactory.java` - 工厂类

## 不拦截的RDD操作

以下操作在SGX中有实现，但不创建新的RDD，因此不需要拦截：

### Actions (直接返回结果或执行副作用)
- `collect()` - 返回Array，不创建RDD
- `count()` - 返回Long，不创建RDD
- `first()` - 返回单个元素，不创建RDD
- `foreach()` - 执行副作用，不创建RDD
- `saveAsTextFile()` - 执行副作用，不创建RDD
- `take()` - 返回Array，不创建RDD

### Transformations (但通常不通过compute方法)
- `reduce()` - 通常作为action使用，不创建RDD

## 文件统计

### 核心文件 (3个)
- `SGXSparkAgent.java` - 主代理类
- `SGXJNIWrapper.java` - JNI接口
- `SGXDefaultRDDAdvice.java` - 默认拦截器

### RDD拦截器 (9个)
- `SGXMapPartitionsRDDAdvice.java`
- `SGXFilteredRDDAdvice.java`
- `SGXFlatMappedRDDAdvice.java`
- `SGXUnionRDDAdvice.java`
- `SGXIntersectionRDDAdvice.java`
- `SGXDistinctRDDAdvice.java`
- `SGXGroupedRDDAdvice.java`
- `SGXReducedRDDAdvice.java`
- `SGXJoinedRDDAdvice.java`

### RDD工厂 (9个)
- `SGXMapPartitionsRDDFactory.java`
- `SGXFilteredRDDFactory.java`
- `SGXFlatMappedRDDFactory.java`
- `SGXUnionRDDFactory.java`
- `SGXIntersectionRDDFactory.java`
- `SGXDistinctRDDFactory.java`
- `SGXGroupedRDDFactory.java`
- `SGXReducedRDDFactory.java`
- `SGXJoinedRDDFactory.java`

**总计**: 21个文件

## 架构优势

1. **精确拦截** - 只拦截真正会创建新RDD的操作
2. **完整覆盖** - 覆盖了所有主要的RDD transformation操作
3. **模块化设计** - 每个RDD类型独立管理
4. **易于维护** - 修改某个RDD类型只需修改对应文件
5. **便于扩展** - 添加新RDD类型只需创建新文件

## 执行流程

```
用户代码: rdd.map(x => x * 2)
    ↓
RDD.map() 创建 MapPartitionsRDD
    ↓
MapPartitionsRDD.compute() ← SGXMapPartitionsRDDAdvice拦截
    ↓
SGXMapPartitionsRDDFactory.createSGXMapPartitionsRDD()
    ↓
SGXJNIWrapper.nativeExecuteMapPartitionsRDD()
    ↓
SGX Enclave 执行 rdd_map.cpp 或 rdd_mappartitions.cpp
    ↓
返回结果给Java
```
