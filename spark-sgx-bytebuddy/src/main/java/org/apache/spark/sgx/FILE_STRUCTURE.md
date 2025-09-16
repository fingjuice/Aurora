# SGX Spark ByteBuddy 文件结构

## 文件组织

### 核心文件
- `SGXSparkAgent.java` - 主代理类，负责安装字节码转换
- `SGXJNIWrapper.java` - JNI接口，连接Java和C++ SGX后端
- `SGXDefaultRDDAdvice.java` - 默认拦截器（备用）

### RDD拦截器（按RDD类型）
每个RDD类型都有独立的Advice和Factory文件：

#### MapPartitionsRDD (map, mapPartitions)
- `SGXMapPartitionsRDDAdvice.java` - 拦截MapPartitionsRDD.compute()
- `SGXMapPartitionsRDDFactory.java` - 处理MapPartitionsRDD的SGX操作

#### FilteredRDD (filter)
- `SGXFilteredRDDAdvice.java` - 拦截FilteredRDD.compute()
- `SGXFilteredRDDFactory.java` - 处理FilteredRDD的SGX操作

#### FlatMappedRDD (flatMap)
- `SGXFlatMappedRDDAdvice.java` - 拦截FlatMappedRDD.compute()
- `SGXFlatMappedRDDFactory.java` - 处理FlatMappedRDD的SGX操作

#### UnionRDD (union)
- `SGXUnionRDDAdvice.java` - 拦截UnionRDD.compute()
- `SGXUnionRDDFactory.java` - 处理UnionRDD的SGX操作

#### IntersectionRDD (intersection)
- `SGXIntersectionRDDAdvice.java` - 拦截IntersectionRDD.compute()
- `SGXIntersectionRDDFactory.java` - 处理IntersectionRDD的SGX操作

#### DistinctRDD (distinct)
- `SGXDistinctRDDAdvice.java` - 拦截DistinctRDD.compute()
- `SGXDistinctRDDFactory.java` - 处理DistinctRDD的SGX操作

#### GroupedRDD (groupByKey)
- `SGXGroupedRDDAdvice.java` - 拦截GroupedRDD.compute()
- `SGXGroupedRDDFactory.java` - 处理GroupedRDD的SGX操作

#### ReducedRDD (reduceByKey)
- `SGXReducedRDDAdvice.java` - 拦截ReducedRDD.compute()
- `SGXReducedRDDFactory.java` - 处理ReducedRDD的SGX操作

#### JoinedRDD (join)
- `SGXJoinedRDDAdvice.java` - 拦截JoinedRDD.compute()
- `SGXJoinedRDDFactory.java` - 处理JoinedRDD的SGX操作

## 架构特点

### 1. 模块化设计
- 每个RDD类型独立文件
- 清晰的职责分离
- 便于维护和扩展

### 2. 统一命名规范
- `SGX` + `RDD类型` + `Advice` = 拦截器
- `SGX` + `RDD类型` + `Factory` = 工厂类
- 所有文件都以SGX开头

### 3. 拦截策略
- 只拦截已实现的RDD类型
- 只拦截compute方法
- 每个RDD类型有专门的拦截器

### 4. 执行流程
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
SGX Enclave 执行实际计算
    ↓
返回结果给Java
```

## 文件统计
- 总文件数: 21个
- 核心文件: 3个
- RDD拦截器: 9个
- RDD工厂: 9个

## 优势
1. **清晰的结构** - 每个RDD类型独立管理
2. **易于维护** - 修改某个RDD类型只需修改对应文件
3. **便于扩展** - 添加新RDD类型只需创建新文件
4. **职责分离** - Advice负责拦截，Factory负责处理
5. **统一规范** - 所有文件遵循相同的命名和结构规范
