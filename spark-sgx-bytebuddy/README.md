# AgentSpark - Simple Configuration

## 配置文件

编辑 `src/main/resources/sgx-spark-config.properties`:

```properties
# RDD类路径，用逗号分隔
rdd.types=org.apache.spark.rdd.MapPartitionsRDD,org.apache.spark.rdd.FilteredRDD,org.apache.spark.rdd.FlatMappedRDD

# Actions方法，用逗号分隔  
actions=collect,count,reduce,first,take,foreach

# Transformations方法，用逗号分隔
transformations=mapValues,flatMapValues,sample,sortBy,coalesce
```

## 使用方法

```bash
java -javaagent:spark-sgx-bytebuddy-1.0.0.jar your.spark.application.Main
```

## 添加新的RDD类型

1. 在配置文件中添加RDD类路径
2. 在 `AgentSpark.java` 的 `getAdviceClass()` 方法中添加对应的case
3. 创建对应的Advice类

## 添加新的Actions/Transformations

1. 在配置文件中添加方法名
2. 在 `AgentSpark.java` 的对应方法中添加case
3. 创建对应的Advice类