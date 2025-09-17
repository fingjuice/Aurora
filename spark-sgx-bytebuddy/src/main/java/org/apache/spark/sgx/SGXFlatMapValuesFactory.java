package org.apache.spark.sgx;

import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * SGX FlatMapValues Factory - 处理FlatMapValues的SGX操作
 */
public class SGXFlatMapValuesFactory {
    private static final Logger logger = LoggerFactory.getLogger(SGXFlatMapValuesFactory.class);

    @SuppressWarnings("unchecked")
    public static <T> RDD<T> executeFlatMapValues(RDD<?> rdd, Object f) {
        try {
            logger.debug("Executing SGX FlatMapValues for RDD type: {}", rdd.getClass().getSimpleName());
            
            // 收集所有分区的数据
            List<Object> inputData = new ArrayList<>();
            for (int i = 0; i < rdd.getNumPartitions(); i++) {
                scala.collection.Iterator<?> partitionData = rdd.iterator(rdd.partitions()[i], null);
                while (partitionData.hasNext()) {
                    Object item = partitionData.next();
                    inputData.add(item.toString());
                }
            }
            
            // 准备操作数据
            String operationData = prepareFlatMapValuesOperationData(rdd, f);
            
            // 调用JNI执行SGX计算
            List<Object> result = SGXJNIWrapper.executeFlatMapValues(inputData, operationData);
            
            // 转换结果 - 这里简化处理，实际应该创建新的RDD
            // 在实际实现中，这里需要创建MapPartitionsRDD
            logger.debug("SGX FlatMapValues completed for RDD type: {}, result size: {}", 
                        rdd.getClass().getSimpleName(), result.size());
            
            // 简化实现：返回原始RDD（实际应该创建新的RDD）
            return (RDD<T>) rdd;
            
        } catch (Exception e) {
            logger.error("Failed to execute SGX FlatMapValues for RDD type: {}", rdd.getClass().getSimpleName(), e);
            throw new RuntimeException("SGX FlatMapValues execution failed", e);
        }
    }
    
    private static String prepareFlatMapValuesOperationData(RDD<?> rdd, Object f) {
        // 准备flatMapValues操作所需的数据
        return "flatmapvalues_operation_data:" + f.toString();
    }
}
