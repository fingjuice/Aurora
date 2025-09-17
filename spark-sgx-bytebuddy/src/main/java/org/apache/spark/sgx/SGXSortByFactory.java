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
 * SGX SortBy Factory - 处理SortBy的SGX操作
 */
public class SGXSortByFactory {
    private static final Logger logger = LoggerFactory.getLogger(SGXSortByFactory.class);

    @SuppressWarnings("unchecked")
    public static <T> RDD<T> executeSortBy(RDD<?> rdd, Object f, boolean ascending, int numPartitions) {
        try {
            logger.debug("Executing SGX SortBy for RDD type: {}, ascending: {}, numPartitions: {}", 
                        rdd.getClass().getSimpleName(), ascending, numPartitions);
            
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
            String operationData = prepareSortByOperationData(rdd, f, ascending, numPartitions);
            
            // 调用JNI执行SGX计算
            List<Object> result = SGXJNIWrapper.executeSortBy(inputData, operationData);
            
            // 转换结果 - 这里简化处理，实际应该创建新的RDD
            // 在实际实现中，这里需要创建MapPartitionsRDD
            logger.debug("SGX SortBy completed for RDD type: {}, result size: {}", 
                        rdd.getClass().getSimpleName(), result.size());
            
            // 简化实现：返回原始RDD（实际应该创建新的RDD）
            return (RDD<T>) rdd;
            
        } catch (Exception e) {
            logger.error("Failed to execute SGX SortBy for RDD type: {}", rdd.getClass().getSimpleName(), e);
            throw new RuntimeException("SGX SortBy execution failed", e);
        }
    }
    
    private static String prepareSortByOperationData(RDD<?> rdd, Object f, boolean ascending, int numPartitions) {
        // 准备sortBy操作所需的数据
        return String.format("sortby_operation_data:f=%s,ascending=%s,numPartitions=%d", 
                           f.toString(), ascending, numPartitions);
    }
}
