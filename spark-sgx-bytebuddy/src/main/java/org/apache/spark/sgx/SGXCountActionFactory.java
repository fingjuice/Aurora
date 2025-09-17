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
 * SGX Count Action Factory - 处理Count Action的SGX操作
 */
public class SGXCountActionFactory {
    private static final Logger logger = LoggerFactory.getLogger(SGXCountActionFactory.class);

    public static Long executeCountAction(RDD<?> rdd) {
        try {
            logger.debug("Executing SGX Count Action for RDD type: {}", rdd.getClass().getSimpleName());
            
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
            String operationData = prepareCountOperationData(rdd);
            
            // 调用JNI执行SGX计算
            List<Object> result = SGXJNIWrapper.executeCountAction(inputData, operationData);
            
            // 转换结果 - count返回Long类型
            if (!result.isEmpty() && result.get(0) instanceof String) {
                return Long.parseLong((String) result.get(0));
            }
            
            logger.debug("SGX Count Action completed for RDD type: {}, count: {}", 
                        rdd.getClass().getSimpleName(), inputData.size());
            return (long) inputData.size();
            
        } catch (Exception e) {
            logger.error("Failed to execute SGX Count Action for RDD type: {}", rdd.getClass().getSimpleName(), e);
            throw new RuntimeException("SGX Count Action execution failed", e);
        }
    }
    
    private static String prepareCountOperationData(RDD<?> rdd) {
        // 准备count操作所需的数据
        return "count_operation_data";
    }
}
