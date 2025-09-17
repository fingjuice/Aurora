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
 * SGX Take Action Factory - 处理Take Action的SGX操作
 */
public class SGXTakeActionFactory {
    private static final Logger logger = LoggerFactory.getLogger(SGXTakeActionFactory.class);

    @SuppressWarnings("unchecked")
    public static <T> List<T> executeTakeAction(RDD<?> rdd, int num) {
        try {
            logger.debug("Executing SGX Take Action for RDD type: {}, taking {} elements", 
                        rdd.getClass().getSimpleName(), num);
            
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
            String operationData = prepareTakeOperationData(rdd, num);
            
            // 调用JNI执行SGX计算
            List<Object> result = SGXJNIWrapper.executeTakeAction(inputData, operationData);
            
            // 转换结果
            List<T> convertedResult = new ArrayList<>();
            for (Object item : result) {
                convertedResult.add((T) item);
            }
            
            logger.debug("SGX Take Action completed for RDD type: {}, took {} elements", 
                        rdd.getClass().getSimpleName(), convertedResult.size());
            return convertedResult;
            
        } catch (Exception e) {
            logger.error("Failed to execute SGX Take Action for RDD type: {}", rdd.getClass().getSimpleName(), e);
            throw new RuntimeException("SGX Take Action execution failed", e);
        }
    }
    
    private static String prepareTakeOperationData(RDD<?> rdd, int num) {
        // 准备take操作所需的数据
        return "take_operation_data:" + num;
    }
}
