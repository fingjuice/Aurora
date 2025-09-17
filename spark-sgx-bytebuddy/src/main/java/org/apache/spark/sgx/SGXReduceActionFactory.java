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
 * SGX Reduce Action Factory - 处理Reduce Action的SGX操作
 */
public class SGXReduceActionFactory {
    private static final Logger logger = LoggerFactory.getLogger(SGXReduceActionFactory.class);

    public static Object executeReduceAction(RDD<?> rdd, Object f) {
        try {
            logger.debug("Executing SGX Reduce Action for RDD type: {}", rdd.getClass().getSimpleName());
            
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
            String operationData = prepareReduceOperationData(rdd, f);
            
            // 调用JNI执行SGX计算
            List<Object> result = SGXJNIWrapper.executeReduceAction(inputData, operationData);
            
            // 转换结果 - reduce返回单个值
            if (!result.isEmpty()) {
                logger.debug("SGX Reduce Action completed for RDD type: {}, reduced value: {}", 
                            rdd.getClass().getSimpleName(), result.get(0));
                return result.get(0);
            }
            
            logger.debug("SGX Reduce Action completed for RDD type: {}, no elements to reduce", 
                        rdd.getClass().getSimpleName());
            return null;
            
        } catch (Exception e) {
            logger.error("Failed to execute SGX Reduce Action for RDD type: {}", rdd.getClass().getSimpleName(), e);
            throw new RuntimeException("SGX Reduce Action execution failed", e);
        }
    }
    
    private static String prepareReduceOperationData(RDD<?> rdd, Object f) {
        // 准备reduce操作所需的数据
        return "reduce_operation_data:" + f.toString();
    }
}
