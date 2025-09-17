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
 * SGX First Action Factory - 处理First Action的SGX操作
 */
public class SGXFirstActionFactory {
    private static final Logger logger = LoggerFactory.getLogger(SGXFirstActionFactory.class);

    public static Object executeFirstAction(RDD<?> rdd) {
        try {
            logger.debug("Executing SGX First Action for RDD type: {}", rdd.getClass().getSimpleName());
            
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
            String operationData = prepareFirstOperationData(rdd);
            
            // 调用JNI执行SGX计算
            List<Object> result = SGXJNIWrapper.executeFirstAction(inputData, operationData);
            
            // 转换结果 - first返回第一个元素
            if (!result.isEmpty()) {
                logger.debug("SGX First Action completed for RDD type: {}, first element: {}", 
                            rdd.getClass().getSimpleName(), result.get(0));
                return result.get(0);
            }
            
            logger.debug("SGX First Action completed for RDD type: {}, no elements found", 
                        rdd.getClass().getSimpleName());
            return null;
            
        } catch (Exception e) {
            logger.error("Failed to execute SGX First Action for RDD type: {}", rdd.getClass().getSimpleName(), e);
            throw new RuntimeException("SGX First Action execution failed", e);
        }
    }
    
    private static String prepareFirstOperationData(RDD<?> rdd) {
        // 准备first操作所需的数据
        return "first_operation_data";
    }
}
