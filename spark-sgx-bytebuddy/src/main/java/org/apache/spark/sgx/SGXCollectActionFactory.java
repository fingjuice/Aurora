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
 * SGX Collect Action Factory - 处理Collect Action的SGX操作
 */
public class SGXCollectActionFactory {
    private static final Logger logger = LoggerFactory.getLogger(SGXCollectActionFactory.class);

    @SuppressWarnings("unchecked")

    public static <T> List<T> executeCollectAction(RDD<?> rdd) {
        try {
            logger.debug("Executing SGX Collect Action for RDD type: {}", rdd.getClass().getSimpleName());
            
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
            String operationData = prepareCollectOperationData(rdd);
            
            // 调用JNI执行SGX计算
            List<Object> result = SGXJNIWrapper.executeCollectAction(inputData, operationData);
            
            // 转换结果
            List<T> convertedResult = new ArrayList<>();
            for (Object item : result) {
                convertedResult.add((T) item);
            }
            
            logger.debug("SGX Collect Action completed for RDD type: {}, result size: {}", 
                        rdd.getClass().getSimpleName(), convertedResult.size());
            return convertedResult;
            
        } catch (Exception e) {
            logger.error("Failed to execute SGX Collect Action for RDD type: {}", rdd.getClass().getSimpleName(), e);
            throw new RuntimeException("SGX Collect Action execution failed", e);
        }
    }
    
    private static String prepareCollectOperationData(RDD<?> rdd) {
        // 准备collect操作所需的数据
        return "collect_operation_data";
    }
}
