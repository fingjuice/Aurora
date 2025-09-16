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

    @SuppressWarnings("unchecked")
    public static <T> Iterator<T> createSGXCountAction(RDD<?> rdd, Partition split, TaskContext context) {
        try {
            logger.debug("Creating SGX Count Action for partition: {}", split.index());
            
            // 收集当前分区的数据
            scala.collection.Iterator<?> currentPartitionData = rdd.iterator(split, context);
            List<Object> inputData = new ArrayList<>();
            while (currentPartitionData.hasNext()) {
                Object item = currentPartitionData.next();
                inputData.add(item.toString());
            }
            
            // 准备操作数据
            String operationData = prepareCountOperationData(rdd);
            
            // 调用JNI执行SGX计算
            List<Object> result = SGXJNIWrapper.executeCountAction(inputData, operationData);
            
            // 转换结果
            List<T> convertedResult = new ArrayList<>();
            for (Object item : result) {
                convertedResult.add((T) item);
            }
            
            logger.debug("SGX Count Action completed for partition: {}, result size: {}", 
                        split.index(), convertedResult.size());
            return convertedResult.iterator();
            
        } catch (Exception e) {
            logger.error("Failed to create SGX Count Action for partition: {}", split.index(), e);
            throw new RuntimeException("SGX Count Action creation failed", e);
        }
    }
    
    private static String prepareCountOperationData(RDD<?> rdd) {
        // 准备count操作所需的数据
        return "count_operation_data";
    }
}
