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
 * SGX MapValues Factory - 处理MapValues的SGX操作
 */
public class SGXMapValuesFactory {
    private static final Logger logger = LoggerFactory.getLogger(SGXMapValuesFactory.class);

    @SuppressWarnings("unchecked")
    public static <T> Iterator<T> createSGXMapValues(RDD<?> rdd, Partition split, TaskContext context) {
        try {
            logger.debug("Creating SGX MapValues for partition: {}", split.index());
            
            // 收集当前分区的数据
            scala.collection.Iterator<?> currentPartitionData = rdd.iterator(split, context);
            List<Object> inputData = new ArrayList<>();
            while (currentPartitionData.hasNext()) {
                Object item = currentPartitionData.next();
                inputData.add(item.toString());
            }
            
            // 准备操作数据
            String operationData = prepareMapValuesOperationData(rdd);
            
            // 调用JNI执行SGX计算
            List<Object> result = SGXJNIWrapper.executeMapValues(inputData, operationData);
            
            // 转换结果
            List<T> convertedResult = new ArrayList<>();
            for (Object item : result) {
                convertedResult.add((T) item);
            }
            
            logger.debug("SGX MapValues completed for partition: {}, result size: {}", 
                        split.index(), convertedResult.size());
            return convertedResult.iterator();
            
        } catch (Exception e) {
            logger.error("Failed to create SGX MapValues for partition: {}", split.index(), e);
            throw new RuntimeException("SGX MapValues creation failed", e);
        }
    }
    
    private static String prepareMapValuesOperationData(RDD<?> rdd) {
        // 准备mapValues操作所需的数据
        return "mapvalues_operation_data";
    }
}
