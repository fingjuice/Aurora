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
 * SGX Foreach Action Factory - 处理Foreach Action的SGX操作
 */
public class SGXForeachActionFactory {
    private static final Logger logger = LoggerFactory.getLogger(SGXForeachActionFactory.class);

    public static void executeForeachAction(RDD<?> rdd, Object f) {
        try {
            logger.debug("Executing SGX Foreach Action for RDD type: {}", rdd.getClass().getSimpleName());
            
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
            String operationData = prepareForeachOperationData(rdd, f);
            
            // 调用JNI执行SGX计算
            SGXJNIWrapper.executeForeachAction(inputData, operationData);
            
            logger.debug("SGX Foreach Action completed for RDD type: {}, processed {} elements", 
                        rdd.getClass().getSimpleName(), inputData.size());
            
        } catch (Exception e) {
            logger.error("Failed to execute SGX Foreach Action for RDD type: {}", rdd.getClass().getSimpleName(), e);
            throw new RuntimeException("SGX Foreach Action execution failed", e);
        }
    }
    
    private static String prepareForeachOperationData(RDD<?> rdd, Object f) {
        // 准备foreach操作所需的数据
        return "foreach_operation_data:" + f.toString();
    }
}
