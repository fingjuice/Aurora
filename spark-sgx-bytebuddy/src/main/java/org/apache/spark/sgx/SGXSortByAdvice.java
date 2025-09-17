package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SGX SortBy Advice - 拦截SortBy操作的compute方法
 * 用于sortBy()操作
 */
public class SGXSortByAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXSortByAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) Object f,
            @Advice.Argument(1) boolean ascending,
            @Advice.Argument(2) int numPartitions,
            @Advice.Return(readOnly = false) RDD<?> result) {

        logger.debug("Intercepting RDD.sortBy() for RDD type: {}", rdd.getClass().getSimpleName());

        try {
            // 调用SGX工厂方法处理SortBy
            result = SGXSortByFactory.executeSortBy(rdd, f, ascending, numPartitions);
            logger.debug("SGX SortBy processing completed for RDD type: {}", rdd.getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("SGX SortBy processing failed for RDD type: {}, using original implementation", rdd.getClass().getSimpleName(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
