package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SGX Coalesce Advice - 拦截Coalesce操作的compute方法
 * 用于coalesce()操作
 */
public class SGXCoalesceAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXCoalesceAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) int numPartitions,
            @Advice.Argument(1) boolean shuffle,
            @Advice.Return(readOnly = false) RDD<?> result) {

        logger.debug("Intercepting RDD.coalesce() for RDD type: {}", rdd.getClass().getSimpleName());

        try {
            // 调用SGX工厂方法处理Coalesce
            result = SGXCoalesceFactory.executeCoalesce(rdd, numPartitions, shuffle);
            logger.debug("SGX Coalesce processing completed for RDD type: {}", rdd.getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("SGX Coalesce processing failed for RDD type: {}, using original implementation", rdd.getClass().getSimpleName(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
