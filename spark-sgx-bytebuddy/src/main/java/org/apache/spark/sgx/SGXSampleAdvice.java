package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SGX Sample Advice - 拦截Sample操作的compute方法
 * 用于sample()操作
 */
public class SGXSampleAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXSampleAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) boolean withReplacement,
            @Advice.Argument(1) double fraction,
            @Advice.Argument(2) long seed,
            @Advice.Return(readOnly = false) RDD<?> result) {

        logger.debug("Intercepting RDD.sample() for RDD type: {}", rdd.getClass().getSimpleName());

        try {
            // 调用SGX工厂方法处理Sample
            result = SGXSampleFactory.executeSample(rdd, withReplacement, fraction, seed);
            logger.debug("SGX Sample processing completed for RDD type: {}", rdd.getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("SGX Sample processing failed for RDD type: {}, using original implementation", rdd.getClass().getSimpleName(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
