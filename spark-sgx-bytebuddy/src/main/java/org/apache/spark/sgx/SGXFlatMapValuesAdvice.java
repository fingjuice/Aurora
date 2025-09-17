package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SGX FlatMapValues Advice - 拦截FlatMapValues操作的compute方法
 * 用于flatMapValues()操作
 */
public class SGXFlatMapValuesAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXFlatMapValuesAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) Object f,
            @Advice.Return(readOnly = false) RDD<?> result) {

        logger.debug("Intercepting RDD.flatMapValues() for RDD type: {}", rdd.getClass().getSimpleName());

        try {
            // 调用SGX工厂方法处理FlatMapValues
            result = SGXFlatMapValuesFactory.executeFlatMapValues(rdd, f);
            logger.debug("SGX FlatMapValues processing completed for RDD type: {}", rdd.getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("SGX FlatMapValues processing failed for RDD type: {}, using original implementation", rdd.getClass().getSimpleName(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
