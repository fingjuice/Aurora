package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SGX MapValues Advice - 拦截MapValues操作的compute方法
 * 用于mapValues()操作
 */
public class SGXMapValuesAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXMapValuesAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) Object f,
            @Advice.Return(readOnly = false) RDD<?> result) {

        logger.debug("Intercepting RDD.mapValues() for RDD type: {}", rdd.getClass().getSimpleName());

        try {
            // 调用SGX工厂方法处理MapValues
            result = SGXMapValuesFactory.executeMapValues(rdd, f);
            logger.debug("SGX MapValues processing completed for RDD type: {}", rdd.getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("SGX MapValues processing failed for RDD type: {}, using original implementation", rdd.getClass().getSimpleName(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
