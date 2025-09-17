package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SGX Reduce Action Advice - 拦截Reduce操作的compute方法
 * 用于reduce()操作
 */
public class SGXReduceActionAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXReduceActionAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) Object f,
            @Advice.Return(readOnly = false) Object result) {

        logger.debug("Intercepting RDD.reduce() for RDD type: {}", rdd.getClass().getSimpleName());

        try {
            // 调用SGX工厂方法处理Reduce Action
            result = SGXReduceActionFactory.executeReduceAction(rdd, f);
            logger.debug("SGX Reduce Action processing completed for RDD type: {}", rdd.getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("SGX Reduce Action processing failed for RDD type: {}, using original implementation", rdd.getClass().getSimpleName(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
