package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SGX First Action Advice - 拦截RDD.first()方法
 * 用于first()操作
 */
public class SGXFirstActionAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXFirstActionAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Return(readOnly = false) Object result) {

        logger.debug("Intercepting RDD.first() for RDD type: {}", rdd.getClass().getSimpleName());

        try {
            // 调用SGX工厂方法处理First Action
            result = SGXFirstActionFactory.executeFirstAction(rdd);
            logger.debug("SGX First Action processing completed for RDD type: {}", rdd.getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("SGX First Action processing failed for RDD type: {}, using original implementation", rdd.getClass().getSimpleName(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
