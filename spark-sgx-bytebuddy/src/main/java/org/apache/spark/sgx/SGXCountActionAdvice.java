package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SGX Count Action Advice - 拦截RDD.count()方法
 * 用于count()操作
 */
public class SGXCountActionAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXCountActionAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Return(readOnly = false) Long result) {

        logger.debug("Intercepting RDD.count() for RDD type: {}", rdd.getClass().getSimpleName());

        try {
            // 调用SGX工厂方法处理Count Action
            result = SGXCountActionFactory.executeCountAction(rdd);
            logger.debug("SGX Count Action processing completed for RDD type: {}", rdd.getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("SGX Count Action processing failed for RDD type: {}, using original implementation", rdd.getClass().getSimpleName(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
