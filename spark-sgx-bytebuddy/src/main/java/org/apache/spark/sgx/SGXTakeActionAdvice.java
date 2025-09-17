package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * SGX Take Action Advice - 拦截Take操作的compute方法
 * 用于take()操作
 */
public class SGXTakeActionAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXTakeActionAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) int num,
            @Advice.Return(readOnly = false) List<?> result) {

        logger.debug("Intercepting RDD.take({}) for RDD type: {}", num, rdd.getClass().getSimpleName());

        try {
            // 调用SGX工厂方法处理Take Action
            result = SGXTakeActionFactory.executeTakeAction(rdd, num);
            logger.debug("SGX Take Action processing completed for RDD type: {}", rdd.getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("SGX Take Action processing failed for RDD type: {}, using original implementation", rdd.getClass().getSimpleName(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
