package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * SGX Collect Action Advice - 拦截RDD.collect()方法
 * 用于collect()操作
 */
public class SGXCollectActionAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXCollectActionAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Return(readOnly = false) List<?> result) {

        logger.debug("Intercepting RDD.collect() for RDD type: {}", rdd.getClass().getSimpleName());

        try {
            // 调用SGX工厂方法处理Collect Action
            result = SGXCollectActionFactory.executeCollectAction(rdd);
            logger.debug("SGX Collect Action processing completed for RDD type: {}", rdd.getClass().getSimpleName());
        } catch (Exception e) {
            logger.error("SGX Collect Action processing failed for RDD type: {}, using original implementation", rdd.getClass().getSimpleName(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
