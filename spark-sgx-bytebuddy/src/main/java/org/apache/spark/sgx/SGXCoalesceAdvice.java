package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;

/**
 * SGX Coalesce Advice - 拦截Coalesce操作的compute方法
 * 用于coalesce()操作
 */
public class SGXCoalesceAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXCoalesceAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) Partition split,
            @Advice.Argument(1) TaskContext context,
            @Advice.Return(readOnly = false) Iterator<?> result) {

        logger.debug("Intercepting Coalesce compute for partition: {}", split.index());

        try {
            // 调用SGX工厂方法处理Coalesce
            result = SGXCoalesceFactory.createSGXCoalesce(rdd, split, context);
            logger.debug("SGX Coalesce processing completed for partition: {}", split.index());
        } catch (Exception e) {
            logger.error("SGX Coalesce processing failed for partition: {}, using original implementation", split.index(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
