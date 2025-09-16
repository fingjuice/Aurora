package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;

/**
 * SGX First Action Advice - 拦截First操作的compute方法
 * 用于first()操作
 */
public class SGXFirstActionAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXFirstActionAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) Partition split,
            @Advice.Argument(1) TaskContext context,
            @Advice.Return(readOnly = false) Iterator<?> result) {

        logger.debug("Intercepting First Action compute for partition: {}", split.index());

        try {
            // 调用SGX工厂方法处理First Action
            result = SGXFirstActionFactory.createSGXFirstAction(rdd, split, context);
            logger.debug("SGX First Action processing completed for partition: {}", split.index());
        } catch (Exception e) {
            logger.error("SGX First Action processing failed for partition: {}, using original implementation", split.index(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
