package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;

/**
 * SGX Foreach Action Advice - 拦截Foreach操作的compute方法
 * 用于foreach()操作
 */
public class SGXForeachActionAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXForeachActionAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) Partition split,
            @Advice.Argument(1) TaskContext context,
            @Advice.Return(readOnly = false) Iterator<?> result) {

        logger.debug("Intercepting Foreach Action compute for partition: {}", split.index());

        try {
            // 调用SGX工厂方法处理Foreach Action
            result = SGXForeachActionFactory.createSGXForeachAction(rdd, split, context);
            logger.debug("SGX Foreach Action processing completed for partition: {}", split.index());
        } catch (Exception e) {
            logger.error("SGX Foreach Action processing failed for partition: {}, using original implementation", split.index(), e);
            // 如果SGX处理失败，让原始方法继续执行
        }
    }
}
