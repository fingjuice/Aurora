package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;

public class SGXUnionRDDAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXUnionRDDAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) Partition split,
            @Advice.Argument(1) TaskContext context,
            @Advice.Return(readOnly = false) Iterator<?> result) {

        logger.debug("Intercepting UnionRDD compute for partition: {}", split.index());

        if (SGXJNIWrapper.isSGXAvailable()) {
            try {
                logger.debug("Redirecting UnionRDD to SGX");
                result = SGXUnionRDDFactory.createSGXUnionRDD(rdd, split, context);
                return;
            } catch (Exception e) {
                logger.warn("Failed to redirect UnionRDD to SGX, using original implementation", e);
            }
        }
        
        logger.debug("Using original UnionRDD implementation");
    }

    @Advice.OnMethodExit
    public static void onExit(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) Partition split,
            @Advice.Argument(1) TaskContext context,
            @Advice.Return Iterator<?> result) {

        logger.debug("UnionRDD compute completed for partition: {}", split.index());
    }
}
