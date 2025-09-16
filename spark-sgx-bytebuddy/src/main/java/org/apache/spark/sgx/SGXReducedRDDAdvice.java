package org.apache.spark.sgx;

import net.bytebuddy.asm.Advice;
import org.apache.spark.Partition;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;

public class SGXReducedRDDAdvice {
    private static final Logger logger = LoggerFactory.getLogger(SGXReducedRDDAdvice.class);

    @Advice.OnMethodEnter
    public static void onEnter(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) Partition split,
            @Advice.Argument(1) TaskContext context,
            @Advice.Return(readOnly = false) Iterator<?> result) {

        logger.debug("Intercepting ReducedRDD compute for partition: {}", split.index());

        if (SGXJNIWrapper.isSGXAvailable()) {
            try {
                logger.debug("Redirecting ReducedRDD to SGX");
                result = SGXReducedRDDFactory.createSGXReducedRDD(rdd, split, context);
                return;
            } catch (Exception e) {
                logger.warn("Failed to redirect ReducedRDD to SGX, using original implementation", e);
            }
        }
        
        logger.debug("Using original ReducedRDD implementation");
    }

    @Advice.OnMethodExit
    public static void onExit(
            @Advice.This RDD<?> rdd,
            @Advice.Argument(0) Partition split,
            @Advice.Argument(1) TaskContext context,
            @Advice.Return Iterator<?> result) {

        logger.debug("ReducedRDD compute completed for partition: {}", split.index());
    }
}
