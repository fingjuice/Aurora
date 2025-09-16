package org.apache.spark.sgx;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;

/**
 * SGX Spark Agent - 主代理类
 * 
 * 只拦截已经在enclave中实现的RDD类型的compute方法
 */
public class SGXSparkAgent {
    private static final Logger logger = LoggerFactory.getLogger(SGXSparkAgent.class);
    
    // 只拦截已实现的RDD类型
    private static final String[] IMPLEMENTED_RDD_TYPES = {
        "org.apache.spark.rdd.MapPartitionsRDD",    // mapPartitions
        "org.apache.spark.rdd.FilteredRDD",         // filter
        "org.apache.spark.rdd.FlatMappedRDD",       // flatMap
        "org.apache.spark.rdd.UnionRDD",            // union
        "org.apache.spark.rdd.IntersectionRDD",     // intersection
        "org.apache.spark.rdd.DistinctRDD",         // distinct
        "org.apache.spark.rdd.GroupedRDD",          // groupByKey
        "org.apache.spark.rdd.ReducedRDD",          // reduceByKey
        "org.apache.spark.rdd.JoinedRDD"            // join
    };
    
    public static void premain(String agentArgs, Instrumentation inst) {
        logger.info("SGX Spark Agent starting (premain)...");
        installAgent(inst);
    }
    
    public static void agentmain(String agentArgs, Instrumentation inst) {
        logger.info("SGX Spark Agent attaching (agentmain)...");
        installAgent(inst);
    }
    
    private static void installAgent(Instrumentation inst) {
        try {
            // Initialize SGX JNI wrapper
            SGXJNIWrapper.initialize();
            
            // Install bytecode transformations for each implemented RDD type
            AgentBuilder agentBuilder = new AgentBuilder.Default();
            
            for (String rddClassName : IMPLEMENTED_RDD_TYPES) {
                agentBuilder = agentBuilder
                    .type(ElementMatchers.named(rddClassName))
                    .transform((builder, type, classLoader, module, protectionDomain) -> {
                        logger.debug("Transforming RDD class: {}", type.getName());
                        
                        return builder
                            // 只拦截compute方法
                            .method(ElementMatchers.named("compute")
                                .and(ElementMatchers.takesArguments(2))
                                .and(ElementMatchers.returns(ElementMatchers.named("java.util.Iterator"))))
                            .intercept(Advice.to(getAdviceClass(rddClassName)));
                    });
            }
            
            agentBuilder
                .with(AgentBuilder.Listener.StreamWriting.toSystemOut())
                .with(AgentBuilder.InstallationListener.StreamWriting.toSystemOut())
                .installOn(inst);
                
            logger.info("SGX Spark Agent installed successfully for {} RDD types", IMPLEMENTED_RDD_TYPES.length);
        } catch (Exception e) {
            logger.error("Failed to install SGX Spark Agent", e);
            throw new RuntimeException("Failed to install SGX Spark Agent", e);
        }
    }
    
    /**
     * 获取对应的Advice类
     */
    private static Class<?> getAdviceClass(String rddClassName) {
        switch (rddClassName) {
            case "org.apache.spark.rdd.MapPartitionsRDD":
                return SGXMapPartitionsRDDAdvice.class;
            case "org.apache.spark.rdd.FilteredRDD":
                return SGXFilteredRDDAdvice.class;
            case "org.apache.spark.rdd.FlatMappedRDD":
                return SGXFlatMappedRDDAdvice.class;
            case "org.apache.spark.rdd.UnionRDD":
                return SGXUnionRDDAdvice.class;
            case "org.apache.spark.rdd.IntersectionRDD":
                return SGXIntersectionRDDAdvice.class;
            case "org.apache.spark.rdd.DistinctRDD":
                return SGXDistinctRDDAdvice.class;
            case "org.apache.spark.rdd.GroupedRDD":
                return SGXGroupedRDDAdvice.class;
            case "org.apache.spark.rdd.ReducedRDD":
                return SGXReducedRDDAdvice.class;
            case "org.apache.spark.rdd.JoinedRDD":
                return SGXJoinedRDDAdvice.class;
            default:
                logger.warn("Unknown RDD class: {}", rddClassName);
                return null;
        }
    }
    
    public static void main(String[] args) {
        // For testing purposes
        ByteBuddyAgent.install();
        logger.info("SGX Spark Agent installed via main method");
    }
}