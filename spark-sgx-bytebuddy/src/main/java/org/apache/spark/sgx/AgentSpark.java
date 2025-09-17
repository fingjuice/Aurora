package org.apache.spark.sgx;

import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.spark.sgx.config.SGXSparkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;
import java.util.List;

/**
 * AgentSpark - Main agent class for SGX-enabled Spark operations
 * 
 * This agent intercepts RDD compute methods, Actions, and Transformations
 * and redirects them to SGX enclaves for secure computation.
 * 
 * Configuration is loaded from sgx-spark-config.properties file.
 */
public class AgentSpark {
    private static final Logger logger = LoggerFactory.getLogger(AgentSpark.class);
    private static final SGXSparkConfig config = SGXSparkConfig.getInstance();
    
    public static void premain(String agentArgs, Instrumentation inst) {
        logger.info("AgentSpark starting (premain)...");
        installAgent(inst);
    }
    
    public static void agentmain(String agentArgs, Instrumentation inst) {
        logger.info("AgentSpark attaching (agentmain)...");
        installAgent(inst);
    }
    
    private static void installAgent(Instrumentation inst) {
        try {
            // Print configuration
            config.printConfiguration();
            
            // Initialize SGX JNI wrapper
            SGXJNIWrapper.initialize();
            
            // Install bytecode transformations
            AgentBuilder agentBuilder = new AgentBuilder.Default();
            
            // 1. Intercept RDD types compute methods
            List<String> rddTypes = config.getRDDTypes();
            for (String rddClassName : rddTypes) {
                agentBuilder = agentBuilder
                    .type(ElementMatchers.named(rddClassName))
                    .transform((builder, type, classLoader, module, protectionDomain) -> {
                        logger.debug("Transforming RDD class: {}", type.getName());
                        
                        return builder
                            // Only intercept compute method
                            .method(ElementMatchers.named("compute")
                                .and(ElementMatchers.takesArguments(2))
                                .and(ElementMatchers.returns(ElementMatchers.named("java.util.Iterator"))))
                            .intercept(Advice.to(getAdviceClass(rddClassName)));
                    });
            }
            
            // 2. Intercept RDD Actions methods
            List<String> actions = config.getActions();
            agentBuilder = agentBuilder
                .type(ElementMatchers.isSubTypeOf(org.apache.spark.rdd.RDD.class))
                .transform((builder, type, classLoader, module, protectionDomain) -> {
                    logger.debug("Transforming RDD Actions for class: {}", type.getName());
                    
                    for (String action : actions) {
                        builder = builder
                            .method(ElementMatchers.named(action)
                                .and(ElementMatchers.isPublic())
                                .and(ElementMatchers.not(ElementMatchers.isStatic())))
                            .intercept(Advice.to(getActionAdviceClass(action)));
                    }
                    return builder;
                });
            
            // 3. Intercept RDD Transformations methods
            List<String> transformations = config.getTransformations();
            agentBuilder = agentBuilder
                .type(ElementMatchers.isSubTypeOf(org.apache.spark.rdd.RDD.class))
                .transform((builder, type, classLoader, module, protectionDomain) -> {
                    logger.debug("Transforming RDD Transformations for class: {}", type.getName());
                    
                    for (String transformation : transformations) {
                        builder = builder
                            .method(ElementMatchers.named(transformation)
                                .and(ElementMatchers.isPublic())
                                .and(ElementMatchers.not(ElementMatchers.isStatic())))
                            .intercept(Advice.to(getTransformationAdviceClass(transformation)));
                    }
                    return builder;
                });
            
            agentBuilder.installOn(inst);
                
            logger.info("AgentSpark installed successfully for {} RDD types, {} Actions, {} Transformations", 
                       rddTypes.size(), actions.size(), transformations.size());
                       
        } catch (Exception e) {
            logger.error("Failed to install AgentSpark", e);
            throw new RuntimeException("Failed to install AgentSpark", e);
        }
    }
    
    /**
     * Get corresponding Advice class for RDD type
     */
    private static Class<?> getAdviceClass(String rddClassName) {
        String simpleClassName = rddClassName.substring(rddClassName.lastIndexOf('.') + 1);
        
        switch (simpleClassName) {
            case "MapPartitionsRDD":
                return SGXMapPartitionsRDDAdvice.class;
            case "FilteredRDD":
                return SGXFilteredRDDAdvice.class;
            case "FlatMappedRDD":
                return SGXFlatMappedRDDAdvice.class;
            case "UnionRDD":
                return SGXUnionRDDAdvice.class;
            case "IntersectionRDD":
                return SGXIntersectionRDDAdvice.class;
            case "DistinctRDD":
                return SGXDistinctRDDAdvice.class;
            case "GroupedRDD":
                return SGXGroupedRDDAdvice.class;
            case "ReducedRDD":
                return SGXReducedRDDAdvice.class;
            case "JoinedRDD":
                return SGXJoinedRDDAdvice.class;
            default:
                logger.warn("Unknown RDD class: {} ({})", rddClassName, simpleClassName);
                return null;
        }
    }
    
    /**
     * Get corresponding Action Advice class
     */
    private static Class<?> getActionAdviceClass(String action) {
        switch (action) {
            case "collect":
                return SGXCollectActionAdvice.class;
            case "count":
                return SGXCountActionAdvice.class;
            case "first":
                return SGXFirstActionAdvice.class;
            case "take":
                return SGXTakeActionAdvice.class;
            case "foreach":
                return SGXForeachActionAdvice.class;
            case "reduce":
                return SGXReduceActionAdvice.class;
            default:
                logger.warn("Unknown Action: {}", action);
                return null;
        }
    }
    
    /**
     * Get corresponding Transformation Advice class
     */
    private static Class<?> getTransformationAdviceClass(String transformation) {
        switch (transformation) {
            case "mapValues":
                return SGXMapValuesAdvice.class;
            case "flatMapValues":
                return SGXFlatMapValuesAdvice.class;
            case "sample":
                return SGXSampleAdvice.class;
            case "sortBy":
                return SGXSortByAdvice.class;
            case "coalesce":
                return SGXCoalesceAdvice.class;
            default:
                logger.warn("Unknown Transformation: {}", transformation);
                return null;
        }
    }
    
    /**
     * Reload configuration at runtime
     */
    public static void reloadConfiguration() {
        config.reload();
        logger.info("AgentSpark configuration reloaded");
    }
    
    /**
     * Get current configuration
     */
    public static SGXSparkConfig getConfiguration() {
        return config;
    }
    
    /**
     * Get agent statistics
     */
    public static String getAgentStatistics() {
        return String.format("AgentSpark Statistics:\n" +
                           "  RDD Types: %d\n" +
                           "  Actions: %d\n" +
                           "  Transformations: %d",
                           config.getRDDTypes().size(),
                           config.getActions().size(),
                           config.getTransformations().size());
    }
    
    public static void main(String[] args) {
        // For testing purposes
        ByteBuddyAgent.install();
        logger.info("AgentSpark installed via main method");
        logger.info(getAgentStatistics());
    }
}
