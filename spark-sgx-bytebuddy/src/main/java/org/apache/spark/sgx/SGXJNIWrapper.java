package org.apache.spark.sgx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

public class SGXJNIWrapper {
    private static final Logger logger = LoggerFactory.getLogger(SGXJNIWrapper.class);
    private static boolean sgxAvailable = false;
    private static boolean initialized = false;
    
    static {
        try {
            System.loadLibrary("sgx_spark_jni");
            logger.info("SGX Spark JNI library loaded successfully");
        } catch (UnsatisfiedLinkError e) {
            logger.warn("Failed to load SGX Spark JNI library: {}", e.getMessage());
        }
    }
    
    public static void initialize() {
        if (initialized) return;
        try {
            sgxAvailable = nativeInitialize();
            initialized = true;
        } catch (Exception e) {
            logger.error("Failed to initialize SGX JNI", e);
            sgxAvailable = false;
            initialized = true;
        }
    }
    
    public static boolean isSGXAvailable() {
        return sgxAvailable;
    }
    
    // Public methods for each RDD type
    public static List<Object> executeMapPartitionsRDD(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteMapPartitionsRDD(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute MapPartitionsRDD in SGX", e);
            throw new RuntimeException("SGX MapPartitionsRDD execution failed", e);
        }
    }
    
    public static List<Object> executeFilteredRDD(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteFilteredRDD(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute FilteredRDD in SGX", e);
            throw new RuntimeException("SGX FilteredRDD execution failed", e);
        }
    }
    
    public static List<Object> executeFlatMappedRDD(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteFlatMappedRDD(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute FlatMappedRDD in SGX", e);
            throw new RuntimeException("SGX FlatMappedRDD execution failed", e);
        }
    }
    
    public static List<Object> executeUnionRDD(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteUnionRDD(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute UnionRDD in SGX", e);
            throw new RuntimeException("SGX UnionRDD execution failed", e);
        }
    }
    
    public static List<Object> executeIntersectionRDD(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteIntersectionRDD(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute IntersectionRDD in SGX", e);
            throw new RuntimeException("SGX IntersectionRDD execution failed", e);
        }
    }
    
    public static List<Object> executeDistinctRDD(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteDistinctRDD(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute DistinctRDD in SGX", e);
            throw new RuntimeException("SGX DistinctRDD execution failed", e);
        }
    }
    
    public static List<Object> executeGroupedRDD(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteGroupedRDD(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute GroupedRDD in SGX", e);
            throw new RuntimeException("SGX GroupedRDD execution failed", e);
        }
    }
    
    public static List<Object> executeReducedRDD(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteReducedRDD(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute ReducedRDD in SGX", e);
            throw new RuntimeException("SGX ReducedRDD execution failed", e);
        }
    }
    
    public static List<Object> executeJoinedRDD(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteJoinedRDD(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute JoinedRDD in SGX", e);
            throw new RuntimeException("SGX JoinedRDD execution failed", e);
        }
    }
    
    // Action methods
    public static List<Object> executeCollectAction(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteCollectAction(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute CollectAction in SGX", e);
            throw new RuntimeException("SGX CollectAction execution failed", e);
        }
    }
    
    public static List<Object> executeCountAction(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteCountAction(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute CountAction in SGX", e);
            throw new RuntimeException("SGX CountAction execution failed", e);
        }
    }
    
    public static List<Object> executeFirstAction(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteFirstAction(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute FirstAction in SGX", e);
            throw new RuntimeException("SGX FirstAction execution failed", e);
        }
    }
    
    public static List<Object> executeTakeAction(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteTakeAction(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute TakeAction in SGX", e);
            throw new RuntimeException("SGX TakeAction execution failed", e);
        }
    }
    
    public static List<Object> executeForeachAction(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteForeachAction(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute ForeachAction in SGX", e);
            throw new RuntimeException("SGX ForeachAction execution failed", e);
        }
    }
    
    public static List<Object> executeReduceAction(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteReduceAction(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute ReduceAction in SGX", e);
            throw new RuntimeException("SGX ReduceAction execution failed", e);
        }
    }
    
    // Transformation methods
    public static List<Object> executeMapValues(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteMapValues(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute MapValues in SGX", e);
            throw new RuntimeException("SGX MapValues execution failed", e);
        }
    }
    
    public static List<Object> executeFlatMapValues(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteFlatMapValues(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute FlatMapValues in SGX", e);
            throw new RuntimeException("SGX FlatMapValues execution failed", e);
        }
    }
    
    public static List<Object> executeSample(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteSample(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute Sample in SGX", e);
            throw new RuntimeException("SGX Sample execution failed", e);
        }
    }
    
    public static List<Object> executeSortBy(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteSortBy(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute SortBy in SGX", e);
            throw new RuntimeException("SGX SortBy execution failed", e);
        }
    }
    
    public static List<Object> executeCoalesce(List<Object> inputData, String operationData) {
        if (!sgxAvailable) {
            throw new RuntimeException("SGX not available");
        }
        try {
            return nativeExecuteCoalesce(inputData, operationData);
        } catch (Exception e) {
            logger.error("Failed to execute Coalesce in SGX", e);
            throw new RuntimeException("SGX Coalesce execution failed", e);
        }
    }
    
    // Native methods
    private static native boolean nativeInitialize();
    private static native List<Object> nativeExecuteMapPartitionsRDD(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteFilteredRDD(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteFlatMappedRDD(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteUnionRDD(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteIntersectionRDD(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteDistinctRDD(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteGroupedRDD(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteReducedRDD(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteJoinedRDD(List<Object> inputData, String operationData);
    
    // Action native methods
    private static native List<Object> nativeExecuteCollectAction(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteCountAction(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteFirstAction(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteTakeAction(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteForeachAction(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteReduceAction(List<Object> inputData, String operationData);
    
    // Transformation native methods
    private static native List<Object> nativeExecuteMapValues(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteFlatMapValues(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteSample(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteSortBy(List<Object> inputData, String operationData);
    private static native List<Object> nativeExecuteCoalesce(List<Object> inputData, String operationData);
}
