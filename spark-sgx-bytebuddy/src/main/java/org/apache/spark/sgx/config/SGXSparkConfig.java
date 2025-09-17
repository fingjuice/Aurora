package org.apache.spark.sgx.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Simple configuration manager for SGX Spark Agent
 * Only handles RDD types, Actions, and Transformations
 */
public class SGXSparkConfig {
    private static final Logger logger = LoggerFactory.getLogger(SGXSparkConfig.class);
    private static final String CONFIG_FILE = "sgx-spark-config.properties";
    
    private static volatile SGXSparkConfig instance;
    private final Properties properties;
    
    private SGXSparkConfig() {
        this.properties = new Properties();
        loadConfiguration();
    }
    
    public static SGXSparkConfig getInstance() {
        if (instance == null) {
            synchronized (SGXSparkConfig.class) {
                if (instance == null) {
                    instance = new SGXSparkConfig();
                }
            }
        }
        return instance;
    }
    
    private void loadConfiguration() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                logger.warn("Configuration file {} not found, using default values", CONFIG_FILE);
                loadDefaultConfiguration();
                return;
            }
            
            properties.load(input);
            logger.info("Configuration loaded successfully from {}", CONFIG_FILE);
            
        } catch (IOException e) {
            logger.error("Error loading configuration file: {}", e.getMessage());
            loadDefaultConfiguration();
        }
    }
    
    private void loadDefaultConfiguration() {
        // Default RDD types
        properties.setProperty("rdd.types", 
            "org.apache.spark.rdd.MapPartitionsRDD,org.apache.spark.rdd.FilteredRDD,org.apache.spark.rdd.FlatMappedRDD," +
            "org.apache.spark.rdd.UnionRDD,org.apache.spark.rdd.IntersectionRDD,org.apache.spark.rdd.DistinctRDD," +
            "org.apache.spark.rdd.GroupedRDD,org.apache.spark.rdd.ReducedRDD,org.apache.spark.rdd.JoinedRDD");
        
        // Default Actions
        properties.setProperty("actions", "collect,count,reduce,first,take,foreach");
        
        // Default Transformations
        properties.setProperty("transformations", "mapValues,flatMapValues,sample,sortBy,coalesce");
        
        logger.info("Default configuration loaded");
    }
    
    // =============================================================================
    // RDD Types Configuration
    // =============================================================================
    
    public List<String> getRDDTypes() {
        return getList("rdd.types");
    }
    
    // =============================================================================
    // Actions Configuration
    // =============================================================================
    
    public List<String> getActions() {
        return getList("actions");
    }
    
    // =============================================================================
    // Transformations Configuration
    // =============================================================================
    
    public List<String> getTransformations() {
        return getList("transformations");
    }
    
    // =============================================================================
    // Utility Methods
    // =============================================================================
    
    private List<String> getList(String key) {
        String value = properties.getProperty(key, "");
        if (value.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.asList(value.split(","));
    }
    
    public void reload() {
        loadConfiguration();
        logger.info("Configuration reloaded");
    }
    
    public void printConfiguration() {
        logger.info("=== SGX Spark Agent Configuration ===");
        logger.info("RDD Types: {}", getRDDTypes());
        logger.info("Actions: {}", getActions());
        logger.info("Transformations: {}", getTransformations());
        logger.info("=====================================");
    }
}
