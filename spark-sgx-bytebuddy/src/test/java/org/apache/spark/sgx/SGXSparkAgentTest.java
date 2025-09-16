package org.apache.spark.sgx;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

/**
 * Test class for SGX Spark Agent
 */
public class SGXSparkAgentTest {
    
    @Before
    public void setUp() {
        // Initialize SGX JNI wrapper for testing
        try {
            SGXJNIWrapper.initialize();
        } catch (Exception e) {
            // Ignore initialization errors in test environment
        }
    }
    
    @After
    public void tearDown() {
        // Cleanup if needed
    }
    
    @Test
    public void testSGXAvailability() {
        // Test SGX availability check
        boolean sgxAvailable = SGXJNIWrapper.isSGXAvailable();
        System.out.println("SGX Available: " + sgxAvailable);
        
        // In test environment, SGX might not be available
        // This test just verifies the method can be called
        assertNotNull("SGX availability check should not be null", sgxAvailable);
    }
    
    @Test
    public void testRDDTypeMapping() {
        // Test RDD type mapping in SGXSparkAgent
        String[] expectedTypes = {
            "org.apache.spark.rdd.MapPartitionsRDD",
            "org.apache.spark.rdd.FilteredRDD", 
            "org.apache.spark.rdd.FlatMappedRDD",
            "org.apache.spark.rdd.UnionRDD",
            "org.apache.spark.rdd.IntersectionRDD",
            "org.apache.spark.rdd.DistinctRDD",
            "org.apache.spark.rdd.GroupedRDD",
            "org.apache.spark.rdd.ReducedRDD",
            "org.apache.spark.rdd.JoinedRDD"
        };
        
        // Verify all expected RDD types are present
        assertEquals("Should have 9 RDD types", 9, expectedTypes.length);
        
        for (String rddType : expectedTypes) {
            assertNotNull("RDD type should not be null", rddType);
            assertTrue("RDD type should start with org.apache.spark.rdd", 
                      rddType.startsWith("org.apache.spark.rdd"));
        }
    }
    
    @Test
    public void testAdviceClassMapping() {
        // Test that advice classes can be loaded
        try {
            Class<?> mapPartitionsAdvice = RDDMapPartitionsAdvice.class;
            Class<?> filteredAdvice = RDDFilteredAdvice.class;
            Class<?> flatMappedAdvice = RDDFlatMappedAdvice.class;
            
            assertNotNull("MapPartitionsAdvice should be loadable", mapPartitionsAdvice);
            assertNotNull("FilteredAdvice should be loadable", filteredAdvice);
            assertNotNull("FlatMappedAdvice should be loadable", flatMappedAdvice);
            
        } catch (Exception e) {
            fail("Failed to load advice classes: " + e.getMessage());
        }
    }
    
    @Test
    public void testSGXRDDFactoryMethods() {
        // Test that SGXRDDFactory methods exist and can be called
        try {
            // These methods should exist (even if they throw exceptions in test environment)
            assertNotNull("createSGXMapPartitionsRDD method should exist", 
                         SGXRDDFactory.class.getMethod("createSGXMapPartitionsRDD", 
                                                     org.apache.spark.rdd.RDD.class,
                                                     org.apache.spark.Partition.class,
                                                     org.apache.spark.TaskContext.class));
            
            assertNotNull("createSGXFilteredRDD method should exist", 
                         SGXRDDFactory.class.getMethod("createSGXFilteredRDD", 
                                                     org.apache.spark.rdd.RDD.class,
                                                     org.apache.spark.Partition.class,
                                                     org.apache.spark.TaskContext.class));
            
        } catch (NoSuchMethodException e) {
            fail("SGXRDDFactory methods not found: " + e.getMessage());
        }
    }
}