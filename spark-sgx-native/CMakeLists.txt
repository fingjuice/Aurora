cmake_minimum_required(VERSION 3.10)
project(spark_sgx_native)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# SGX specific flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSGX_ENCLAVE")

# Include directories
include_directories(include)

# Find JNI
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

# Source files
set(SOURCES
    # Core compute operations
    src/operations/core/map_partitions_rdd_compute.cpp
    src/operations/core/filtered_rdd_compute.cpp
    src/operations/core/flat_mapped_rdd_compute.cpp
    src/operations/core/union_rdd_compute.cpp
    src/operations/core/intersection_rdd_compute.cpp
    src/operations/core/distinct_rdd_compute.cpp
    
    # Key-value compute operations
    src/operations/keyvalue/grouped_rdd_compute.cpp
    src/operations/keyvalue/reduced_rdd_compute.cpp
    src/operations/keyvalue/joined_rdd_compute.cpp
    
    # Action compute operations
    src/operations/actions/collect_action_compute.cpp
    src/operations/actions/count_action_compute.cpp
    src/operations/actions/first_action_compute.cpp
    src/operations/actions/take_action_compute.cpp
    src/operations/actions/foreach_action_compute.cpp
    src/operations/actions/reduce_action_compute.cpp
    
    # Transformation compute operations
    src/operations/transformations/map_values_compute.cpp
    src/operations/transformations/flat_map_values_compute.cpp
    src/operations/transformations/sample_compute.cpp
    src/operations/transformations/sort_by_compute.cpp
    src/operations/transformations/coalesce_compute.cpp
    
    # Factory
    src/operations/rdd_compute_factory.cpp
    
    # Enclave
    src/enclave/sgx_spark_enclave.cpp
    
    # JNI - Main
    src/jni/sgx_spark_jni_main.cpp
    
    # JNI - Core operations
    src/jni/operations/core/map_partitions_rdd_jni.cpp
    src/jni/operations/core/filtered_rdd_jni.cpp
    src/jni/operations/core/flat_mapped_rdd_jni.cpp
    src/jni/operations/core/union_rdd_jni.cpp
    src/jni/operations/core/intersection_rdd_jni.cpp
    src/jni/operations/core/distinct_rdd_jni.cpp
    
    # JNI - Key-value operations
    src/jni/operations/keyvalue/grouped_rdd_jni.cpp
    src/jni/operations/keyvalue/reduced_rdd_jni.cpp
    src/jni/operations/keyvalue/joined_rdd_jni.cpp
    
    # JNI - Action operations
    src/jni/operations/actions/collect_action_jni.cpp
    src/jni/operations/actions/count_action_jni.cpp
    src/jni/operations/actions/first_action_jni.cpp
    src/jni/operations/actions/take_action_jni.cpp
    src/jni/operations/actions/foreach_action_jni.cpp
    src/jni/operations/actions/reduce_action_jni.cpp
    
    # JNI - Transformation operations
    src/jni/operations/transformations/map_values_jni.cpp
    src/jni/operations/transformations/flat_map_values_jni.cpp
    src/jni/operations/transformations/sample_jni.cpp
    src/jni/operations/transformations/sort_by_jni.cpp
    src/jni/operations/transformations/coalesce_jni.cpp
    
    # Utils
    src/utils/sgx_utils.cpp
)

# Create shared library
add_library(sgx_spark_jni SHARED ${SOURCES})

# Link libraries
target_link_libraries(sgx_spark_jni)

# Set output directory
set_target_properties(sgx_spark_jni PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# Install targets
install(TARGETS sgx_spark_jni
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)