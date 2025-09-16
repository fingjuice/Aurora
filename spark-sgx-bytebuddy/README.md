# SGX Spark ByteBuddy Agent

This project implements a ByteBuddy-based Java agent that intercepts Spark RDD compute operations and redirects them to Intel SGX enclaves for secure computation.

## Architecture

### 1. Interception Strategy

The agent intercepts the `compute` method of specific RDD types:

- **MapPartitionsRDD** - for `map()` and `mapPartitions()` operations
- **FilteredRDD** - for `filter()` operations  
- **FlatMappedRDD** - for `flatMap()` operations
- **UnionRDD** - for `union()` operations
- **IntersectionRDD** - for `intersection()` operations
- **DistinctRDD** - for `distinct()` operations
- **GroupedRDD** - for `groupByKey()` operations
- **ReducedRDD** - for `reduceByKey()` operations
- **JoinedRDD** - for `join()` operations

### 2. Execution Flow

```
User Code: rdd.map(x => x * 2)
    ↓
RDD.map() creates MapPartitionsRDD
    ↓
MapPartitionsRDD.compute() ← INTERCEPTED HERE
    ↓
RDDMapPartitionsAdvice.onEnter()
    ↓
SGXRDDFactory.createSGXMapPartitionsRDD()
    ↓
SGXJNIWrapper.executeComputeOperation()
    ↓
SGX Enclave executes the actual computation
    ↓
Result returned to Java
```

### 3. Key Components

#### SGXSparkAgent
- Main agent class that installs bytecode transformations
- Intercepts specific RDD types based on class names
- Routes to appropriate advice classes

#### RDD*Advice Classes
- Individual advice classes for each RDD type
- Intercept `compute` method calls
- Redirect to SGX when available, fallback to original implementation

#### SGXRDDFactory
- Factory class for creating SGX-enabled RDDs
- Handles data collection and operation preparation
- Manages JNI calls to SGX backend

#### SGXJNIWrapper
- JNI interface to C++ SGX backend
- Handles data serialization/deserialization
- Manages SGX enclave lifecycle

## Usage

### 1. Build the Agent

```bash
mvn clean package
```

### 2. Run Spark with the Agent

```bash
java -javaagent:target/spark-sgx-bytebuddy-1.0.0.jar \
     -cp target/spark-sgx-bytebuddy-1.0.0.jar:spark-core.jar \
     your.spark.application.Main
```

### 3. Programmatic Usage

```java
// Initialize the agent
SGXSparkAgent.install();

// Your Spark code will automatically use SGX when available
JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
JavaRDD<Integer> mapped = rdd.map(x -> x * 2); // This will be intercepted and run in SGX
```

## Configuration

### Environment Variables

- `SGX_ENABLED=true` - Enable SGX processing
- `SGX_DEBUG=true` - Enable debug logging
- `SGX_FALLBACK=true` - Fallback to original implementation on SGX failure

### JVM Arguments

- `-Dsgx.enabled=true` - Enable SGX processing
- `-Dsgx.debug=true` - Enable debug logging
- `-Dsgx.fallback=true` - Enable fallback mode

## Security Features

### 1. Secure Computation
- All RDD operations execute within SGX enclaves
- Data is protected from external access
- Computation results are verified

### 2. Function Serialization
- User-defined functions are serialized and executed in SGX
- Function logic is protected from inspection
- Secure function execution environment

### 3. Data Protection
- Input data is encrypted before entering SGX
- Output data is encrypted before leaving SGX
- No data exposure in JVM memory

## Error Handling

### 1. SGX Unavailable
- Automatically falls back to original Spark implementation
- Logs warning messages
- Continues normal operation

### 2. SGX Failure
- Catches exceptions from SGX operations
- Falls back to original implementation
- Logs error details for debugging

### 3. JNI Errors
- Handles JNI call failures
- Provides meaningful error messages
- Maintains system stability

## Testing

### Unit Tests

```bash
mvn test
```

### Integration Tests

```bash
mvn verify
```

### Manual Testing

```bash
# Test with SGX available
SGX_ENABLED=true mvn test

# Test without SGX (fallback mode)
SGX_ENABLED=false mvn test
```

## Dependencies

- **ByteBuddy** - Bytecode manipulation
- **SLF4J** - Logging framework
- **JUnit** - Testing framework
- **Spark Core** - Apache Spark core library

## Requirements

- Java 8 or higher
- Apache Spark 3.2.3
- Intel SGX SDK (for native backend)
- Linux x86_64 architecture

## Limitations

1. **Function Serialization**: Currently uses simplified function serialization
2. **Data Collection**: RDD data collection is simplified for demonstration
3. **Type Safety**: Limited type checking in SGX operations
4. **Performance**: Overhead from JNI calls and data serialization

## Future Work

1. **Complete Function Serialization**: Implement full function serialization
2. **Optimized Data Collection**: Improve RDD data collection efficiency
3. **Type Safety**: Add comprehensive type checking
4. **Performance Optimization**: Reduce JNI and serialization overhead
5. **Additional RDD Types**: Support more RDD operations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0.