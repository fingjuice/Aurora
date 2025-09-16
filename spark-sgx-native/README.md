# Spark SGX Native Project (with JNI Integration)

This project implements the C/C++ native library that provides SGX-enabled RDD operations for Apache Spark, with integrated JNI support for seamless Java integration.

## Overview

The native library implements secure RDD operations within Intel SGX enclaves, providing hardware-level security for Spark data processing. It includes integrated JNI functionality for direct communication with the Java ByteBuddy agent.

## Features

- **SGX Integration**: Executes RDD operations within Intel SGX enclaves
- **JNI Integration**: Direct JNI interface for Java communication
- **Multiple Operations**: Supports map, filter, reduce, collect, count, foreach, and take
- **Data Serialization**: Automatic conversion between Java and C++ data types
- **Memory Management**: Secure memory allocation and deallocation
- **Error Handling**: Comprehensive error handling and logging
- **Performance Optimization**: Optimized for SGX environment

## Project Structure

```
spark-sgx-native/
├── include/                     # Header files
│   ├── jni/                     # JNI interface headers
│   │   └── sgx_spark_jni.h      # Main JNI interface
│   ├── enclave/                 # SGX enclave headers
│   │   └── sgx_spark_enclave.h  # Enclave interface
│   ├── operations/              # RDD operation headers
│   │   └── rdd_operations.h     # RDD operation definitions
│   └── utils/                   # Utility headers
│       ├── data_serialization.h # Data conversion utilities
│       └── sgx_utils.h          # SGX utility functions
├── src/                         # Source files
│   ├── jni/                     # JNI implementation
│   │   └── sgx_spark_jni.cpp    # JNI implementation
│   ├── enclave/                 # SGX enclave implementation
│   │   └── sgx_spark_enclave.cpp # Enclave implementation
│   ├── operations/              # RDD operation implementation
│   │   └── rdd_operations.cpp   # RDD operation logic
│   └── utils/                   # Utility implementation
│       ├── data_serialization.cpp # Data serialization
│       └── sgx_utils.cpp        # SGX utilities
├── tests/                       # Test files
│   └── test_main.cpp            # Main test file
├── CMakeLists.txt               # CMake configuration
├── build.sh                     # Build script
└── README.md                    # This file
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                Java ByteBuddy Agent                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   RDD Map       │  │  RDD Filter     │  │  RDD Reduce     │ │
│  │   Interceptor   │  │  Interceptor    │  │  Interceptor    │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────┬───────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────┐
│                  JNI Interface                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   Data          │  │   Function      │  │   Result        │ │
│  │   Conversion    │  │   Deserialization│  │   Conversion    │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────┬───────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────┐
│                SGX Enclave                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   Map           │  │   Filter        │  │   Reduce        │ │
│  │   Operations    │  │   Operations    │  │   Operations    │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   Collect       │  │   Count         │  │   Foreach       │ │
│  │   Operations    │  │   Operations    │  │   Operations    │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

### System Requirements
- Linux operating system (Ubuntu 20.04+ recommended)
- Intel CPU with SGX support
- Intel SGX SDK 2.17+
- CMake 3.10+
- GCC/G++ 7.0+
- Make
- Java 8+ (for JNI)

### SGX Setup
1. **Install Intel SGX SDK**:
   ```bash
   # Download SGX SDK
   wget https://download.01.org/intel-sgx/sgx-linux/2.17/distro/ubuntu20.04-server/sgx_linux_x64_sdk_2.17.100.3.bin
   chmod +x sgx_linux_x64_sdk_2.17.100.3.bin
   sudo ./sgx_linux_x64_sdk_2.17.100.3.bin
   ```

2. **Set Environment Variables**:
   ```bash
   export SGX_SDK=/opt/intel/sgxsdk
   export PATH=$PATH:$SGX_SDK/bin:$SGX_SDK/bin/x64
   export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$SGX_SDK/pkgconfig
   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$SGX_SDK/sdk_libs
   ```

3. **Set Java Home**:
   ```bash
   export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
   ```

## Building

### Quick Build
```bash
# Clone and build
git clone <repository-url>
cd spark-sgx-native
./build.sh
```

### Manual Build
```bash
# Create build directory
mkdir build && cd build

# Configure with CMake
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DSGX_SDK_PATH=/opt/intel/sgxsdk \
         -DJAVA_HOME=$JAVA_HOME

# Build
make -j$(nproc)

# Install (optional)
sudo make install
```

### Build Options

| Option | Description | Default |
|--------|-------------|---------|
| `CMAKE_BUILD_TYPE` | Build type (Debug/Release) | `Release` |
| `SGX_SDK_PATH` | Path to SGX SDK | `/opt/intel/sgxsdk` |
| `JAVA_HOME` | Path to Java installation | `$JAVA_HOME` |
| `CMAKE_INSTALL_PREFIX` | Installation prefix | `/usr/local` |

## Usage

### Library Loading

The library is automatically loaded by the Java JNI wrapper:

```java
// In Java code
System.loadLibrary("sgxspark");
```

### Manual Loading

```bash
# Set library path
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/spark-sgx-native/build

# Load library
ldconfig
```

## API Reference

### JNI Interface

#### Core Functions

```cpp
// Initialization
JNIEXPORT jint JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeInitialize(JNIEnv*, jclass);

// RDD Operations
JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteMapOperation(
    JNIEnv*, jclass, jobject, jstring);

JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteFilterOperation(
    JNIEnv*, jclass, jobject, jstring);

JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteReduceOperation(
    JNIEnv*, jclass, jobject, jstring);

JNIEXPORT jlong JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteCountOperation(
    JNIEnv*, jclass, jobject);

JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteCollectOperation(
    JNIEnv*, jclass, jobject);

JNIEXPORT void JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteForeachOperation(
    JNIEnv*, jclass, jobject, jstring);

JNIEXPORT jobject JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeExecuteTakeOperation(
    JNIEnv*, jclass, jobject, jint);

// Cleanup
JNIEXPORT void JNICALL Java_org_apache_spark_sgx_SGXJNIWrapper_nativeCleanup(JNIEnv*, jclass);
```

### C++ Classes

#### SGXSparkJNI
Main JNI wrapper class for Java communication.

#### SGXSparkEnclave
SGX enclave interface for secure operations.

#### RDDOperations
Namespace containing RDD operation implementations.

#### DataSerialization
Namespace for Java-C++ data conversion.

#### SGXUtils
Namespace for SGX utility functions.

## Data Format

### Input Format

The library expects Java objects converted to strings:

```java
List<Object> inputData = Arrays.asList(1, 2, 3, 4, 5);
```

### Function Format

Functions are serialized as JSON:

```json
{
  "type": "map",
  "function": "multiply",
  "parameters": {"factor": 2}
}
```

### Output Format

Results are returned as Java objects:

```java
List<Object> result = // ... from SGX operation
```

## Supported Operations

### Map Operations

| Function | Description | Parameters |
|----------|-------------|------------|
| `multiply` | Multiply by factor | `factor` (number) |
| `add` | Add constant | `constant` (number) |
| `uppercase` | Convert to uppercase | None |
| `lowercase` | Convert to lowercase | None |

### Filter Operations

| Predicate | Description | Parameters |
|-----------|-------------|------------|
| `even` | Filter even numbers | None |
| `odd` | Filter odd numbers | None |
| `positive` | Filter positive numbers | None |
| `negative` | Filter negative numbers | None |
| `greater_than` | Filter greater than value | `value` (number) |
| `less_than` | Filter less than value | `value` (number) |

### Reduce Operations

| Function | Description | Parameters |
|----------|-------------|------------|
| `sum` | Sum all elements | None |
| `product` | Multiply all elements | None |
| `max` | Find maximum | None |
| `min` | Find minimum | None |
| `concat` | Concatenate strings | `separator` (string) |

## Testing

### Unit Tests

```bash
# Build with tests
cmake .. -DBUILD_TESTS=ON
make

# Run tests
./test_sgx_spark
```

### Integration Tests

```bash
# Test with Java JNI wrapper
java -Djava.library.path=build \
     -cp ../spark-sgx-bytebuddy/target/spark-sgx-bytebuddy-1.0.0.jar \
     org.apache.spark.sgx.SGXJNIWrapper
```

## Performance Optimization

### Memory Management

- Use secure memory allocation for sensitive data
- Implement proper cleanup in error cases
- Optimize buffer sizes for common operations

### SGX Optimization

- Minimize enclave transitions
- Batch operations when possible
- Use efficient data structures
- Optimize serialization/deserialization

### JNI Optimization

- Cache JNI method IDs
- Minimize JNI calls
- Use bulk operations when possible
- Optimize data conversion

## Troubleshooting

### Common Issues

1. **SGX Not Available**:
   - Check CPU SGX support
   - Verify SGX is enabled in BIOS
   - Check SGX driver installation

2. **JNI Errors**:
   - Check Java version compatibility
   - Verify JAVA_HOME is set correctly
   - Check library path

3. **Compilation Errors**:
   - Check SGX SDK installation
   - Verify environment variables
   - Check compiler version

### Debug Mode

Enable debug logging:

```bash
# Set debug environment variable
export SGX_DEBUG=1

# Run with debug output
./test_sgx_spark
```

## Security Considerations

### SGX Security

- **Enclave Isolation**: Data is processed in isolated enclaves
- **Memory Protection**: SGX provides hardware-level memory protection
- **Attestation**: Verify enclave integrity before processing
- **Secure Communication**: Encrypted communication between enclaves

### Data Protection

- **Input Validation**: Validate all input data
- **Output Sanitization**: Sanitize output data
- **Memory Clearing**: Clear sensitive data from memory
- **Error Handling**: Secure error handling without information leakage

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.