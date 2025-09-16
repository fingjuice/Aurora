#!/bin/bash

# Build script for Spark SGX Native project (merged with JNI)

set -e

echo "Building Spark SGX Native project (with JNI integration)..."

# Check if CMake is installed
if ! command -v cmake &> /dev/null; then
    echo "Error: CMake is not installed. Please install CMake first."
    exit 1
fi

# Check if make is installed
if ! command -v make &> /dev/null; then
    echo "Error: Make is not installed. Please install Make first."
    exit 1
fi

# Check if g++ is installed
if ! command -v g++ &> /dev/null; then
    echo "Error: g++ is not installed. Please install g++ first."
    exit 1
fi

# Check if Java is installed
if ! command -v java &> /dev/null; then
    echo "Error: Java is not installed. Please install Java 8 or higher."
    exit 1
fi

# Set Java home if not set
if [ -z "$JAVA_HOME" ]; then
    echo "Warning: JAVA_HOME is not set. Trying to find Java installation..."
    if command -v java &> /dev/null; then
        JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
        export JAVA_HOME
        echo "Found Java at: $JAVA_HOME"
    else
        echo "Error: Could not find Java installation"
        exit 1
    fi
fi

# Create build directory
mkdir -p build
cd build

# Configure with CMake
echo "Configuring with CMake..."
cmake .. -DCMAKE_BUILD_TYPE=Release \
         -DSGX_SDK_PATH=${SGX_SDK_PATH:-/opt/intel/sgxsdk} \
         -DJAVA_HOME=$JAVA_HOME

# Build the project
echo "Building project..."
make -j$(nproc)

echo "Build completed successfully!"
echo "Library location: build/libsgxspark.so"

# Run tests
echo "Running tests..."
if [ -f "./test_sgx_spark" ]; then
    ./test_sgx_spark
    echo "Tests completed successfully!"
else
    echo "Warning: Test executable not found"
fi

# Install (optional)
if [ "$1" = "--install" ]; then
    echo "Installing library..."
    sudo make install
    echo "Library installed successfully!"
fi

echo ""
echo "Project structure:"
echo "├── include/"
echo "│   ├── jni/           # JNI interface headers"
echo "│   ├── enclave/       # SGX enclave headers"
echo "│   ├── operations/    # RDD operation headers"
echo "│   └── utils/         # Utility headers"
echo "├── src/"
echo "│   ├── jni/           # JNI implementation"
echo "│   ├── enclave/       # SGX enclave implementation"
echo "│   ├── operations/    # RDD operation implementation"
echo "│   └── utils/         # Utility implementation"
echo "├── tests/             # Test files"
echo "└── build/             # Build output"
echo ""
echo "To use the library, set LD_LIBRARY_PATH:"
echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:$(pwd)"