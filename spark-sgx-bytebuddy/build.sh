#!/bin/bash

# Build script for Spark SGX ByteBuddy project

set -e

echo "Building Spark SGX ByteBuddy project..."

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed. Please install Maven first."
    exit 1
fi

# Check if Java 8+ is installed
if ! command -v java &> /dev/null; then
    echo "Error: Java is not installed. Please install Java 8 or higher."
    exit 1
fi

# Clean and compile
echo "Cleaning previous build..."
mvn clean

echo "Compiling project..."
mvn compile

echo "Running tests..."
mvn test

echo "Packaging project..."
mvn package

echo "Build completed successfully!"
echo "JAR file location: target/spark-sgx-bytebuddy-1.0.0.jar"

# Create agent JAR
echo "Creating agent JAR..."
mvn package -DskipTests

echo "Agent JAR created: target/spark-sgx-bytebuddy-1.0.0.jar"
echo ""
echo "To use the agent, run Spark with:"
echo "java -javaagent:target/spark-sgx-bytebuddy-1.0.0.jar -jar your-spark-app.jar"

