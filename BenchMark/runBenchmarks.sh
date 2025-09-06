#!/bin/bash
echo "--- Starting all JMH benchmarks ---"

echo "Running BenchmarkConcurrentRead..."
java -jar ./target/dbBenchmarks.jar BenchmarkConcurrentRead -f 3 -wi 3 -i 3 -to 60m -rf json -rff BenchmarkConcurrentRead.json | tee BenchmarkConcurrentRead.txt
rm -rf RocksDBAdaptor* PureJavaLevelDBAdaptor* NativeLevelDBAdaptor* DataStore4JAdaptor*

echo "Running BenchmarkConcurrentWrite..."
java  -jar ./target/dbBenchmarks.jar BenchmarkConcurrentWrite -f 3 -wi 3 -i 3 -to 60m -rf json -rff BenchmarkConcurrentWrite.json | tee BenchmarkConcurrentWrite.txt
rm -rf RocksDBAdaptor* PureJavaLevelDBAdaptor* NativeLevelDBAdaptor* DataStore4JAdaptor*

echo "Running BenchmarkRead..."
java  -jar ./target/dbBenchmarks.jar BenchmarkRead -f 3 -wi 3 -i 3 -to 60m -rf json -rff BenchmarkRead.json | tee BenchmarkRead.txt
rm -rf RocksDBAdaptor* PureJavaLevelDBAdaptor* NativeLevelDBAdaptor* DataStore4JAdaptor*

echo "Running BenchmarkVariableSizeDataRead..."
java  -jar ./target/dbBenchmarks.jar BenchmarkVariableSizeDataRead -f 3 -wi 3 -i 3 -to 60m -rf json -rff BenchmarkVariableSizeDataRead.json | tee BenchmarkVariableSizeDataRead.txt
rm -rf RocksDBAdaptor* PureJavaLevelDBAdaptor* NativeLevelDBAdaptor* DataStore4JAdaptor*

echo "Running BenchmarkFreshRead..."
java  -jar ./target/dbBenchmarks.jar BenchmarkFreshRead -f 3 -wi 3 -i 3 -to 60m -rf json -rff BenchmarkFreshRead.json | tee BenchmarkFreshRead.txt
rm -rf RocksDBAdaptor* PureJavaLevelDBAdaptor* NativeLevelDBAdaptor* DataStore4JAdaptor*

echo "Running BenchmarkWrite..."
java  -jar ./target/dbBenchmarks.jar BenchmarkWrite -f 3 -wi 3 -i 3 -to 60m -rf json -rff BenchmarkWrite.json | tee BenchmarkWrite.txt
rm -rf RocksDBAdaptor* PureJavaLevelDBAdaptor* NativeLevelDBAdaptor* DataStore4JAdaptor*

echo "--- All benchmarks finished successfully. ---"