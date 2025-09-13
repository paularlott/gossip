# Testing Guide for Gossip Library

This document provides comprehensive instructions for running tests, benchmarks, and code coverage analysis for the gossip library.

## Prerequisites

Ensure you have Go 1.19 or later installed:
```bash
go version
```

## Running Tests

### Basic Test Execution

Run all tests:
```bash
go test ./...
```

Run tests with verbose output:
```bash
go test -v ./...
```

Run tests in short mode (skips long-running tests):
```bash
go test -short ./...
```

### Specific Test Categories

Run packet pool tests specifically:
```bash
go test -v -run "TestPacket" ./...
```

Run integration tests (longer running):
```bash
go test -v -run "Integration" ./...
```

Run comprehensive library tests:
```bash
go test -v -run "TestCluster|TestCodec|TestCompression|TestEncryption" ./...
```

### Parallel Test Execution

Run tests in parallel for faster execution:
```bash
go test -parallel 4 ./...
```

## Benchmarks

### Running All Benchmarks

```bash
go test -bench=. ./...
```

### Running Specific Benchmarks

Handler registry benchmarks:
```bash
go test -bench=BenchmarkHandler ./...
```

Packet pool benchmarks:
```bash
go test -bench=BenchmarkPacket ./...
```

### Benchmark with Memory Profiling

```bash
go test -bench=. -benchmem ./...
```

### Benchmark Comparison

To compare benchmark results over time:
```bash
# Run baseline
go test -bench=. ./... > baseline.txt

# After changes
go test -bench=. ./... > current.txt

# Compare (requires benchcmp tool)
benchcmp baseline.txt current.txt
```

## Code Coverage

### Basic Coverage Analysis

Generate coverage report:
```bash
go test -cover ./...
```

### Detailed Coverage Report

Generate detailed HTML coverage report:
```bash
# Generate coverage profile
go test -coverprofile=coverage.out ./...

# Generate HTML report
go tool cover -html=coverage.out -o coverage.html

# Open in browser (macOS)
open coverage.html

# Open in browser (Linux)
xdg-open coverage.html
```

### Coverage by Package

```bash
go test -cover ./... | grep -E "(PASS|FAIL|coverage)"
```

### Coverage with Function Details

```bash
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

### Coverage Threshold Checking

Set minimum coverage threshold:
```bash
#!/bin/bash
COVERAGE=$(go test -cover ./... | grep "coverage:" | awk '{print $5}' | sed 's/%//' | sort -n | tail -1)
THRESHOLD=80

if (( $(echo "$COVERAGE < $THRESHOLD" | bc -l) )); then
    echo "Coverage $COVERAGE% is below threshold $THRESHOLD%"
    exit 1
else
    echo "Coverage $COVERAGE% meets threshold $THRESHOLD%"
fi
```

## Race Condition Detection

Run tests with race detector:
```bash
go test -race ./...
```

Run specific tests with race detection:
```bash
go test -race -run "TestPacket.*Concurrent" ./...
```

## Memory Leak Detection

### Using Built-in Tools

Run tests with memory profiling:
```bash
go test -memprofile=mem.prof ./...
go tool pprof mem.prof
```

### Stress Testing

Run stress tests to detect memory leaks:
```bash
go test -run "TestPacketPoolStressTest" -timeout=30s ./...
```

## Performance Profiling

### CPU Profiling

```bash
go test -cpuprofile=cpu.prof -bench=. ./...
go tool pprof cpu.prof
```

### Memory Profiling

```bash
go test -memprofile=mem.prof -bench=. ./...
go tool pprof mem.prof
```

### Block Profiling

```bash
go test -blockprofile=block.prof -bench=. ./...
go tool pprof block.prof
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-go@v3
      with:
        go-version: '1.21'
    - name: Run tests
      run: go test -race -coverprofile=coverage.out ./...
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.out
```

## Test Organization

### Test File Structure

- `*_test.go` - Standard unit tests
- `packet_pool_test.go` - Packet allocation/release tests
- `packet_integration_test.go` - Integration scenarios
- `comprehensive_test.go` - Library-wide functionality tests
- `*_benchmark_test.go` - Performance benchmarks

### Test Categories

1. **Unit Tests**: Test individual functions and methods
2. **Integration Tests**: Test component interactions
3. **Packet Pool Tests**: Verify memory management
4. **Stress Tests**: High-load scenarios
5. **Race Tests**: Concurrent access patterns
6. **Benchmark Tests**: Performance measurements

## Debugging Failed Tests

### Verbose Output

```bash
go test -v -run "TestFailingTest" ./...
```

### Test with Debugging

```bash
go test -v -run "TestFailingTest" -args -debug ./...
```

### Running Single Test

```bash
go test -v -run "^TestSpecificTest$" ./...
```

## Best Practices

1. **Always run tests before committing**:
   ```bash
   go test -race -short ./...
   ```

2. **Check coverage regularly**:
   ```bash
   go test -cover ./... | grep -v "no test files"
   ```

3. **Run stress tests periodically**:
   ```bash
   go test -run "Stress|Concurrent" -timeout=60s ./...
   ```

4. **Profile performance-critical code**:
   ```bash
   go test -bench=BenchmarkCriticalPath -cpuprofile=cpu.prof ./...
   ```

5. **Validate packet management**:
   ```bash
   go test -v -run "TestPacket.*Leak|TestPacket.*Pool" ./...
   ```

## Troubleshooting

### Common Issues

1. **Race conditions**: Use `-race` flag
2. **Memory leaks**: Check packet reference counting
3. **Timeouts**: Increase timeout with `-timeout=30s`
4. **Flaky tests**: Run multiple times with `-count=10`

### Environment Variables

```bash
# Enable detailed logging
export GOSSIP_LOG_LEVEL=debug

# Disable parallel execution
export GOMAXPROCS=1

# Enable race detection
export CGO_ENABLED=1
```

## Coverage Goals

Target coverage levels:
- **Overall**: 85%+
- **Core packet management**: 95%+
- **Transport layer**: 80%+
- **Codec implementations**: 90%+
- **Metadata operations**: 90%+

## Reporting Issues

When reporting test failures, include:
1. Go version: `go version`
2. OS and architecture: `go env GOOS GOARCH`
3. Test command used
4. Full test output
5. Coverage report if relevant