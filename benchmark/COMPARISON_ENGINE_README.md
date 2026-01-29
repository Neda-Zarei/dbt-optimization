# Comparison Engine - Benchmark Regression Detection

## Overview

The Comparison Engine evaluates candidate benchmark results against baseline benchmarks with configurable regression thresholds and automatic severity classification. It enables regression detection for dbt pipelines in CI/CD environments.

## Features

### Metrics Comparison
- **Delta Calculation**: Computes absolute and percentage changes for all KPIs
- **Per-Model Comparison**: Evaluates each model's metrics independently
- **Pipeline-Level Aggregation**: Compares total and average metrics across pipeline
- **Edge Case Handling**: Gracefully handles nulls, zeros, and missing data

### Threshold Configuration
- **Percentage-Based Thresholds**: Relative limits (e.g., `execution_time_ms: 10%`)
- **Absolute Thresholds**: Fixed limits (e.g., `spilling_bytes: 1GB`)
- **Asymmetric Thresholds**: Different limits for improvements vs regressions
- **Per-Metric Configuration**: YAML-based configuration for all metrics

### Severity Classification
- **INFO Level**: Improvements and neutral changes
- **WARNING Level**: Minor regressions (user-configurable severity)
- **ERROR Level**: Major regressions (user-configurable severity)

### CI/CD Integration
- **Exit Codes**: 
  - `0`: PASS - No violations
  - `1`: WARNING - Minor regressions detected
  - `2`: ERROR - Major regressions detected
- **Structured Reports**: Text and JSON output for automation

## Architecture

### Core Components

#### 1. `thresholds.py` - Threshold Management

**ThresholdManager**
```python
manager = ThresholdManager("benchmark/config/thresholds.yaml")
threshold = manager.get_threshold("execution_time_ms")
is_configured = manager.is_metric_configured("bytes_scanned")
```

**MetricsComparer**
```python
comparer = MetricsComparer(threshold_manager, ignore_improvements=False)

# Calculate deltas
delta, delta_pct = comparer.calculate_delta(baseline=100, candidate=110)

# Evaluate single metric
violation = comparer.evaluate_threshold(
    "execution_time_ms",
    baseline_value=100,
    candidate_value=115
)

# Compare multiple metrics
violations = comparer.compare_metrics(
    baseline_metrics={"metric1": 100, "metric2": 200},
    candidate_metrics={"metric1": 105, "metric2": 230}
)
```

#### 2. `comparison_engine.py` - Pipeline Comparison

**ComparisonEngine**
```python
engine = ComparisonEngine(
    threshold_config_path="benchmark/config/thresholds.yaml",
    ignore_improvements=False
)

# Load data
baseline = engine.load_baseline("path/to/baseline.json")
candidate = engine.load_candidate("path/to/candidate.json")

# Compare
comparison = engine.compare_pipeline(baseline, candidate)

# Generate reports
status, exit_code = engine.generate_summary(comparison)
text_report = engine.format_report(comparison)
json_report = engine.generate_json_report(comparison)
```

### Data Structures

#### Violation
```python
violation = Violation(
    metric_name="execution_time_ms",
    baseline_value=100,
    candidate_value=115,
    delta=15,
    delta_percent=15.0,
    threshold=10,
    threshold_type="percent",
    severity=SeverityLevel.WARNING,
    is_improvement=False
)

# Get detailed message
message = violation.get_message()
# Output: "execution_time_ms: baseline=100, candidate=115, delta=15.00 (15.00%), threshold=10%"
```

#### ModelComparison
```python
model_comp = ModelComparison(
    model_name="fact_portfolio",
    baseline_metrics={...},
    candidate_metrics={...},
    violations=[...],
    metrics_missing_in_baseline=[...],
    metrics_missing_in_candidate=[...]
)

max_severity = model_comp.get_max_severity()
```

#### PipelineComparison
```python
pipeline_comp = PipelineComparison(
    pipeline_name="A",
    baseline_timestamp="20240115_100000",
    candidate_timestamp="20240115_110000",
    model_comparisons={...},
    pipeline_violations=[...]
)

all_violations = pipeline_comp.get_all_violations()
violation_counts = pipeline_comp.count_violations_by_severity()
# Output: {'INFO': 0, 'WARNING': 1, 'ERROR': 0}
```

## Configuration

### Threshold Configuration (thresholds.yaml)

```yaml
thresholds:
  # Percentage-based threshold
  execution_time_ms:
    max_increase_percent: 10
    severity: "high"
    description: "Query execution time - 10% increase tolerance"
  
  # Absolute threshold
  spilling_to_local_storage_bytes:
    max_increase_absolute: 1073741824  # 1GB
    severity: "high"
    description: "Local disk spilling - 1GB increase tolerance"
  
  # Improvement tracking (negative threshold)
  partition_pruning_ratio:
    max_increase_percent: -10  # Allow 10% decrease in pruning
    severity: "medium"
```

### Severity Levels

Each metric's severity level affects how violations are classified:

- **critical**: Always results in ERROR
- **high**: ERROR if violation > 50% of threshold, else WARNING
- **medium**: Always WARNING
- **low**: Always INFO

### Comparison Configuration (config.yaml)

```yaml
comparison:
  thresholds:
    config_path: "benchmark/config/thresholds.yaml"
  
  behavior:
    ignore_improvements: false      # Report improvements as violations
    report_per_model: true          # Include per-model comparisons
    compare_aggregations: true      # Include pipeline aggregations
    report_missing_metrics: true    # Report metrics missing in baseline/candidate
  
  output:
    write_text_report: true         # Generate human-readable report
    write_json_report: true         # Generate machine-readable report
    output_directory: "benchmark/results"
    report_pattern: "comparison_{pipeline}_{timestamp}"
```

## Usage Examples

### Basic Comparison

```python
from comparison_engine import ComparisonEngine

# Initialize engine
engine = ComparisonEngine()

# Load data
baseline = engine.load_baseline("benchmark/baselines/baseline_A_20240115_100000.json")
candidate = engine.load_candidate("benchmark/results/candidate_A_20240115_110000.json")

# Compare
comparison = engine.compare_pipeline(baseline, candidate)

# Get status
status, exit_code = engine.generate_summary(comparison)
print(f"Status: {status.name}, Exit Code: {exit_code}")

# Print report
print(engine.format_report(comparison))

# Get JSON report
json_report = engine.generate_json_report(comparison)
```

### Ignore Improvements

```python
# Don't report improvements as violations
engine = ComparisonEngine(ignore_improvements=True)

comparison = engine.compare_pipeline(baseline, candidate)
# Only regressions will be reported
```

### Custom Thresholds

```python
# Use custom threshold configuration
engine = ComparisonEngine(
    threshold_config_path="path/to/custom_thresholds.yaml"
)
```

### Per-Model Analysis

```python
comparison = engine.compare_pipeline(baseline, candidate)

# Analyze each model
for model_name, model_comp in comparison.model_comparisons.items():
    print(f"\nModel: {model_name}")
    print(f"  Max Severity: {model_comp.get_max_severity().name}")
    print(f"  Violations: {len(model_comp.violations)}")
    
    for violation in model_comp.violations:
        print(f"    - {violation.get_message()}")
```

## Delta Calculation

### Percentage Delta
```
delta_percent = ((candidate_value - baseline_value) / baseline_value) * 100
```

### Absolute Delta
```
delta = candidate_value - baseline_value
```

### Edge Cases

| Baseline | Candidate | Delta | Delta % | Notes |
|----------|-----------|-------|---------|-------|
| 100 | 110 | +10 | +10% | Normal increase |
| 100 | 90 | -10 | -10% | Improvement |
| 0 | 100 | +100 | +∞ | Baseline is zero |
| 100 | 0 | -100 | -100% | Candidate is zero |
| 0 | 0 | 0 | 0% | Both zero |
| null | 100 | null | null | Missing baseline |
| 100 | null | null | null | Missing candidate |

## Severity Classification

### HIGH Severity Metric

For metrics marked as `severity: "high"`:
- **ERROR**: if violation > 50% of threshold
- **WARNING**: if violation ≤ 50% of threshold

Example with `execution_time_ms` (10% threshold):
- 15% increase → ERROR (violation is 150% of threshold)
- 12% increase → WARNING (violation is 120% of threshold)
- 11% increase → WARNING (violation is 110% of threshold)

### CRITICAL Severity Metric

Always results in ERROR, regardless of violation magnitude.

### MEDIUM Severity Metric

Always results in WARNING.

## Report Generation

### Text Report Format

```
================================================================================
BENCHMARK COMPARISON REPORT
================================================================================
Pipeline: A
Baseline: 20240115_100000
Candidate: 20240115_110000

VIOLATION SUMMARY
----------------
  INFO:    0 violations (improvements/neutral)
  WARNING: 2 violations (minor regressions)
  ERROR:   1 violations (major regressions)

OVERALL STATUS
----------------
  Status: ERROR (exit code: 2)

MODEL-LEVEL RESULTS
----------------
  fact_portfolio: ERROR
    ✗ warehouse_credits: baseline=50.25, candidate=60.30, delta=10.05 (20.00%), threshold=15%
    ⚠ execution_time_ms: baseline=5000, candidate=5150, delta=150.00 (3.00%), threshold=10%
```

### JSON Report Format

```json
{
  "pipeline": "A",
  "baseline_timestamp": "20240115_100000",
  "candidate_timestamp": "20240115_110000",
  "status": "ERROR",
  "exit_code": 2,
  "summary": {
    "total_violations": 3,
    "info": 0,
    "warning": 2,
    "error": 1
  },
  "violations": {
    "INFO": [],
    "WARNING": [...],
    "ERROR": [...]
  },
  "models": {
    "fact_portfolio": {
      "violations": [...],
      "missing_in_baseline": [],
      "missing_in_candidate": []
    }
  }
}
```

## Testing

Run the test suite:

```bash
cd benchmark/scripts
python test_comparison_engine.py
```

Tests cover:
- Threshold configuration loading
- Delta calculation for various scenarios
- Threshold evaluation and violation detection
- Severity classification based on magnitude
- Absolute vs percentage thresholds
- Pipeline comparison orchestration
- Exit code generation
- Report generation (text and JSON)
- Missing metrics handling

## Integration with CI/CD

### GitHub Actions Example

```yaml
- name: Compare Benchmark
  run: |
    python benchmark/scripts/comparison_engine.py \
      --baseline benchmark/baselines/baseline_A_20240115_100000.json \
      --candidate benchmark/results/candidate_A_20240115_110000.json \
      --output-dir benchmark/results
  continue-on-error: true

- name: Check Benchmark Status
  run: |
    exit_code=$?
    if [ $exit_code -eq 2 ]; then
      echo "❌ Benchmark FAILED - Major regressions detected"
      exit 1
    elif [ $exit_code -eq 1 ]; then
      echo "⚠️  Benchmark WARNING - Minor regressions detected"
    else
      echo "✅ Benchmark PASSED - No regressions"
    fi
```

## Common Scenarios

### Scenario 1: Performance Regression Detected

```
Baseline execution_time_ms: 5000
Candidate execution_time_ms: 5600
Delta: +600ms (+12%)
Threshold: 10%

Result: ERROR (violation exceeds threshold)
```

### Scenario 2: Within Acceptable Range

```
Baseline execution_time_ms: 5000
Candidate execution_time_ms: 5400
Delta: +400ms (+8%)
Threshold: 10%

Result: PASS (within threshold)
```

### Scenario 3: Improvement

```
Baseline execution_time_ms: 5000
Candidate execution_time_ms: 4500
Delta: -500ms (-10%)
Threshold: 10%

Result: INFO (improvement, if not ignoring improvements)
       or PASS (if ignoring improvements)
```

### Scenario 4: Multiple Metrics

```
Baseline metrics:
  - execution_time_ms: 5000
  - warehouse_credits: 50

Candidate metrics:
  - execution_time_ms: 5400 (+8%, PASS)
  - warehouse_credits: 60 (+20%, ERROR)

Result: ERROR (one critical metric failed)
```

## Troubleshooting

### No Violations Reported (Expected Violations)

1. Check threshold values in `thresholds.yaml`
2. Verify metric names match configuration
3. Check `ignore_improvements` setting
4. Review delta calculation for edge cases

### Unexpected Exit Codes

1. Verify severity classification logic
2. Check threshold configuration for metric
3. Review violation magnitude calculations

### Missing Metrics in Reports

1. Ensure metrics are present in both baseline and candidate
2. Check metric naming consistency
3. Verify data collection in baseline/candidate

## Performance Considerations

- **Memory**: Comparison engine loads full baseline and candidate into memory
- **Speed**: Delta calculation is O(n) where n = number of metrics
- **Scalability**: Designed for pipelines with 10-1000 models

## Future Enhancements

- [ ] Statistical significance testing (t-tests for metric changes)
- [ ] Trend analysis (multiple baselines over time)
- [ ] Automatic threshold suggestions based on historical data
- [ ] Custom comparison functions per metric
- [ ] Streaming/incremental comparison for large pipelines
- [ ] Integration with cloud storage (S3, GCS)

## See Also

- [Baseline Manager](./BASELINE_MANAGER_README.md) - Baseline capture and storage
- [Quick Start Guide](./QUICK_START.md) - Getting started with the benchmark system
- [Configuration Reference](./config/) - Configuration file formats
