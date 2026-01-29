# dbt Benchmark System - Comprehensive Documentation

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Configuration Guide](#configuration-guide)
5. [CLI Command Reference](#cli-command-reference)
6. [Workflow Guides](#workflow-guides)
7. [Metrics Glossary](#metrics-glossary)
8. [Cross-Pipeline Dependencies](#cross-pipeline-dependencies)
9. [Troubleshooting Guide](#troubleshooting-guide)
10. [CI/CD Integration](#cicd-integration)

---

## Overview

The dbt Benchmark System is a comprehensive performance monitoring and optimization framework designed to track, compare, and analyze the execution metrics of dbt pipelines running on Snowflake. It enables data teams to detect performance regressions, validate optimization changes, and maintain performance standards across three interdependent analytics pipelines.

### What It Does

- **Captures Baselines**: Records comprehensive performance metrics and output validation hashes for reproducible comparison
- **Runs Benchmarks**: Executes pipelines and automatically compares them against saved baselines
- **Detects Regressions**: Identifies performance degradations across 20+ tracked metrics with configurable severity levels
- **Generates Reports**: Creates detailed JSON and text reports showing violations, improvements, and delta calculations
- **Manages Dependencies**: Automatically handles cross-pipeline dependencies (Pipeline C requires A and B)
- **CI/CD Integration**: Provides exit codes and machine-readable output for automated workflows

### Key Capabilities

| Feature | Details |
|---------|---------|
| **Metric Tracking** | 20+ performance KPIs including execution time, bytes scanned, warehouse credits, query complexity |
| **Configurable Thresholds** | Per-metric regression limits with severity levels (critical, high, medium) |
| **Output Validation** | Tracks data integrity with hash-based validation of pipeline outputs |
| **Pipeline Management** | Baseline capture, listing, comparison, and deletion with safeguards |
| **User-Friendly CLI** | Colored output, progress indicators, and helpful error messages |
| **Flexible Configuration** | YAML-based configuration for all thresholds, pipelines, and settings |

### Three Pipelines

The system manages three interconnected analytics pipelines:

| Pipeline | Name | Models | Complexity | Dependencies |
|----------|------|--------|-----------|--------------|
| **A** | Cashflow Analytics | 4 models | Simple | None |
| **B** | Trade Analytics | 9 models | Medium | None |
| **C** | Portfolio Analytics | 19 models | Complex | A, B |

Example key model from Pipeline C: `fact_portfolio_performance` aggregates portfolio-level metrics with comprehensive risk and attribution analysis.

---

## Prerequisites

### System Requirements

- **Python**: 3.8+ (3.10+ recommended)
- **dbt-core**: 1.0 or later (1.8+ tested)
- **dbt-snowflake**: adapter compatible with dbt-core version
- **Snowflake**: Account with appropriate warehouse and database permissions

### Python Packages

Core dependencies (installed via your dbt environment):

```bash
pip install dbt-snowflake>=1.5.0
pip install snowflake-connector-python>=3.0.0
pip install pyyaml>=6.0
pip install jsonschema>=4.0
```

### Snowflake Access Requirements

- **Role**: ACCOUNTADMIN or role with QUERY_HISTORY access
- **Database**: Access to your target analytics database (e.g., `DBT_DEMO`)
- **Warehouse**: Access to execute queries (any size, though larger = faster execution)
- **Account Usage Views**: Access to `ACCOUNT_USAGE.QUERY_HISTORY` (45-minute latency)
- **Information Schema**: Access to `INFORMATION_SCHEMA.QUERY_HISTORY` (real-time)

### Environment Variables

```bash
# Snowflake credentials
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ROLE="ACCOUNTADMIN"
export SNOWFLAKE_DATABASE="DBT_DEMO"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
```

---

## Quick Start

### Your First Benchmark (5 minutes)

```bash
# 1. Capture baseline for Pipeline A
python -m benchmark capture-baseline --pipeline A

# 2. View captured baselines
python -m benchmark list-baselines

# 3. Run a benchmark (compare current execution to baseline)
python -m benchmark run-benchmark --pipeline A

# 4. View the report
cat benchmark/results/comparison_A_*.json | jq .
```

### Complete Example Workflow

```bash
# Step 1: Initial baseline capture
python -m benchmark capture-baseline --pipeline A
python -m benchmark capture-baseline --pipeline B
python -m benchmark capture-baseline --pipeline C  # auto-runs A and B first

# Step 2: Make optimization to fact_portfolio_performance
# (e.g., add partition pruning, use window functions instead of joins)
# ... edit models/pipeline_c/marts/fact_portfolio_performance.sql ...

# Step 3: Run benchmark to detect changes
python -m benchmark run-benchmark --pipeline C

# Step 4: Review report
# - Metrics improved? Success!
# - Metrics worse? Consider different optimization approach
```

---

## Configuration Guide

### Directory Structure

```
benchmark/
├── config/
│   ├── config.yaml           # Main settings (baseline, comparison, CLI)
│   ├── pipelines.yaml        # Pipeline definitions and dependencies
│   ├── thresholds.yaml       # Regression thresholds and severity
│   └── snowflake.yaml        # Snowflake connection (optional, uses env vars)
├── baselines/                # Baseline JSON files (tracked in git)
├── results/                  # Report outputs (gitignored)
├── scripts/                  # Python modules (cli.py, baseline_manager.py, etc.)
└── README.md                 # This file
```

### config.yaml

Main configuration file controlling baseline capture, comparison behavior, and CLI defaults:

```yaml
baseline:
  storage:
    base_directory: "benchmark/baselines"
    file_pattern: "baseline_{pipeline}_{timestamp}.json"
  retention:
    max_age_days: 90          # Keep baselines for 90 days
    max_count: 10             # Keep max 10 per pipeline
  capture:
    include_metrics: true     # Collect Snowflake performance metrics
    include_validation: true  # Validate output consistency
  content:
    include_dbt_version: true # Save dbt version in baseline
    include_git_commit: true  # Save git commit in baseline

comparison:
  thresholds:
    config_path: "benchmark/config/thresholds.yaml"
  behavior:
    ignore_improvements: false  # Report improvements as INFO violations
    report_per_model: true      # Show model-level breakdowns
  output:
    write_json_report: true
    output_directory: "benchmark/results"

cli:
  output_format: "text"    # text, json, or quiet
  use_color: true          # Colored terminal output
  verbosity: "normal"      # quiet, normal, verbose, debug
  show_progress: true
  confirm_delete: true     # Prompt before deleting baselines
```

### pipelines.yaml

Defines your three pipelines and their interdependencies:

```yaml
pipelines:
  A:
    name: "Simple Cashflow Pipeline"
    schema: "pipeline_a"
    models: "+pipeline_a.*"
    dependencies: []
  B:
    name: "Medium Complexity Trade Analytics"
    schema: "pipeline_b"
    models: "+pipeline_b.*"
    dependencies: []
  C:
    name: "Complex Portfolio Analytics"
    schema: "pipeline_c"
    models: "+pipeline_c.*"
    dependencies: ["A", "B"]  # Must run A and B first
```

### thresholds.yaml

Configurable regression thresholds with severity levels:

```yaml
thresholds:
  execution_time_ms:
    max_increase_percent: 10
    severity: "high"
  warehouse_credits:
    max_increase_percent: 15
    severity: "critical"
  bytes_scanned:
    max_increase_percent: 20
    severity: "high"
  spilling_to_local_storage_bytes:
    max_increase_absolute: 1073741824  # 1GB
    severity: "high"
```

**Severity Levels**:
- **critical**: Always results in ERROR (exit code 2)
- **high**: WARNING if < 50% of threshold, ERROR if > 50%
- **medium**: Always WARNING

---

## CLI Command Reference

### Overview

Run commands via:
```bash
python -m benchmark [GLOBAL_FLAGS] <command> [COMMAND_FLAGS]
```

### Global Flags

```bash
--verbose, -v              # Enable debug logging
--output-dir DIR          # Override output directory
--config-file PATH        # Use custom config file
--no-color                # Disable colored output
```

### Command: capture-baseline

**Purpose**: Execute a pipeline and save baseline metrics and validation hashes.

**Syntax**:
```bash
python -m benchmark capture-baseline --pipeline <A|B|C>
```

**Example**:
```bash
python -m benchmark capture-baseline --pipeline A

# Output:
# [1/4] Initializing baseline manager...
# [2/4] Running pipeline A...
# [3/4] Saving baseline...
# [4/4] Baseline captured successfully
# ✓ Baseline captured successfully
#   Filename: baseline_A_20240115_143022.json
#   Pipeline: A
#   Duration: 45.32s
#   Models:   4
```

**Exit Codes**:
- `0`: Success
- `1`: Failure (pipeline error, metrics collection failed)

### Command: run-benchmark

**Purpose**: Execute a pipeline and automatically compare against the latest baseline.

**Syntax**:
```bash
python -m benchmark run-benchmark --pipeline <A|B|C>
```

**Example**:
```bash
python -m benchmark run-benchmark --pipeline A

# Output shows status (PASS/WARNING/ERROR) and violation summary
```

**Exit Codes**:
- `0`: PASS - No regressions
- `1`: WARNING - Minor regressions
- `2`: ERROR - Major regressions or pipeline failed

### Command: list-baselines

**Purpose**: Display all captured baselines with metadata.

**Syntax**:
```bash
python -m benchmark list-baselines [--pipeline <A|B|C>]
```

**Example**:
```bash
python -m benchmark list-baselines --pipeline A

# Shows all baselines for Pipeline A with timestamps, status, duration, etc.
```

### Command: delete-baseline

**Purpose**: Remove a specific baseline file (with confirmation).

**Syntax**:
```bash
python -m benchmark delete-baseline --id FILENAME
```

**Example**:
```bash
python -m benchmark delete-baseline --id baseline_A_20240114_090000.json
```

### Command: compare

**Purpose**: Compare two specific baselines directly.

**Syntax**:
```bash
python -m benchmark compare --baseline FILE1 --candidate FILE2
```

**Example**:
```bash
python -m benchmark compare \
  --baseline baseline_A_20240114.json \
  --candidate baseline_A_20240115.json
```

---

## Workflow Guides

### Workflow 1: Baseline Capture → Optimize → Verify

**Step 1: Capture Initial Baseline**
```bash
python -m benchmark capture-baseline --pipeline C
python -m benchmark list-baselines
```

**Step 2: Optimize fact_portfolio_performance**
```sql
-- Add partition pruning or refactor joins for better performance
SELECT portfolio_id, date, portfolio_value,
  LAG(portfolio_value) OVER (PARTITION BY portfolio_id ORDER BY date) as daily_return
FROM int_portfolio_returns_daily
WHERE date >= CURRENT_DATE - INTERVAL '2 years'
ORDER BY portfolio_id, date;
```

**Step 3: Run Benchmark**
```bash
python -m benchmark run-benchmark --pipeline C
```

**Step 4: Evaluate Results**
- Exit code 0: PASS - Optimization successful!
- Exit code 1: WARNING - Minor regressions, review
- Exit code 2: ERROR - Major regressions, revert changes

### Workflow 2: Performance Regression Investigation

```bash
# Compare two baselines to identify when performance degraded
python -m benchmark compare \
  --baseline baseline_B_20240114.json \
  --candidate baseline_B_20240115.json

# Check git history to identify changes
git log --oneline --since="3 days ago" -- models/pipeline_b/

# Review affected SQL
git diff HEAD~5 models/pipeline_b/intermediate/int_trade_pnl.sql

# Fix issues and re-test
python -m benchmark run-benchmark --pipeline B
```

### Workflow 3: Cross-Pipeline Benchmarking

```bash
# Capture all pipelines (C auto-runs A and B)
python -m benchmark capture-baseline --pipeline C

# Run full stack benchmark
python -m benchmark run-benchmark --pipeline C

# Check cross-pipeline impact
cat benchmark/results/comparison_C_*.json | jq '.metrics.violations[]'
```

---

## Metrics Glossary

### Execution & Performance

**execution_time_ms**: Total query execution time in milliseconds. Directly impacts user experience and job duration.

**compilation_time_ms**: dbt's time to parse and compile SQL. High values indicate complex Jinja templating.

### Data Scanning

**bytes_scanned**: Total bytes read from storage. Directly impacts warehouse credits (Snowflake's cost metric).

**rows_scanned**: Number of rows read. Indicates if filtering is happening efficiently.

### Cost Metrics

**warehouse_credits**: Snowflake compute credits consumed. **Direct cost to your bill** (most critical KPI).

Formula: `(bytes_scanned / 10 GB) * (warehouse_size / 1)`

### Memory & Spilling

**spilling_to_local_storage_bytes**: Bytes written to local disk when memory overflows. Causes slowdown.

**spilling_to_remote_storage_bytes**: Bytes written to S3. Even more severe, extremely slow.

### Query Complexity

**join_count**: Number of JOIN operations. More joins = potential optimization opportunity.

**subquery_depth**: Maximum nesting depth. Deeper nesting = harder for query optimizer.

**window_function_count**: Number of window functions. Consolidate when possible.

### Partitioning

**partition_pruning_ratio**: Percentage of partitions scanned vs. available. Higher = better.

**partitions_scanned**: Actual partitions scanned. Fewer = less data read.

### Pipeline-Level Aggregations

**total_execution_time_ms**: Sum of all model execution times. Indicates overall pipeline throughput.

**total_bytes_scanned**: Total data movement across pipeline. Impacts credits directly.

**total_warehouse_credits**: Total cost for entire pipeline execution.

**model_count**: Number of models executed. Should never change (sanity check).

---

## Cross-Pipeline Dependencies

### Dependency Graph

```
Pipeline A (Cashflow)
├── stg_portfolios → fact_cashflow_summary
└── → report_monthly_cashflows

Pipeline B (Trade Analytics, depends on A)
├── stg_trades → fact_trade_summary
└── → fact_portfolio_positions

Pipeline C (Portfolio Analytics, depends on A + B)
├── stg_positions → int_portfolio_returns_daily
└── → fact_portfolio_performance
    ├── joins to fact_trade_summary (from B)
    └── joins to fact_cashflow_summary (from A)
```

### Automatic Dependency Handling

When you run Pipeline C:

```bash
python -m benchmark capture-baseline --pipeline C
```

**Execution sequence:**
1. Run Pipeline A (generate fact_cashflow_summary)
2. Run Pipeline B (depends on A) (generate fact_trade_summary)
3. Run Pipeline C (depends on A + B) (generate fact_portfolio_performance)
4. Save metrics from all three pipelines in baseline_C_*.json

**Key points:**
- Single baseline file contains all three pipelines' metrics
- Comparison evaluates all three pipelines
- Exit code reflects overall health
- Cannot run Pipeline C independently

---

## Troubleshooting Guide

### Issue: "Baseline not found for pipeline X"

**Solution**:
```bash
# Capture baseline first
python -m benchmark capture-baseline --pipeline A

# Verify creation
python -m benchmark list-baselines --pipeline A
```

### Issue: "Output hash mismatch" validation error

**Cause**: Schema or data filter changed

**Solution**:
```bash
# Check recent changes
git diff HEAD~5 models/pipeline_c/marts/fact_portfolio_performance.sql

# If change is intentional, capture new baseline
python -m benchmark capture-baseline --pipeline C

# If unintentional, revert
git checkout models/pipeline_c/marts/fact_portfolio_performance.sql
dbt run --select fact_portfolio_performance
```

### Issue: Snowflake metrics not collecting

**Solution**:
```bash
# Verify credentials
echo $SNOWFLAKE_ACCOUNT

# Test connection
python -c "from benchmark.scripts.metrics_collector import MetricsCollector; 
MetricsCollector()"

# Enable warehouse
ALTER WAREHOUSE COMPUTE_WH RESUME;
```

### Issue: Slow/timeout during execution

**Solution**:
```bash
# Use larger warehouse
SNOWFLAKE_WAREHOUSE=LARGE_WH python -m benchmark run-benchmark --pipeline A

# Check model times
dbt run --select pipeline_a | tail -20
```

### Issue: Exit code doesn't match expected

**Solution**:
```bash
# Check violations
cat benchmark/results/comparison_A_*.json | jq '.violations[]'

# If changes intentional, capture new baseline
python -m benchmark capture-baseline --pipeline A

# If unintentional, revert changes
git checkout models/pipeline_a/
```

---

## CI/CD Integration

### Exit Code Reference

```
0 = PASS          ✓ No regressions, safe to deploy
1 = WARNING       ⚠ Minor regressions, review before merging
2 = ERROR         ✗ Major regressions, block deployment
130 = INTERRUPTED User interruption
```

### GitHub Actions Example

```yaml
name: Performance Benchmark

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  benchmark:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          pip install dbt-snowflake pyyaml
          pip install snowflake-connector-python
      
      - name: Configure Snowflake
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
        run: echo "Configured"
      
      - name: Capture baseline (main only)
        if: github.ref == 'refs/heads/main'
        run: python -m benchmark capture-baseline --pipeline C
      
      - name: Run benchmark (PR only)
        if: github.event_name == 'pull_request'
        run: python -m benchmark run-benchmark --pipeline C
      
      - name: Upload report
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: benchmark-reports
          path: benchmark/results/*.json
```

### GitLab CI Example

```yaml
stages:
  - benchmark
  - report

variables:
  SNOWFLAKE_ACCOUNT: ${CI_SNOWFLAKE_ACCOUNT}
  SNOWFLAKE_USER: ${CI_SNOWFLAKE_USER}
  SNOWFLAKE_PASSWORD: ${CI_SNOWFLAKE_PASSWORD}

benchmark:test:
  stage: benchmark
  image: python:3.10
  script:
    - pip install dbt-snowflake pyyaml snowflake-connector-python
    - python -m benchmark run-benchmark --pipeline C
  artifacts:
    paths:
      - benchmark/results/*.json
  allow_failure: true
  only:
    - merge_requests
    - main

benchmark:capture:
  stage: benchmark
  image: python:3.10
  script:
    - pip install dbt-snowflake pyyaml snowflake-connector-python
    - python -m benchmark capture-baseline --pipeline C
  artifacts:
    paths:
      - benchmark/baselines/*.json
  only:
    - main
    - schedules
```

### Jenkins Pipeline Example

```groovy
pipeline {
    agent any
    
    environment {
        SNOWFLAKE_ACCOUNT = credentials('snowflake-account')
        SNOWFLAKE_USER = credentials('snowflake-user')
        SNOWFLAKE_PASSWORD = credentials('snowflake-password')
    }
    
    stages {
        stage('Setup') {
            steps {
                sh 'pip install dbt-snowflake pyyaml snowflake-connector-python'
            }
        }
        
        stage('Benchmark') {
            steps {
                sh 'python -m benchmark run-benchmark --pipeline C'
            }
            post {
                always {
                    archiveArtifacts artifacts: 'benchmark/results/*.json'
                }
                failure {
                    emailext(
                        subject: 'Benchmark Failed',
                        body: 'Pipeline benchmark test failed.',
                        to: '${DEFAULT_RECIPIENTS}'
                    )
                }
            }
        }
        
        stage('Capture Baseline') {
            when {
                branch 'main'
            }
            steps {
                sh 'python -m benchmark capture-baseline --pipeline C'
            }
        }
    }
}
```

### Parsing Report Output

```bash
# Extract exit code
python -m benchmark run-benchmark --pipeline C
EXIT_CODE=$?
echo "Exit code: $EXIT_CODE"

# Parse JSON report
jq '.metadata.overall_status' benchmark/results/comparison_C_*.json

# Count violations
jq '.metadata.violations_summary' benchmark/results/comparison_C_*.json

# Get slowest models
jq '[.metrics.per_model[] | select(.violations[0]) | 
  {model: .model_name, metric: .violations[0].metric_name}]' \
  benchmark/results/comparison_C_*.json
```

---

## Summary

The dbt Benchmark System provides everything needed to:

✓ **Track performance** across 20+ metrics  
✓ **Detect regressions** with configurable thresholds  
✓ **Manage baselines** for comparison  
✓ **Integrate with CI/CD** using exit codes  
✓ **Optimize models** using concrete metrics  
✓ **Scale to complex pipelines** with dependency handling  

**Get started in 3 commands:**
```bash
python -m benchmark capture-baseline --pipeline A
python -m benchmark run-benchmark --pipeline A
cat benchmark/results/comparison_A_*.json | jq .
```

For more help, consult the Troubleshooting Guide or review the CLI Command Reference above.
