# Task 6: Integration Architecture

## How Report Generator Fits Into the Benchmark System

```
┌─────────────────────────────────────────────────────────────────┐
│  EXECUTION LAYER (Task #2, #3, #4, #5)                          │
│  ┌───────────────┐  ┌─────────────────┐  ┌──────────────────┐   │
│  │ PipelineRunner│  │ MetricsCollector│  │ OutputValidator  │   │
│  │ (dbt_runner)  │  │                 │  │                  │   │
│  └───────────────┘  └─────────────────┘  └──────────────────┘   │
│         ↓                    ↓                      ↓              │
│      Metrics[]        Metrics Dict         Validation Results    │
└──────────────────────────────────────────────────────────────────┘
                              ↓
        ┌──────────────────────────────────────────┐
        │  COMPARISON LAYER (Task #5)              │
        │  ┌──────────────────────────────────────┐│
        │  │ ComparisonEngine                      ││
        │  │ + BaselineManager                    ││
        │  └──────────────────────────────────────┘│
        │         ↓                                 │
        │  Comparison Results (with violations)    │
        └──────────────────────────────────────────┘
                              ↓
        ┌──────────────────────────────────────────┐
        │  REPORTING LAYER (Task #6) ← YOU ARE HERE│
        │  ┌──────────────────────────────────────┐│
        │  │ ReportGenerator                       ││
        │  │ - add_metadata()                     ││
        │  │ - add_metrics()                      ││
        │  │ - add_validation_results()           ││
        │  │ - add_comparison_results()           ││
        │  │ - generate_summary()                 ││
        │  │ - save() / to_json()                 ││
        │  └──────────────────────────────────────┘│
        │         ↓                                 │
        │    JSON Report Output                    │
        └──────────────────────────────────────────┘
                              ↓
        ┌──────────────────────────────────────────┐
        │  PRESENTATION LAYER (Task #7)            │
        │  ┌──────────────────────────────────────┐│
        │  │ CLI Interface                         ││
        │  │ + Dashboard Integration              ││
        │  │ + CI/CD Integration                  ││
        │  └──────────────────────────────────────┘│
        └──────────────────────────────────────────┘
```

## Data Flow Through Report Generator

### Step 1: Collect Execution Data

Multiple components produce data during pipeline execution:

```python
# From PipelineRunner
dbt_run_result = {
    'status': 'success',
    'execution_time_ms': 5000,
    'model_results': [...]
}

# From MetricsCollector  
metrics = {
    'model_a': {
        'execution_time_ms': 1500,
        'bytes_scanned': 1073741824,
        'rows_scanned': 1000000
    }
}

# From OutputValidator
validation_results = {
    'status': 'pass',
    'per_model': {
        'model_a': {
            'status': 'pass',
            'row_count': 1000,
            'hash': 'abc123'
        }
    }
}
```

### Step 2: Perform Comparison (Optional)

If baseline exists, ComparisonEngine provides comparison data:

```python
# From ComparisonEngine
comparison = {
    'status': 'warning',
    'per_model': {
        'model_a': {
            'status': 'warning',
            'metrics': {
                'execution_time_ms': {
                    'baseline_value': 1000,
                    'candidate_value': 1200,
                    'delta': 200,
                    'delta_percent': 20.0
                }
            },
            'violations': [...]
        }
    }
}
```

### Step 3: Generate Comprehensive Report

ReportGenerator combines all data into structured JSON:

```python
from report_generator import ReportGenerator

# Create report
gen = ReportGenerator("A", "Pipeline A")

# Add all data
gen.add_metadata(
    execution_start=start_time,
    execution_end=end_time,
    git_commit=get_git_commit(),
    dbt_version=get_dbt_version(),
    warehouse_name="COMPUTE_WH",
    database_name="DEV_DB"
)

gen.add_metrics(
    per_model_metrics=metrics,
    aggregated_metrics=aggregate_metrics(metrics)
)

gen.add_validation_results(
    overall_status='pass',
    per_model_results=validation_results['per_model']
)

gen.add_comparison_results(
    overall_status=comparison['status'],
    per_model_comparisons=comparison['per_model'],
    violations_summary=count_violations(comparison)
)

gen.generate_summary()

# Save report
report_path = gen.save()
```

### Step 4: Output for Downstream Systems

The generated report serves multiple consumers:

```
report_A_20240115_103000.json
├── Consumed by CLI (Task #7)
│   └─ Display summary, violations, recommendations
├── Consumed by Dashboard
│   └─ Visualize metrics, trends, performance
├── Consumed by CI/CD
│   └─ Fail build if ERROR status
└── Consumed by Email/Slack
    └─ Send alerts and summaries
```

## Integration with Existing Components

### With MetricsCollector

```python
from metrics_collector import MetricsCollector
from report_generator import ReportGenerator

collector = MetricsCollector()
metrics = collector.collect_metrics(query_ids)

# Extract per-model and aggregated metrics
per_model = {}
for model, model_metrics in metrics.items():
    per_model[model] = {
        'execution_time_ms': model_metrics.get('execution_time_ms'),
        'bytes_scanned': model_metrics.get('bytes_scanned'),
        # ... other metrics
    }

aggregated = aggregate_by_pipeline(metrics)

gen = ReportGenerator("A")
gen.add_metrics(per_model, aggregated)
```

### With OutputValidator

```python
from output_validator import OutputValidator
from report_generator import ReportGenerator

validator = OutputValidator()
validation = validator.validate_outputs("A")

gen.add_validation_results(
    overall_status=validation['status'],
    per_model_results=validation['per_model'],
    summary=validation['summary']
)
```

### With ComparisonEngine

```python
from comparison_engine import ComparisonEngine
from report_generator import ReportGenerator

engine = ComparisonEngine()
baseline = load_baseline("baseline_A.json")
candidate = collect_current_metrics()

comparison = engine.compare_pipeline(baseline, candidate)

# Convert violation objects to dicts
per_model_comparisons = {}
for model_name, model_comp in comparison.model_comparisons.items():
    per_model_comparisons[model_name] = {
        'status': model_comp.get_max_severity().name.lower(),
        'violations': [v.get_message() for v in model_comp.violations]
    }

gen.add_comparison_results(
    overall_status=comparison.get_max_severity().name.lower(),
    per_model_comparisons=per_model_comparisons,
    violations_summary=comparison.count_violations_by_severity()
)
```

### With BaselineManager

```python
from baseline_manager import BaselineManager
from report_generator import ReportGenerator

manager = BaselineManager()
baseline_data = manager.retrieve_baseline("A")

if baseline_data:
    gen.add_comparison_results(
        baseline_timestamp=baseline_data['timestamp'],
        baseline_git_commit=baseline_data['git_commit'],
        # ... comparison data
    )
```

## Data Transformation Examples

### Example 1: Metric Formatting

Input → Automatic Formatting:
```
execution_time_ms: 1500     → "1.50 s"
bytes_scanned: 1073741824   → "1.00 GB"
partition_pruning_ratio: 85 → "85.00%"
join_count: 3.0             → "3"
```

### Example 2: Per-Model Aggregation

Input:
```python
metrics = {
    'model_a': {
        'execution_time_ms': 1000,
        'bytes_scanned': 500000000
    },
    'model_b': {
        'execution_time_ms': 2000,
        'bytes_scanned': 1500000000
    }
}
```

Aggregated:
```python
aggregated = {
    'total_execution_time_ms': 3000,
    'total_bytes_scanned': 2000000000,
    'model_count': 2,
    'avg_execution_time_ms': 1500,
    'avg_bytes_scanned': 1000000000
}
```

### Example 3: Violation Extraction

Input (ComparisonEngine violations):
```python
violations = [
    Violation(
        metric_name='execution_time_ms',
        delta=200,
        delta_percent=20.0,
        severity=SeverityLevel.WARNING
    )
]
```

Output (in Report):
```json
{
  "violations": [
    {
      "metric_name": "execution_time_ms",
      "severity": "WARNING",
      "message": "Execution time: baseline=1000, candidate=1200..."
    }
  ]
}
```

## Report Merging for Multi-Pipeline Analysis

When running multiple pipelines, reports can be merged:

```python
from report_generator import merge_reports

# Generate individual reports
report_a = gen_a.report
report_b = gen_b.report
report_c = gen_c.report

# Merge into cross-pipeline summary
merged = merge_reports(
    [report_a, report_b, report_c],
    output_file="benchmark/results/merged_summary.json"
)

# Result
merged = {
    'schema_version': '1.0.0',
    'merge_timestamp': '2024-01-15T10:35:00Z',
    'pipeline_reports': [report_a, report_b, report_c],
    'cross_pipeline_summary': {
        'total_pipelines': 3,
        'overall_status': 'warning',  # Highest priority
        'aggregated_violations': {
            'total': 5,
            'by_severity': {
                'INFO': 1,
                'WARNING': 3,
                'ERROR': 1
            }
        }
    }
}
```

## Error Handling and Edge Cases

### Missing Baseline

If no baseline is available for comparison:

```python
# Simply omit comparison section
gen.add_metadata(...)
gen.add_metrics(...)
gen.add_validation_results(...)  # Include if available
# Skip: gen.add_comparison_results(...)
gen.generate_summary()
gen.save()

# Result: Report with validation but without comparison
```

### Null/Missing Metrics

Null values are preserved in both raw and formatted sections:

```json
{
  "raw": {
    "metric_a": 100,
    "metric_b": null
  },
  "formatted": {
    "metric_a": "100",
    "metric_b": null
  }
}
```

### File Write Failures

Atomic writing prevents partial files:

```
1. Write to temp file (/tmp/tmpXXXXXX.json)
   ✓ Success → Continue
   ✗ Fail → Log error, raise exception (no temp file left)

2. Atomic rename (temp → target)
   ✓ Success → File is complete and readable
   ✗ Fail → Clean up temp, log error, raise exception
```

## Performance Characteristics

- **Report Generation**: ~10-100ms for typical pipelines
- **File Writing**: ~1-10ms per MB of JSON
- **Memory Usage**: ~100KB per 100 metrics
- **Formatting Overhead**: ~2x (formatted strings add ~100% file size)

## Schema Versioning Strategy

Current: `1.0.0`

Future versions may:
- **1.1.0**: Add new optional fields
- **2.0.0**: Change required fields or structure
- **2.1.0**: Add new sections

Consumers should check `schema_version` before processing:

```python
import json

with open('report.json') as f:
    report = json.load(f)

version = report['schema_version']
if version.startswith('1.'):
    # Parse as v1
    process_v1_report(report)
elif version.startswith('2.'):
    # Parse as v2
    process_v2_report(report)
```

## Next Steps (Task #7: CLI Interface)

The JSON reports will be consumed by the CLI interface to:

1. **Display Reports**: Pretty-print summaries to console
2. **Export Formats**: Convert to CSV, HTML, PDF
3. **Integration**: Send to dashboards, CI/CD systems
4. **Alerting**: Trigger notifications on failures
5. **Trending**: Track metrics over time
6. **Comparison**: Compare multiple runs side-by-side

---

**Status**: Report Generator is complete and ready for CLI integration.
