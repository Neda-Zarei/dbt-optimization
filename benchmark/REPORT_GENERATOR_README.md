# JSON Report Generator

Comprehensive JSON report generation for dbt benchmark results, combining metrics, validation results, and comparison data into a structured, machine-readable format.

## Overview

The Report Generator creates detailed JSON reports that capture:

1. **Metadata**: Execution context (timestamps, git info, warehouse details)
2. **Metrics**: Performance KPIs (per-model and pipeline-level aggregations)
3. **Validation**: Output correctness verification (hash comparison, row counts)
4. **Comparison**: Baseline performance comparison with delta calculations
5. **Summary**: High-level performance analysis with regressions and improvements

## Architecture

### Core Components

```
ReportGenerator
├── MetricFormatter (utility for human-readable values)
├── Report Structure
│   ├── metadata
│   ├── metrics (per_model + aggregated)
│   ├── validation (optional)
│   ├── comparison (optional)
│   └── summary
└── File I/O (atomic writing, merging)
```

### Design Principles

- **Incremental Building**: Add sections as they become available during execution
- **Atomic Writing**: Complete files only (no partial writes on failure)
- **Schema Versioning**: Supports future compatibility with version tracking
- **Extensible**: Support for custom metrics without breaking consumers
- **Machine & Human Readable**: Both raw values and formatted strings included

## Usage

### Basic Report Generation

```python
from report_generator import ReportGenerator
from datetime import datetime

# Create report
gen = ReportGenerator(
    pipeline_id="A",
    pipeline_name="Simple Cashflow Pipeline",
    output_directory="benchmark/results"
)

# Add metadata
gen.add_metadata(
    execution_start=datetime.utcnow(),
    execution_end=datetime.utcnow(),
    git_commit="abc123def456",
    dbt_version="1.5.0",
    warehouse_name="COMPUTE_WH",
    database_name="DEV_DB"
)

# Add metrics
gen.add_metrics(
    per_model_metrics={
        'model_a': {
            'execution_time_ms': 1500,
            'bytes_scanned': 1073741824,
            'rows_scanned': 1000000
        }
    },
    aggregated_metrics={
        'total_execution_time_ms': 5000,
        'model_count': 2
    }
)

# Add validation results (optional)
gen.add_validation_results(
    overall_status='pass',
    per_model_results={
        'model_a': {
            'status': 'pass',
            'row_count': 1000,
            'hash': 'abc123xyz'
        }
    }
)

# Add comparison results (optional)
gen.add_comparison_results(
    overall_status='warning',
    baseline_timestamp='2024-01-15T10:30:00Z',
    per_model_comparisons={
        'model_a': {
            'status': 'warning',
            'metrics': {
                'execution_time_ms': {
                    'baseline_value': 1000,
                    'candidate_value': 1200,
                    'delta': 200,
                    'delta_percent': 20.0,
                    'threshold': 10,
                    'threshold_type': 'percent',
                    'status': 'warning'
                }
            }
        }
    }
)

# Generate summary (auto-calculated)
gen.generate_summary()

# Save report
output_path = gen.save()
print(f"Report saved to: {output_path}")

# Or save with custom filename
output_path = gen.save(filename="custom_report.json")

# Or get JSON string without saving
json_string = gen.to_json(pretty_print=True)
```

### Metric Formatting

The report generator automatically formats metrics based on their type:

```python
from report_generator import MetricFormatter

# Format specific types
ms_string = MetricFormatter.format_milliseconds(1500)  # "1.50 s"
bytes_string = MetricFormatter.format_bytes(1073741824)  # "1.00 GB"
pct_string = MetricFormatter.format_percentage(15.5)  # "15.50%"
count_string = MetricFormatter.format_count(3.0)  # "3"

# Auto-format by metric name
formatted, metric_type = MetricFormatter.format_metric(1500, "execution_time_ms")
# formatted = "1.50 s", metric_type = "time"
```

### Report Merging

Combine multiple pipeline reports into a cross-pipeline summary:

```python
from report_generator import merge_reports
import json

# Load individual reports
with open('report_A_20240115_103000.json') as f:
    report_a = json.load(f)

with open('report_B_20240115_103000.json') as f:
    report_b = json.load(f)

# Merge with summary
merged = merge_reports(
    [report_a, report_b],
    output_file='benchmark/results/merged_report.json'
)

print(f"Overall Status: {merged['cross_pipeline_summary']['overall_status']}")
print(f"Total Violations: {merged['cross_pipeline_summary']['aggregated_violations']['total']}")
```

## Report Structure

### Metadata Section

Contains execution context and system information:

```json
{
  "metadata": {
    "pipeline_id": "A",
    "pipeline_name": "Simple Cashflow Pipeline",
    "timestamp": "2024-01-15T10:35:00Z",
    "execution_start": "2024-01-15T10:30:00Z",
    "execution_end": "2024-01-15T10:35:00Z",
    "execution_duration_ms": 300000,
    "git_commit": "abc123def456",
    "git_branch": "feature/optimization",
    "dbt_version": "1.5.0",
    "warehouse": {
      "name": "COMPUTE_WH",
      "size": "L",
      "region": "us-west-2",
      "account": "xy12345"
    },
    "database": {
      "name": "DEV_DB",
      "schema": "PIPELINE_A"
    },
    "environment": "dev",
    "tags": {
      "team": "analytics",
      "env": "test"
    }
  }
}
```

### Metrics Section

Per-model and aggregated metrics with both raw and formatted values:

```json
{
  "metrics": {
    "per_model": {
      "model_a": {
        "model_name": "model_a",
        "raw": {
          "execution_time_ms": 1500,
          "bytes_scanned": 1073741824,
          "rows_scanned": 1000000
        },
        "formatted": {
          "execution_time_ms": "1.50 s",
          "bytes_scanned": "1.00 GB",
          "rows_scanned": "1000000"
        }
      }
    },
    "aggregated": {
      "raw": {
        "total_execution_time_ms": 5000,
        "total_bytes_scanned": 3221225472,
        "model_count": 2
      },
      "formatted": {
        "total_execution_time_ms": "5.00 s",
        "total_bytes_scanned": "3.00 GB",
        "model_count": "2"
      }
    }
  }
}
```

### Validation Section (Optional)

Output validation with hash comparison and row counts:

```json
{
  "validation": {
    "status": "pass",
    "timestamp": "2024-01-15T10:35:00Z",
    "per_model": {
      "model_a": {
        "model_name": "model_a",
        "status": "pass",
        "row_count": 1000,
        "hash": "abc123xyz789",
        "message": "Output matches baseline hash"
      }
    },
    "summary": {
      "total_models": 1,
      "models_passed": 1,
      "models_failed": 0,
      "issues": []
    }
  }
}
```

### Comparison Section (Optional)

Baseline comparison with deltas, percentages, and violations:

```json
{
  "comparison": {
    "status": "warning",
    "baseline_timestamp": "2024-01-14T10:30:00Z",
    "baseline_git_commit": "baseline123",
    "per_model": {
      "model_a": {
        "model_name": "model_a",
        "status": "warning",
        "metrics": {
          "execution_time_ms": {
            "baseline_value": 1000,
            "candidate_value": 1200,
            "delta": 200,
            "delta_percent": 20.0,
            "threshold": 10,
            "threshold_type": "percent",
            "status": "warning",
            "message": "Execution time increased by 20%, threshold is 10%"
          }
        },
        "violations": [
          {
            "metric_name": "execution_time_ms",
            "severity": "WARNING",
            "message": "Execution time: baseline=1000, candidate=1200, delta=200.00 (20.00%), threshold=10%"
          }
        ]
      }
    },
    "violation_summary": {
      "total_violations": 1,
      "by_severity": {
        "INFO": 0,
        "WARNING": 1,
        "ERROR": 0
      }
    }
  }
}
```

### Summary Section

Auto-generated performance analysis:

```json
{
  "summary": {
    "overall_status": "warning",
    "performance_overview": {
      "total_execution_time_ms": {
        "value": 5000,
        "formatted": "5.00 s",
        "percentile": "baseline"
      }
    },
    "top_regressions": [
      {
        "model": "model_a",
        "metric": "execution_time_ms",
        "delta_percent": 20.0,
        "severity": "WARNING"
      }
    ],
    "top_improvements": [
      {
        "model": "model_b",
        "metric": "bytes_scanned",
        "delta_percent": 5.0
      }
    ],
    "notes": [
      "WARNING: 1 error-level violation(s) detected"
    ]
  }
}
```

## Metric Formatting Rules

The report generator automatically formats metrics based on their names:

| Pattern | Format | Example |
|---------|--------|---------|
| `*_ms`, `*_time` | Milliseconds → Time | 1500 ms → 1.50 s |
| `*_bytes`, `spilling`, `scanned`, `memory` | Bytes → Size | 1073741824 → 1.00 GB |
| `*percent*`, `*ratio` | Percentage | 15.5 → 15.50% |
| `*_count`, `*_number`, `depth`, `partitions` | Count | 3.0 → 3 |
| Default | As-is | 100 → 100 |

## JSON Schema

The report conforms to the JSON Schema defined in `benchmark/schemas/report_schema.json`. This schema:

- Validates report structure
- Documents all fields with descriptions
- Specifies required vs optional sections
- Enables IDE autocompletion
- Supports schema evolution with versioning

## Integration Points

### With Comparison Engine (Task #5)

Consume comparison results and violations:

```python
from comparison_engine import ComparisonEngine

engine = ComparisonEngine()
comparison = engine.compare_pipeline(baseline, candidate)

gen.add_comparison_results(
    overall_status=comparison.get_max_severity().name.lower(),
    per_model_comparisons={
        model_name: {
            'violations': [v.get_message() for v in model_comp.violations],
            'metrics': {...}
        }
        for model_name, model_comp in comparison.model_comparisons.items()
    }
)
```

### With Metrics Collector (Task #3)

Include collected metrics:

```python
from metrics_collector import MetricsCollector

collector = MetricsCollector()
metrics = collector.collect_metrics(query_ids)

per_model_metrics = {
    model: {
        'execution_time_ms': metrics[model]['execution_time_ms'],
        'bytes_scanned': metrics[model]['bytes_scanned'],
        # ... other metrics
    }
    for model in models
}

gen.add_metrics(per_model_metrics=per_model_metrics)
```

### With Output Validator (Task #4)

Include validation results:

```python
from output_validator import OutputValidator

validator = OutputValidator()
validation_results = validator.validate_outputs(pipeline_id)

gen.add_validation_results(
    overall_status=validation_results['status'],
    per_model_results=validation_results['per_model']
)
```

## File Organization

```
benchmark/
├── results/                    # Generated report directory
│   ├── report_A_20240115_103000.json
│   ├── report_B_20240115_103000.json
│   ├── report_C_20240115_103000.json
│   └── merged_report.json
├── schemas/
│   └── report_schema.json      # JSON Schema definition
└── scripts/
    ├── report_generator.py     # Main implementation
    └── test_report_generator.py # Test suite
```

## Advanced Features

### Atomic Writing

Reports are written atomically using temporary files:

1. Write to temporary file
2. Validate write succeeded
3. Atomically rename to target
4. Clean up on failure

This ensures reports are never partial or corrupted.

### Incremental Report Building

Sections can be added in any order:

```python
gen = ReportGenerator("A")
gen.add_metrics(...)           # Add metrics first
gen.add_metadata(...)          # Add metadata second
gen.add_validation_results()   # Add validation
gen.add_comparison_results()   # Add comparison
gen.generate_summary()         # Auto-calculate summary
gen.save()                     # Save to file
```

### Schema Versioning

Reports include schema version for compatibility:

```json
{
  "schema_version": "1.0.0"
}
```

This allows:
- Version checking before processing
- Migration logic for older versions
- Documentation of breaking changes

### Custom Tags

Add custom tags for filtering and searching:

```python
gen.add_metadata(
    tags={
        'team': 'analytics',
        'environment': 'test',
        'feature': 'query-optimization'
    }
)
```

## Error Handling

The report generator handles various error scenarios:

- Missing values (None) → Included as null in JSON
- Infinite deltas → Formatted as "inf %" or "-inf %"
- File write failures → Rolls back temp file, logs error
- Invalid metric names → Uses default formatting

## Performance Considerations

- **Memory**: Reports can grow large with many models/metrics
- **File Size**: JSON formatting adds ~2x overhead (use compression for large reports)
- **Validation**: Schema validation can be performed post-generation

## Troubleshooting

### Report Missing Comparison Section

- **Cause**: `add_comparison_results()` not called
- **Solution**: Call when baseline comparison is available

### Formatted Values are `null`

- **Cause**: Raw value is None
- **Solution**: Verify metrics are properly populated

### File Not Written

- **Cause**: Permission issue or disk full
- **Solution**: Check output directory permissions and disk space

### Schema Validation Fails

- **Cause**: Missing required fields
- **Solution**: Ensure `schema_version`, `metadata`, and `metrics` are present

## Examples

See `test_report_generator.py` for complete working examples including:
- Basic report generation
- Metric formatting
- Validation result handling
- Comparison result integration
- Report merging
- File operations

## Future Enhancements

Potential improvements for future versions:

1. **Schema Validation**: Add JSON Schema validation at write time
2. **Report Compression**: Support gzip compression for large reports
3. **Multiple Baselines**: Support comparison against multiple baselines
4. **Custom Formatters**: Allow user-defined metric formatting rules
5. **Report Rendering**: HTML/PDF rendering from JSON reports
6. **Metrics Validation**: Schema for metric types and units
7. **Incremental Updates**: Update reports without full regeneration

## Related Documentation

- [JSON Schema Definition](schemas/report_schema.json)
- [Comparison Engine](COMPARISON_ENGINE_README.md)
- [Baseline Manager](BASELINE_MANAGER_README.md)
- [Metrics Collector](scripts/metrics_collector.py)
- [Output Validator](scripts/output_validator.py)
