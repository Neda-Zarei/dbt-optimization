# Baseline Management System - Quick Start Guide

## Installation

The baseline management system is already integrated into the benchmark module. No additional installation required beyond the existing dbt and Snowflake setup.

## Basic Usage

### 1. Initialize the Baseline Manager

```python
from scripts.baseline_manager import BaselineManager

# Create manager instance (uses default paths)
manager = BaselineManager()

# Or specify custom paths
manager = BaselineManager(
    base_dir="custom/path/to/baselines",
    config_path="custom/path/to/config.yaml",
    pipelines_config_path="custom/path/to/pipelines.yaml"
)
```

### 2. Capture a Baseline

```python
# Capture baseline for pipeline A
baseline = manager.capture_baseline(
    pipeline_id="A",
    project_root=".",
    metrics_enabled=True,
    validation_enabled=True
)

# Check status
if baseline["summary"]["status"] == "SUCCESS":
    print("Baseline captured successfully")
else:
    print(f"Errors: {baseline['summary']['errors']}")
```

### 3. Save the Baseline

```python
# Save baseline
success, filename = manager.save_baseline(baseline)

if success:
    print(f"Saved: {filename}")
else:
    print(f"Error: {filename}")
```

### 4. Load a Baseline

```python
# Load most recent baseline for pipeline A
baseline = manager.load_baseline("A")

# Load specific baseline by timestamp
baseline = manager.load_baseline("A", timestamp="20240115_143022")

# Access baseline data
print(f"Status: {baseline['summary']['status']}")
print(f"Duration: {baseline['execution_context']['duration_seconds']}s")
print(f"Models: {baseline['execution_context']['models_executed']}")
```

### 5. List Baselines

```python
# List all baselines (newest first)
all_baselines = manager.list_baselines()

# List pipeline-specific baselines
a_baselines = manager.list_baselines("A")

# Print summaries
for summary in a_baselines:
    print(f"{summary['timestamp']}: {summary['status']} ({summary['execution_time']}s)")
```

### 6. Delete a Baseline

```python
# Delete specific baseline (requires confirm=True)
success, msg = manager.delete_baseline(
    pipeline_id="A",
    timestamp="20240115_143022",
    confirm=True
)

if success:
    print(f"Deleted: {msg}")
```

### 7. Cleanup Old Baselines

```python
# Remove baselines older than 30 days
deleted_count, deleted_files = manager.cleanup_old_baselines(
    max_age_days=30
)
print(f"Deleted {deleted_count} old baselines")

# Keep only 5 most recent baselines
deleted_count, deleted_files = manager.cleanup_old_baselines(
    max_count=5
)

# Preview changes without deleting (dry run)
deleted_count, deleted_files = manager.cleanup_old_baselines(
    max_age_days=60,
    dry_run=True
)
```

## Complete Example

```python
from scripts.baseline_manager import BaselineManager
from datetime import datetime

# Initialize
manager = BaselineManager()

# Capture pipeline A baseline
print("Capturing baseline...")
baseline = manager.capture_baseline("A")

if baseline["summary"]["status"] == "SUCCESS":
    # Save the baseline
    success, filename = manager.save_baseline(baseline)
    print(f"✓ Saved: {filename}")
    
    # List recent baselines
    baselines = manager.list_baselines("A")
    print(f"✓ Total baselines for A: {len(baselines)}")
    
    # Get most recent
    latest = manager.load_baseline("A")
    if latest:
        print(f"✓ Latest status: {latest['summary']['status']}")
        print(f"✓ Execution time: {latest['execution_context']['duration_seconds']}s")
    
    # Cleanup old baselines
    deleted, files = manager.cleanup_old_baselines(
        pipeline_id="A",
        max_count=5,
        dry_run=False
    )
    print(f"✓ Cleaned up {deleted} old baselines")
else:
    print(f"✗ Baseline capture failed")
    for error in baseline["summary"]["errors"]:
        print(f"  - {error}")
```

## Baseline File Locations

All baseline files are stored in: `benchmark/baselines/`

Naming pattern: `baseline_<PIPELINE>_<TIMESTAMP>.json`

Examples:
- `baseline_A_20240115_143022.json` - Pipeline A, captured 2024-01-15 14:30:22
- `baseline_B_20240116_090000.json` - Pipeline B, captured 2024-01-16 09:00:00
- `baseline_C_20240117_153045.json` - Pipeline C, captured 2024-01-17 15:30:45

## Configuration

Edit `benchmark/config/config.yaml` to customize:

```yaml
baseline:
  retention:
    max_age_days: 90        # Keep baselines for 90 days
    max_count: 10           # Keep max 10 per pipeline
    cleanup_on_startup: false
  
  capture:
    include_metrics: true
    include_validation: true
    capture_execution_details: true
```

## Common Tasks

### Task: Capture daily baselines for all pipelines

```python
from scripts.baseline_manager import BaselineManager

manager = BaselineManager()

for pipeline_id in ["A", "B", "C"]:
    print(f"Capturing {pipeline_id}...")
    baseline = manager.capture_baseline(pipeline_id)
    success, filename = manager.save_baseline(baseline)
    print(f"  {'✓' if success else '✗'} {filename}")
```

### Task: Find baseline older than 30 days

```python
from datetime import datetime, timedelta

manager = BaselineManager()
cutoff = datetime.now() - timedelta(days=30)

for summary in manager.list_baselines():
    ts = manager._parse_timestamp(summary["timestamp"])
    if ts < cutoff:
        print(f"Old baseline: {summary['filename']}")
```

### Task: Compare two baselines

```python
manager = BaselineManager()

# Load two baselines
baseline1 = manager.load_baseline("A", "20240115_143022")
baseline2 = manager.load_baseline("A", "20240116_090000")

# Compare execution times
time1 = baseline1["execution_context"]["duration_seconds"]
time2 = baseline2["execution_context"]["duration_seconds"]
change = ((time2 - time1) / time1) * 100

print(f"Time change: {change:+.1f}%")
```

### Task: Automatic cleanup on schedule

```python
from scripts.baseline_manager import BaselineManager

def daily_cleanup():
    manager = BaselineManager()
    
    # Remove baselines older than 60 days
    deleted, _ = manager.cleanup_old_baselines(
        max_age_days=60,
        dry_run=False
    )
    
    # Keep only last 5 per pipeline
    for pipeline in ["A", "B", "C"]:
        deleted, _ = manager.cleanup_old_baselines(
            pipeline_id=pipeline,
            max_count=5,
            dry_run=False
        )
    
    print(f"Cleanup completed: {deleted} baselines removed")

# Schedule with APScheduler, cron, or similar
# daily_cleanup()
```

## Testing

Run the test suite to verify everything works:

```bash
cd benchmark/scripts
python test_baseline_manager.py
```

You should see:
```
============================================================
BASELINE MANAGEMENT SYSTEM - TEST SUITE
============================================================
... 17 test cases ...
============================================================
TEST RESULTS: 17 passed, 0 failed
============================================================
```

## Troubleshooting

### Problem: "Permission denied" when creating baselines directory

**Solution**: Ensure you have write permissions in the benchmark directory:
```bash
chmod 755 benchmark
```

### Problem: "No baselines found" when listing

**Solution**: You need to capture and save a baseline first:
```python
baseline = manager.capture_baseline("A")
manager.save_baseline(baseline)
```

### Problem: Corrupted JSON error when loading

**Solution**: The baseline file is corrupted. Delete it and recapture:
```python
# List corrupted file
summaries = manager.list_baselines()

# Delete it
success, _ = manager.delete_baseline("A", "20240115_143022", confirm=True)

# Recapture
baseline = manager.capture_baseline("A")
manager.save_baseline(baseline)
```

### Problem: Snowflake connection error during capture

**Solution**: Verify Snowflake credentials in `benchmark/config/snowflake.yaml`:
```yaml
snowflake:
  account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
  user: "{{ env_var('SNOWFLAKE_USER') }}"
  password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
```

Set environment variables:
```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
```

### Problem: Accidental deletion prevented

**Symptom**: Delete operation returns error: "Deletion requires confirm=True"

**This is intentional!** The system requires explicit confirmation to prevent accidents:
```python
# Won't work:
manager.delete_baseline("A", "20240115_143022")

# Must use:
manager.delete_baseline("A", "20240115_143022", confirm=True)
```

## Next Steps

1. **Setup Integration**: Integrate with your CI/CD pipeline to capture baselines daily
2. **Configure Retention**: Set `max_age_days` and `max_count` in `config.yaml`
3. **Enable Cleanup**: Set `cleanup_on_startup: true` for automatic cleanup
4. **Build Comparison**: Use baselines with comparison engine (Task #7) for regression detection
5. **Monitor Performance**: Track baseline metrics over time to detect trends

## Related Documentation

- **Full Documentation**: See `BASELINE_MANAGER_README.md`
- **API Reference**: See `BASELINE_MANAGER_README.md` > API Reference
- **Configuration**: See `benchmark/config/config.yaml`
- **Test Suite**: See `benchmark/scripts/test_baseline_manager.py`

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review `BASELINE_MANAGER_README.md` for detailed documentation
3. Check test cases in `test_baseline_manager.py` for usage examples
4. Review your configuration in `benchmark/config/config.yaml`
