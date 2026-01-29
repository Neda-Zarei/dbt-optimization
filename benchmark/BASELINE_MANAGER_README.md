# Baseline Management System

Captures, stores, retrieves, and deletes benchmark baselines for the dbt pipeline benchmarking system.

## Overview

The Baseline Management System provides comprehensive baseline capture and management capabilities for the dbt pipeline benchmarking framework. It orchestrates pipeline execution, metrics collection, and output validation to create complete baseline snapshots for performance comparison and regression detection.

## Architecture

The system is composed of three main modules:

### 1. `storage.py` - File I/O Operations
Handles all file operations with atomic writes and JSON serialization:
- **Atomic writes**: Write to temporary file, then rename to prevent corruption
- **Directory management**: Creates baselines directory if it doesn't exist
- **JSON serialization**: Proper formatting and validation
- **Error handling**: Graceful handling of missing files and corrupted JSON

**Key Classes:**
- `StorageManager`: Manages baseline file I/O operations

### 2. `baseline_manager.py` - Core Baseline Management
Orchestrates baseline capture, storage, and retrieval:
- **Capture**: Pipeline execution, metrics collection, output validation
- **Storage**: JSON files with ISO 8601 timestamps for sortability
- **Retrieval**: Load most recent or specific baseline by timestamp
- **Listing**: Enumerate baselines with summary metadata
- **Deletion**: Delete baselines with safeguards against accidental loss
- **Cleanup**: Automatic cleanup based on retention policies

**Key Classes:**
- `BaselineManager`: Manages baseline lifecycle operations

### 3. `config.yaml` - Configuration
Defines retention policies, storage settings, and capture options:
- **Storage**: Base directory and file naming patterns
- **Retention**: Max age in days and max count per pipeline
- **Capture**: Metrics and validation collection settings
- **Content**: Metadata inclusion options

## File Structure

```
benchmark/
├── baselines/                          # Baseline files directory
│   ├── baseline_A_20240115_143022.json # Example baseline
│   └── baseline_B_20240116_090000.json
├── config/
│   ├── config.yaml                     # Baseline configuration
│   ├── pipelines.yaml                  # Pipeline definitions
│   └── snowflake.yaml                  # Snowflake connection
├── scripts/
│   ├── baseline_manager.py             # Core baseline management
│   ├── storage.py                      # File I/O operations
│   ├── dbt_runner.py                   # Pipeline execution
│   ├── metrics_collector.py            # Metrics collection
│   └── output_validator.py             # Output validation
└── BASELINE_MANAGER_README.md          # This file
```

## Baseline File Format

### Naming Convention
```
baseline_<PIPELINE>_<TIMESTAMP>.json

Example: baseline_A_20240115_143022.json
- Pipeline: A
- Timestamp: 2024-01-15 14:30:22 (ISO 8601 format: YYYYMMDD_HHMMSS)
```

### File Content Structure
```json
{
  "pipeline": "A",
  "captured_at": "20240115_143022",
  "execution_context": {
    "start_time": "20240115_143000",
    "end_time": "20240115_143022",
    "duration_seconds": 22.5,
    "dbt_version": "1.5.0",
    "git_commit": "abc123def456",
    "project_root": ".",
    "dependencies_executed": ["A"],
    "models_executed": ["model_a1", "model_a2"]
  },
  "pipeline_metadata": {
    "name": "Simple Cashflow Pipeline",
    "schema": "pipeline_a",
    "models": "+pipeline_a.*",
    "dependencies": []
  },
  "metrics": {
    "collection_enabled": true,
    "models": {
      "model_a1": {
        "execution_time_ms": 5000,
        "bytes_scanned": 1024000,
        "rows_produced": 1000
      }
    }
  },
  "validation": {
    "validation_enabled": true,
    "schema": "pipeline_a",
    "models": {
      "model_a1": {
        "row_count": 1000,
        "aggregate_hash": "abc123hash"
      }
    }
  },
  "summary": {
    "status": "SUCCESS",
    "errors": []
  }
}
```

## Usage Examples

### Basic Baseline Capture

```python
from baseline_manager import BaselineManager

# Initialize manager
manager = BaselineManager()

# Capture a baseline
baseline_data = manager.capture_baseline(
    pipeline_id="A",
    project_root=".",
    metrics_enabled=True,
    validation_enabled=True
)

# Save the baseline
success, filename = manager.save_baseline(baseline_data)
if success:
    print(f"Baseline saved: {filename}")
```

### Load a Baseline

```python
# Load most recent baseline for pipeline A
baseline = manager.load_baseline("A")

# Load specific baseline by timestamp
baseline = manager.load_baseline("A", timestamp="20240115_143022")
```

### List Baselines

```python
# List all baselines (sorted newest first)
all_baselines = manager.list_baselines()

# List baselines for specific pipeline
pipeline_a_baselines = manager.list_baselines("A")

# Print summary
for summary in pipeline_a_baselines:
    print(f"  {summary['timestamp']}: {summary['status']} ({summary['execution_time']}s)")
```

### Delete a Baseline

```python
# Delete with safeguard (requires confirm=True)
success, msg = manager.delete_baseline(
    "A",
    timestamp="20240115_143022",
    confirm=True
)
```

### Cleanup Old Baselines

```python
# Remove baselines older than 30 days
deleted_count, deleted_files = manager.cleanup_old_baselines(
    max_age_days=30,
    dry_run=False  # Set to True to preview changes
)

# Keep only most recent 5 baselines
deleted_count, deleted_files = manager.cleanup_old_baselines(
    max_count=5,
    dry_run=False
)

# Filter by pipeline
deleted_count, deleted_files = manager.cleanup_old_baselines(
    pipeline_id="A",
    max_age_days=60,
    dry_run=False
)
```

## Configuration

### Retention Policies (`config.yaml`)

```yaml
baseline:
  retention:
    # Maximum age of baselines in days (default: 90)
    max_age_days: 90
    
    # Maximum number of baselines to keep per pipeline (default: 10)
    max_count: 10
    
    # Enable automatic cleanup on startup (default: false)
    cleanup_on_startup: false
```

### Capture Settings

```yaml
baseline:
  capture:
    # Include metrics collection from Snowflake
    include_metrics: true
    
    # Include output validation hashes
    include_validation: true
    
    # Capture detailed execution context
    capture_execution_details: true
```

### Content Settings

```yaml
baseline:
  content:
    # Include dbt version in metadata
    include_dbt_version: true
    
    # Include git commit hash
    include_git_commit: true
    
    # Include full query text (increases file size)
    include_query_text: false
    
    # Compress large baseline files
    compress_large_files: false
    compression_threshold_mb: 10
```

## API Reference

### BaselineManager

#### `capture_baseline(pipeline_id, project_root, metrics_enabled, validation_enabled)`
Captures a complete baseline for a pipeline by orchestrating execution, metrics collection, and validation.

**Parameters:**
- `pipeline_id` (str): Pipeline identifier (A, B, C)
- `project_root` (str, optional): Root directory of dbt project (default: ".")
- `metrics_enabled` (bool, optional): Enable metrics collection (default: True)
- `validation_enabled` (bool, optional): Enable output validation (default: True)

**Returns:** Dictionary with baseline data

#### `save_baseline(baseline_data, force)`
Saves baseline data to JSON file with atomic writes.

**Parameters:**
- `baseline_data` (dict): Baseline data to save
- `force` (bool, optional): Overwrite existing baseline (default: False)

**Returns:** Tuple of (success: bool, filename_or_error: str)

#### `load_baseline(pipeline_id, timestamp)`
Loads a baseline for a pipeline.

**Parameters:**
- `pipeline_id` (str): Pipeline identifier
- `timestamp` (str, optional): Timestamp in format YYYYMMDD_HHMMSS. If not provided, loads most recent.

**Returns:** Baseline data dict or None if not found

#### `list_baselines(pipeline_id)`
Lists all baselines with summary metadata.

**Parameters:**
- `pipeline_id` (str, optional): Filter by pipeline

**Returns:** List of baseline summaries (sorted newest first)

#### `delete_baseline(pipeline_id, timestamp, confirm)`
Deletes a baseline with safeguards.

**Parameters:**
- `pipeline_id` (str): Pipeline identifier
- `timestamp` (str): Timestamp in format YYYYMMDD_HHMMSS
- `confirm` (bool): Must be True to actually delete

**Returns:** Tuple of (success: bool, message: str)

#### `cleanup_old_baselines(pipeline_id, max_age_days, max_count, dry_run)`
Cleans up old baselines based on retention policies.

**Parameters:**
- `pipeline_id` (str, optional): Filter by pipeline
- `max_age_days` (int, optional): Maximum age in days
- `max_count` (int, optional): Maximum number to keep
- `dry_run` (bool, optional): Preview changes without deleting (default: False)

**Returns:** Tuple of (deleted_count: int, deleted_files: List[str])

### StorageManager

#### `save_json(data, file_path)`
Saves data to JSON file with atomic writes.

**Parameters:**
- `data` (dict): Dictionary to serialize
- `file_path` (str): Relative path from base directory

**Returns:** bool - True if successful

#### `load_json(file_path)`
Loads data from JSON file with validation.

**Parameters:**
- `file_path` (str): Relative path from base directory

**Returns:** Loaded dictionary

**Raises:**
- FileNotFoundError: If file doesn't exist
- json.JSONDecodeError: If JSON is corrupted

#### `list_files(pattern)`
Lists files in base directory, optionally filtered by pattern.

**Parameters:**
- `pattern` (str, optional): Glob pattern (e.g., "baseline_A_*.json")

**Returns:** Sorted list of Path objects

#### `delete_file(file_path)`
Deletes a file.

**Parameters:**
- `file_path` (str): Relative path from base directory

**Returns:** bool - True if successful

#### `file_exists(file_path)`
Checks if a file exists.

**Parameters:**
- `file_path` (str): Relative path from base directory

**Returns:** bool

## Error Handling

The system handles various error conditions gracefully:

### Missing Directory
- Creates `benchmark/baselines/` directory if it doesn't exist
- Logs informational message

### Corrupted JSON
- Raises `json.JSONDecodeError` with descriptive message
- Prevents loading of invalid baseline files
- Does not corrupt existing files on failed writes (atomic writes)

### Missing Baselines
- Returns None if baseline not found
- Does not raise exceptions for missing files (unless explicitly deleted)
- Lists empty list if no baselines match filter

### Accidental Deletion
- Requires `confirm=True` parameter to delete
- Requires explicit timestamp (no wildcards)
- Clear error messages for safeguard failures

### File I/O Errors
- Atomic writes prevent corruption if process is interrupted
- Temporary files cleaned up on failure
- Comprehensive logging for debugging

## Testing

A comprehensive test suite is included in `test_baseline_manager.py`:

```bash
cd benchmark/scripts
python test_baseline_manager.py
```

Test coverage includes:
- ✓ Storage operations (save, load, list, delete)
- ✓ Atomic writes and file system reliability
- ✓ Directory creation and validation
- ✓ Corrupted JSON handling
- ✓ Missing file handling
- ✓ Timestamp formatting and parsing
- ✓ Baseline save and load operations
- ✓ Prevention of accidental overwrites
- ✓ Loading most recent baseline
- ✓ Listing baselines with metadata
- ✓ Deletion with safeguards
- ✓ Cleanup by age and count

## Integration with Other Components

### PipelineRunner (dbt_runner.py)
Baseline manager uses PipelineRunner to orchestrate dbt execution:
- Resolves dependencies
- Executes pipelines
- Captures execution metadata

### MetricsCollector (metrics_collector.py)
Optional integration for performance metrics:
- Queries Snowflake QUERY_HISTORY
- Extracts execution time, bytes scanned, credits
- Stores metrics in baseline

### OutputValidator (output_validator.py)
Optional integration for output validation:
- Computes order-agnostic row hashes
- Validates model outputs
- Stores validation hashes in baseline

## Performance Considerations

- **File I/O**: Atomic writes use temporary files in same directory for OS-level atomicity
- **JSON Size**: Baseline files are typically 10-100 KB depending on metrics included
- **Memory**: In-memory loading of complete baseline files (reasonable for typical sizes)
- **Query Performance**: List operations require loading all baseline files (optimize with pagination if needed)

## Future Enhancements

- Compression for large baseline files
- Pagination in list operations
- Baseline diffing and comparison reports
- Baseline versioning and rollback
- Cloud storage support (S3, GCS)
- Incremental baseline updates
- Baseline grouping by release version

## Troubleshooting

### Can't create baselines directory
**Issue**: Permission denied when creating `benchmark/baselines/`
**Solution**: Ensure you have write permissions in the benchmark directory

### Corrupted baseline file
**Issue**: `json.JSONDecodeError` when loading baseline
**Solution**: Delete the corrupted file and recapture the baseline

### Timestamp parsing errors
**Issue**: Invalid timestamp format
**Solution**: Use ISO 8601 format: YYYYMMDD_HHMMSS (e.g., 20240115_143022)

### Metrics collection fails
**Issue**: `MetricsCollector` initialization fails
**Solution**: Verify Snowflake connection credentials in `config/snowflake.yaml`

### Output validation fails
**Issue**: `OutputValidator` initialization fails
**Solution**: Verify Snowflake connection and pipeline configuration

## Related Tasks

- Task #3: Pipeline execution wrapper (dbt_runner.py)
- Task #4: Metrics collection (metrics_collector.py)
- Task #5: Output validation (output_validator.py)
- Task #6: Baseline management (baseline_manager.py)
- Task #7: Comparison engine (upcoming)
