"""
Test Suite for Report Generator

Tests report generation, formatting, validation results, comparison results,
summary generation, and report merging.
"""

import json
import logging
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from report_generator import (
    ReportGenerator,
    MetricFormatter,
    merge_reports,
    ReportStatus
)


# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_metric_formatter_milliseconds():
    """Test millisecond formatting."""
    logger.info("\n=== Test: MetricFormatter - Milliseconds ===")
    
    # Test milliseconds
    assert MetricFormatter.format_milliseconds(500) == "500.00 ms"
    logger.info("✓ Milliseconds: 500 -> 500.00 ms")
    
    # Test seconds
    assert MetricFormatter.format_milliseconds(2500) == "2.50 s"
    logger.info("✓ Seconds: 2500 -> 2.50 s")
    
    # Test minutes
    assert MetricFormatter.format_milliseconds(300000) == "5.00 min"
    logger.info("✓ Minutes: 300000 -> 5.00 min")
    
    # Test None
    assert MetricFormatter.format_milliseconds(None) is None
    logger.info("✓ None: handled correctly")


def test_metric_formatter_bytes():
    """Test byte size formatting."""
    logger.info("\n=== Test: MetricFormatter - Bytes ===")
    
    # Test bytes
    assert MetricFormatter.format_bytes(512) == "512.00 B"
    logger.info("✓ Bytes: 512 -> 512.00 B")
    
    # Test KB
    assert MetricFormatter.format_bytes(2048) == "2.00 KB"
    logger.info("✓ KB: 2048 -> 2.00 KB")
    
    # Test MB
    assert MetricFormatter.format_bytes(5242880) == "5.00 MB"
    logger.info("✓ MB: 5242880 -> 5.00 MB")
    
    # Test GB
    assert MetricFormatter.format_bytes(1073741824) == "1.00 GB"
    logger.info("✓ GB: 1073741824 -> 1.00 GB")
    
    # Test TB
    assert MetricFormatter.format_bytes(1099511627776) == "1.00 TB"
    logger.info("✓ TB: 1099511627776 -> 1.00 TB")


def test_metric_formatter_percentage():
    """Test percentage formatting."""
    logger.info("\n=== Test: MetricFormatter - Percentage ===")
    
    # Test normal percentage
    assert MetricFormatter.format_percentage(15.5) == "15.50%"
    logger.info("✓ Percentage: 15.5 -> 15.50%")
    
    # Test negative percentage
    assert MetricFormatter.format_percentage(-10.2) == "-10.20%"
    logger.info("✓ Negative: -10.2 -> -10.20%")
    
    # Test infinity
    assert MetricFormatter.format_percentage(float('inf')) == "inf %"
    logger.info("✓ Infinity: handled correctly")


def test_metric_formatter_count():
    """Test count formatting."""
    logger.info("\n=== Test: MetricFormatter - Count ===")
    
    # Test integer count
    assert MetricFormatter.format_count(5.0) == "5"
    logger.info("✓ Integer: 5.0 -> 5")
    
    # Test decimal count
    assert MetricFormatter.format_count(5.5) == "5.50"
    logger.info("✓ Decimal: 5.5 -> 5.50")


def test_metric_formatter_metric_name():
    """Test metric formatting by name inference."""
    logger.info("\n=== Test: MetricFormatter - Metric Name Inference ===")
    
    # Time metric
    formatted, mtype = MetricFormatter.format_metric(1500, "execution_time_ms")
    assert mtype == "time"
    logger.info(f"✓ Time metric: {mtype}")
    
    # Byte metric
    formatted, mtype = MetricFormatter.format_metric(1073741824, "bytes_scanned")
    assert mtype == "bytes"
    logger.info(f"✓ Byte metric: {mtype}")
    
    # Percentage metric
    formatted, mtype = MetricFormatter.format_metric(15.5, "partition_pruning_ratio")
    assert mtype == "percentage"
    logger.info(f"✓ Percentage metric: {mtype}")
    
    # Count metric
    formatted, mtype = MetricFormatter.format_metric(3.0, "join_count")
    assert mtype == "count"
    logger.info(f"✓ Count metric: {mtype}")


def test_report_generator_initialization():
    """Test ReportGenerator initialization."""
    logger.info("\n=== Test: ReportGenerator - Initialization ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        gen = ReportGenerator("A", "Test Pipeline", tmpdir)
        
        assert gen.pipeline_id == "A"
        assert gen.pipeline_name == "Test Pipeline"
        assert 'schema_version' in gen.report
        assert 'metadata' in gen.report
        assert 'metrics' in gen.report
        
        logger.info("✓ ReportGenerator initialized correctly")


def test_report_generator_add_metadata():
    """Test adding metadata to report."""
    logger.info("\n=== Test: ReportGenerator - Add Metadata ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        gen = ReportGenerator("A", "Test Pipeline", tmpdir)
        
        start = datetime.utcnow()
        end = start + timedelta(minutes=5)
        
        gen.add_metadata(
            execution_start=start,
            execution_end=end,
            git_commit="abc123def456",
            dbt_version="1.5.0",
            warehouse_name="COMPUTE_WH",
            warehouse_size="L",
            database_name="DEV_DB",
            schema_name="PIPELINE_A",
            environment="dev",
            tags={'team': 'analytics', 'env': 'test'}
        )
        
        metadata = gen.report['metadata']
        assert metadata['pipeline_id'] == "A"
        assert metadata['git_commit'] == "abc123def456"
        assert metadata['dbt_version'] == "1.5.0"
        assert metadata['warehouse']['name'] == "COMPUTE_WH"
        assert metadata['execution_duration_ms'] == 300000
        assert metadata['tags']['team'] == 'analytics'
        
        logger.info("✓ Metadata added correctly")


def test_report_generator_add_metrics():
    """Test adding metrics to report."""
    logger.info("\n=== Test: ReportGenerator - Add Metrics ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        gen = ReportGenerator("A", "Test Pipeline", tmpdir)
        
        per_model = {
            'model_a': {
                'execution_time_ms': 1500,
                'bytes_scanned': 1073741824,
                'rows_scanned': 1000000
            },
            'model_b': {
                'execution_time_ms': 2500,
                'bytes_scanned': 2147483648,
                'rows_scanned': 2000000
            }
        }
        
        aggregated = {
            'total_execution_time_ms': 4000,
            'total_bytes_scanned': 3221225472,
            'model_count': 2
        }
        
        gen.add_metrics(per_model, aggregated)
        
        # Check per-model metrics
        assert 'model_a' in gen.report['metrics']['per_model']
        assert gen.report['metrics']['per_model']['model_a']['raw']['execution_time_ms'] == 1500
        assert gen.report['metrics']['per_model']['model_a']['formatted']['execution_time_ms'] is not None
        
        # Check aggregated metrics
        assert gen.report['metrics']['aggregated']['raw']['total_execution_time_ms'] == 4000
        assert gen.report['metrics']['aggregated']['formatted']['total_execution_time_ms'] is not None
        
        logger.info("✓ Metrics added correctly with formatted values")


def test_report_generator_add_validation_results():
    """Test adding validation results to report."""
    logger.info("\n=== Test: ReportGenerator - Add Validation Results ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        gen = ReportGenerator("A", "Test Pipeline", tmpdir)
        
        per_model_results = {
            'model_a': {
                'model_name': 'model_a',
                'status': 'pass',
                'row_count': 1000,
                'hash': 'abc123',
                'message': 'Output valid'
            },
            'model_b': {
                'model_name': 'model_b',
                'status': 'pass',
                'row_count': 2000,
                'hash': 'def456',
                'message': 'Output valid'
            }
        }
        
        summary = {
            'total_models': 2,
            'models_passed': 2,
            'models_failed': 0,
            'issues': []
        }
        
        gen.add_validation_results('pass', per_model_results, summary)
        
        assert gen.report['validation']['status'] == 'pass'
        assert 'model_a' in gen.report['validation']['per_model']
        assert gen.report['validation']['summary']['models_passed'] == 2
        
        logger.info("✓ Validation results added correctly")


def test_report_generator_add_comparison_results():
    """Test adding comparison results to report."""
    logger.info("\n=== Test: ReportGenerator - Add Comparison Results ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        gen = ReportGenerator("A", "Test Pipeline", tmpdir)
        
        per_model_comparisons = {
            'model_a': {
                'model_name': 'model_a',
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
                },
                'violations': [
                    {
                        'metric_name': 'execution_time_ms',
                        'severity': 'WARNING',
                        'message': 'Execution time exceeded threshold'
                    }
                ]
            }
        }
        
        violations_summary = {
            'total_violations': 1,
            'by_severity': {'INFO': 0, 'WARNING': 1, 'ERROR': 0}
        }
        
        gen.add_comparison_results(
            'warning',
            '2024-01-15T10:30:00Z',
            'baseline_commit_123',
            per_model_comparisons,
            violations_summary=violations_summary
        )
        
        assert gen.report['comparison']['status'] == 'warning'
        assert 'model_a' in gen.report['comparison']['per_model']
        assert gen.report['comparison']['violation_summary']['total_violations'] == 1
        
        logger.info("✓ Comparison results added correctly")


def test_report_generator_generate_summary():
    """Test summary generation."""
    logger.info("\n=== Test: ReportGenerator - Generate Summary ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        gen = ReportGenerator("A", "Test Pipeline", tmpdir)
        
        # Add minimal report sections
        gen.add_metrics(
            aggregated={'total_execution_time_ms': 5000}
        )
        
        gen.add_comparison_results(
            'pass',
            per_model_comparisons={}
        )
        
        gen.generate_summary()
        
        assert 'summary' in gen.report
        assert 'overall_status' in gen.report['summary']
        assert 'performance_overview' in gen.report['summary']
        assert 'top_regressions' in gen.report['summary']
        assert 'notes' in gen.report['summary']
        
        logger.info("✓ Summary generated correctly")


def test_report_generator_to_json():
    """Test JSON conversion and file writing."""
    logger.info("\n=== Test: ReportGenerator - to_json ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        gen = ReportGenerator("A", "Test Pipeline", tmpdir)
        gen.add_metadata(dbt_version="1.5.0")
        gen.add_metrics(aggregated={'test_metric': 100})
        
        # Test JSON string generation
        json_str = gen.to_json(pretty_print=True)
        report_dict = json.loads(json_str)
        
        assert report_dict['schema_version'] == "1.0.0"
        assert report_dict['metadata']['pipeline_id'] == "A"
        
        # Test file writing
        output_file = str(Path(tmpdir) / "test_report.json")
        gen.to_json(pretty_print=True, output_file=output_file)
        
        assert Path(output_file).exists()
        
        with open(output_file) as f:
            saved_report = json.load(f)
        
        assert saved_report['schema_version'] == "1.0.0"
        
        logger.info("✓ JSON conversion and file writing working correctly")


def test_report_generator_save():
    """Test report saving with auto-generated filename."""
    logger.info("\n=== Test: ReportGenerator - Save ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        gen = ReportGenerator("B", "Test Pipeline B", tmpdir)
        gen.add_metadata()
        
        output_path = gen.save()
        
        assert Path(output_path).exists()
        assert "report_B_" in output_path
        assert output_path.endswith(".json")
        
        with open(output_path) as f:
            saved_report = json.load(f)
        
        assert saved_report['metadata']['pipeline_id'] == "B"
        
        logger.info(f"✓ Report saved to {output_path}")


def test_report_generator_atomic_writing():
    """Test atomic file writing (all or nothing)."""
    logger.info("\n=== Test: ReportGenerator - Atomic Writing ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        gen = ReportGenerator("A", "Test Pipeline", tmpdir)
        gen.add_metadata()
        gen.add_metrics(aggregated={'metric1': 100})
        
        output_file = str(Path(tmpdir) / "atomic_test.json")
        gen.to_json(output_file=output_file)
        
        # Verify file is complete (not partial)
        with open(output_file) as f:
            content = f.read()
        
        # Should be valid JSON
        report = json.loads(content)
        assert report['schema_version'] == "1.0.0"
        
        logger.info("✓ Atomic writing verified")


def test_merge_reports():
    """Test merging multiple reports."""
    logger.info("\n=== Test: Merge Reports ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create multiple reports
        reports = []
        
        for pipeline_id in ['A', 'B', 'C']:
            gen = ReportGenerator(pipeline_id, f"Pipeline {pipeline_id}", tmpdir)
            gen.add_metadata()
            gen.add_metrics(aggregated={'metric': 100})
            gen.add_comparison_results(
                'pass',
                violations_summary={'total_violations': 0, 'by_severity': {'INFO': 0, 'WARNING': 0, 'ERROR': 0}}
            )
            gen.generate_summary()
            reports.append(gen.report)
        
        # Merge reports
        merged = merge_reports(reports)
        
        assert merged['cross_pipeline_summary']['total_pipelines'] == 3
        assert len(merged['pipeline_reports']) == 3
        assert 'merge_timestamp' in merged
        
        logger.info("✓ Reports merged correctly")


def test_merge_reports_status_aggregation():
    """Test status aggregation when merging reports."""
    logger.info("\n=== Test: Merge Reports - Status Aggregation ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create reports with different statuses
        reports = []
        
        # Report 1: pass
        gen1 = ReportGenerator("A", "Pipeline A", tmpdir)
        gen1.add_metadata()
        gen1.generate_summary()
        reports.append(gen1.report)
        
        # Report 2: warning
        gen2 = ReportGenerator("B", "Pipeline B", tmpdir)
        gen2.add_metadata()
        gen2.add_comparison_results(
            'warning',
            violations_summary={'total_violations': 1, 'by_severity': {'INFO': 0, 'WARNING': 1, 'ERROR': 0}}
        )
        gen2.generate_summary()
        reports.append(gen2.report)
        
        # Report 3: error
        gen3 = ReportGenerator("C", "Pipeline C", tmpdir)
        gen3.add_metadata()
        gen3.add_comparison_results(
            'error',
            violations_summary={'total_violations': 2, 'by_severity': {'INFO': 0, 'WARNING': 0, 'ERROR': 2}}
        )
        gen3.generate_summary()
        reports.append(gen3.report)
        
        # Merge and verify status
        merged = merge_reports(reports)
        
        # Overall status should be error (highest priority)
        assert merged['cross_pipeline_summary']['overall_status'] == 'error'
        
        # Total violations should be aggregated
        assert merged['cross_pipeline_summary']['aggregated_violations']['total'] == 3
        assert merged['cross_pipeline_summary']['aggregated_violations']['by_severity']['WARNING'] == 1
        assert merged['cross_pipeline_summary']['aggregated_violations']['by_severity']['ERROR'] == 2
        
        logger.info("✓ Status aggregation correct")


def test_merge_reports_file_output():
    """Test saving merged reports to file."""
    logger.info("\n=== Test: Merge Reports - File Output ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create and merge reports
        reports = []
        for pipeline_id in ['A', 'B']:
            gen = ReportGenerator(pipeline_id, f"Pipeline {pipeline_id}", tmpdir)
            gen.add_metadata()
            gen.generate_summary()
            reports.append(gen.report)
        
        output_file = str(Path(tmpdir) / "merged_report.json")
        merged = merge_reports(reports, output_file)
        
        assert Path(output_file).exists()
        
        with open(output_file) as f:
            saved_merged = json.load(f)
        
        assert saved_merged['cross_pipeline_summary']['total_pipelines'] == 2
        
        logger.info("✓ Merged report saved correctly")


def run_all_tests():
    """Run all tests."""
    logger.info("\n" + "="*60)
    logger.info("RUNNING REPORT GENERATOR TEST SUITE")
    logger.info("="*60)
    
    tests = [
        test_metric_formatter_milliseconds,
        test_metric_formatter_bytes,
        test_metric_formatter_percentage,
        test_metric_formatter_count,
        test_metric_formatter_metric_name,
        test_report_generator_initialization,
        test_report_generator_add_metadata,
        test_report_generator_add_metrics,
        test_report_generator_add_validation_results,
        test_report_generator_add_comparison_results,
        test_report_generator_generate_summary,
        test_report_generator_to_json,
        test_report_generator_save,
        test_report_generator_atomic_writing,
        test_merge_reports,
        test_merge_reports_status_aggregation,
        test_merge_reports_file_output,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            logger.error(f"✗ {test.__name__} failed: {str(e)}")
            failed += 1
    
    logger.info("\n" + "="*60)
    logger.info(f"TEST RESULTS: {passed} passed, {failed} failed")
    logger.info("="*60)
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
