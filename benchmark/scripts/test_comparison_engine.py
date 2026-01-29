"""
Test Suite for Comparison Engine

Tests threshold configuration, metric comparison, delta calculation,
severity classification, and report generation.
"""

import json
import logging
import tempfile
import yaml
from pathlib import Path
from typing import Dict, Any

from thresholds import (
    ThresholdManager, MetricsComparer, SeverityLevel,
    Violation
)
from comparison_engine import (
    ComparisonEngine, PipelineComparison, ModelComparison,
    ComparisonStatus
)


# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_test_thresholds() -> Dict[str, Any]:
    """Create a test threshold configuration."""
    return {
        'thresholds': {
            'execution_time_ms': {
                'max_increase_percent': 10,
                'severity': 'high'
            },
            'bytes_scanned': {
                'max_increase_percent': 20,
                'severity': 'high'
            },
            'warehouse_credits': {
                'max_increase_percent': 15,
                'severity': 'critical'
            },
            'spilling_bytes': {
                'max_increase_absolute': 1073741824,
                'severity': 'high'
            }
        }
    }


def test_threshold_manager_loading():
    """Test loading threshold configuration."""
    logger.info("\n=== Test: ThresholdManager - Configuration Loading ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test config
        config_path = Path(tmpdir) / "thresholds.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(create_test_thresholds(), f)
        
        # Load and verify
        manager = ThresholdManager(str(config_path))
        
        assert manager.is_metric_configured('execution_time_ms')
        assert manager.is_metric_configured('bytes_scanned')
        assert not manager.is_metric_configured('nonexistent_metric')
        
        threshold = manager.get_threshold('execution_time_ms')
        assert threshold['max_increase_percent'] == 10
        assert threshold['severity'] == 'high'
        
        logger.info("✓ Threshold configuration loaded successfully")


def test_metrics_comparer_delta_calculation():
    """Test delta calculation for various scenarios."""
    logger.info("\n=== Test: MetricsComparer - Delta Calculation ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "thresholds.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(create_test_thresholds(), f)
        
        threshold_manager = ThresholdManager(str(config_path))
        comparer = MetricsComparer(threshold_manager)
        
        # Test normal regression
        delta, delta_pct = comparer.calculate_delta(100, 110)
        assert delta == 10
        assert delta_pct == 10.0
        logger.info(f"✓ Normal regression: delta={delta}, delta%={delta_pct}")
        
        # Test improvement
        delta, delta_pct = comparer.calculate_delta(100, 90)
        assert delta == -10
        assert delta_pct == -10.0
        logger.info(f"✓ Improvement: delta={delta}, delta%={delta_pct}")
        
        # Test zero baseline
        delta, delta_pct = comparer.calculate_delta(0, 100)
        assert delta == 100
        assert delta_pct == float('inf')
        logger.info(f"✓ Zero baseline: delta={delta}, delta%={delta_pct}")
        
        # Test both zero
        delta, delta_pct = comparer.calculate_delta(0, 0)
        assert delta == 0
        assert delta_pct == 0.0
        logger.info(f"✓ Both zero: delta={delta}, delta%={delta_pct}")
        
        # Test None values
        delta, delta_pct = comparer.calculate_delta(None, 100)
        assert delta is None and delta_pct is None
        logger.info(f"✓ None baseline: delta={delta}, delta%={delta_pct}")


def test_metrics_comparer_threshold_evaluation():
    """Test threshold evaluation and violation detection."""
    logger.info("\n=== Test: MetricsComparer - Threshold Evaluation ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "thresholds.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(create_test_thresholds(), f)
        
        threshold_manager = ThresholdManager(str(config_path))
        comparer = MetricsComparer(threshold_manager)
        
        # Test violation (exceeds threshold)
        violation = comparer.evaluate_threshold(
            'execution_time_ms',
            baseline_value=100,
            candidate_value=115  # 15% increase, exceeds 10% threshold
        )
        assert violation is not None
        assert violation.severity == SeverityLevel.WARNING
        logger.info(f"✓ Violation detected: {violation.get_message()}")
        
        # Test no violation (within threshold)
        violation = comparer.evaluate_threshold(
            'execution_time_ms',
            baseline_value=100,
            candidate_value=108  # 8% increase, within 10% threshold
        )
        assert violation is None
        logger.info("✓ No violation within threshold")
        
        # Test improvement with ignore_improvements=False
        violation = comparer.evaluate_threshold(
            'execution_time_ms',
            baseline_value=100,
            candidate_value=85,  # 15% improvement
            ignore_improvements=False
        )
        assert violation is not None
        assert violation.severity == SeverityLevel.INFO
        assert violation.is_improvement
        logger.info(f"✓ Improvement detected: {violation.get_message()}")
        
        # Test improvement with ignore_improvements=True
        violation = comparer.evaluate_threshold(
            'execution_time_ms',
            baseline_value=100,
            candidate_value=85,
            ignore_improvements=True
        )
        assert violation is None
        logger.info("✓ Improvement ignored when configured")


def test_metrics_comparer_severity_classification():
    """Test severity classification based on violation magnitude."""
    logger.info("\n=== Test: MetricsComparer - Severity Classification ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "thresholds.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(create_test_thresholds(), f)
        
        threshold_manager = ThresholdManager(str(config_path))
        comparer = MetricsComparer(threshold_manager)
        
        # Critical metric (warehouse_credits)
        violation = comparer.evaluate_threshold(
            'warehouse_credits',
            baseline_value=100,
            candidate_value=120  # 20% increase
        )
        assert violation is not None
        assert violation.severity == SeverityLevel.ERROR
        logger.info(f"✓ Critical metric violation: {violation.severity.name}")
        
        # High metric with small violation
        violation = comparer.evaluate_threshold(
            'execution_time_ms',
            baseline_value=100,
            candidate_value=112  # 12% increase, small violation
        )
        assert violation is not None
        assert violation.severity == SeverityLevel.WARNING
        logger.info(f"✓ High metric minor violation: {violation.severity.name}")
        
        # High metric with large violation
        violation = comparer.evaluate_threshold(
            'execution_time_ms',
            baseline_value=100,
            candidate_value=120  # 20% increase, large violation
        )
        assert violation is not None
        assert violation.severity == SeverityLevel.ERROR
        logger.info(f"✓ High metric major violation: {violation.severity.name}")


def test_metrics_comparer_absolute_thresholds():
    """Test absolute (non-percentage) threshold evaluation."""
    logger.info("\n=== Test: MetricsComparer - Absolute Thresholds ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "thresholds.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(create_test_thresholds(), f)
        
        threshold_manager = ThresholdManager(str(config_path))
        comparer = MetricsComparer(threshold_manager)
        
        # Test absolute threshold violation
        violation = comparer.evaluate_threshold(
            'spilling_bytes',
            baseline_value=0,
            candidate_value=2073741824  # 2GB, exceeds 1GB threshold
        )
        assert violation is not None
        assert violation.threshold_type == 'absolute'
        logger.info(f"✓ Absolute threshold violation: {violation.get_message()}")
        
        # Test within absolute threshold
        violation = comparer.evaluate_threshold(
            'spilling_bytes',
            baseline_value=0,
            candidate_value=536870912  # 512MB, within 1GB threshold
        )
        assert violation is None
        logger.info("✓ Within absolute threshold")


def test_comparison_engine_pipeline_comparison():
    """Test full pipeline comparison."""
    logger.info("\n=== Test: ComparisonEngine - Pipeline Comparison ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test configuration
        config_path = Path(tmpdir) / "thresholds.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(create_test_thresholds(), f)
        
        engine = ComparisonEngine(str(config_path))
        
        # Create baseline and candidate data
        baseline = {
            'pipeline': 'test_pipeline',
            'captured_at': '20240115_100000',
            'per_model': {
                'model_a': {
                    'model_name': 'model_a',
                    'metrics': {
                        'execution_time_ms': 100,
                        'bytes_scanned': 1000
                    }
                }
            },
            'pipeline_aggregations': {
                'total_execution_time_ms': 100,
                'total_bytes_scanned': 1000
            }
        }
        
        candidate = {
            'pipeline': 'test_pipeline',
            'captured_at': '20240115_110000',
            'per_model': {
                'model_a': {
                    'model_name': 'model_a',
                    'metrics': {
                        'execution_time_ms': 108,  # 8% increase
                        'bytes_scanned': 1150  # 15% increase
                    }
                }
            },
            'pipeline_aggregations': {
                'total_execution_time_ms': 108,
                'total_bytes_scanned': 1150
            }
        }
        
        # Compare pipelines
        comparison = engine.compare_pipeline(baseline, candidate)
        
        assert comparison.pipeline_name == 'test_pipeline'
        assert len(comparison.model_comparisons) == 1
        assert 'model_a' in comparison.model_comparisons
        
        model_comp = comparison.model_comparisons['model_a']
        assert len(model_comp.violations) == 1  # bytes_scanned exceeds threshold
        
        logger.info(f"✓ Pipeline comparison complete: {len(model_comp.violations)} violations")


def test_comparison_engine_exit_codes():
    """Test exit code generation based on severity."""
    logger.info("\n=== Test: ComparisonEngine - Exit Codes ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "thresholds.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(create_test_thresholds(), f)
        
        engine = ComparisonEngine(str(config_path))
        
        # Test PASS (no violations)
        comparison = PipelineComparison(
            pipeline_name='test',
            baseline_timestamp='20240115_100000',
            candidate_timestamp='20240115_110000'
        )
        status, exit_code = engine.generate_summary(comparison)
        assert status == ComparisonStatus.PASS
        assert exit_code == 0
        logger.info("✓ PASS exit code: 0")
        
        # Test WARNING (INFO/WARNING violations)
        violation = Violation(
            metric_name='test',
            baseline_value=100,
            candidate_value=112,
            delta=12,
            delta_percent=12.0,
            threshold=10,
            threshold_type='percent',
            severity=SeverityLevel.WARNING
        )
        comparison.pipeline_violations = [violation]
        status, exit_code = engine.generate_summary(comparison)
        assert status == ComparisonStatus.WARNING
        assert exit_code == 1
        logger.info("✓ WARNING exit code: 1")
        
        # Test ERROR (ERROR violations)
        violation.severity = SeverityLevel.ERROR
        status, exit_code = engine.generate_summary(comparison)
        assert status == ComparisonStatus.ERROR
        assert exit_code == 2
        logger.info("✓ ERROR exit code: 2")


def test_comparison_engine_report_generation():
    """Test report generation."""
    logger.info("\n=== Test: ComparisonEngine - Report Generation ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "thresholds.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(create_test_thresholds(), f)
        
        engine = ComparisonEngine(str(config_path))
        
        # Create comparison with violations
        violation = Violation(
            metric_name='execution_time_ms',
            baseline_value=100,
            candidate_value=115,
            delta=15,
            delta_percent=15.0,
            threshold=10,
            threshold_type='percent',
            severity=SeverityLevel.WARNING
        )
        
        model_comp = ModelComparison(
            model_name='model_a',
            baseline_metrics={'execution_time_ms': 100},
            candidate_metrics={'execution_time_ms': 115},
            violations=[violation]
        )
        
        comparison = PipelineComparison(
            pipeline_name='test',
            baseline_timestamp='20240115_100000',
            candidate_timestamp='20240115_110000',
            model_comparisons={'model_a': model_comp}
        )
        
        # Generate text report
        report = engine.format_report(comparison)
        assert 'test' in report
        assert 'model_a' in report
        assert 'execution_time_ms' in report
        assert '⚠' in report or 'WARNING' in report
        logger.info("✓ Text report generated successfully")
        
        # Generate JSON report
        json_report = engine.generate_json_report(comparison)
        assert json_report['pipeline'] == 'test'
        assert json_report['status'] == 'WARNING'
        assert json_report['exit_code'] == 1
        assert json_report['summary']['warning'] == 1
        logger.info("✓ JSON report generated successfully")


def test_comparison_engine_missing_metrics():
    """Test handling of missing metrics in baseline or candidate."""
    logger.info("\n=== Test: ComparisonEngine - Missing Metrics ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "thresholds.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(create_test_thresholds(), f)
        
        engine = ComparisonEngine(str(config_path))
        
        baseline_metrics = {
            'execution_time_ms': 100,
            'bytes_scanned': 1000
        }
        
        candidate_metrics = {
            'execution_time_ms': 105,
            'bytes_scanned': 1200,
            'new_metric': 42  # New metric in candidate
        }
        
        baseline = {
            'pipeline': 'test',
            'captured_at': '20240115_100000',
            'per_model': {
                'model_a': {
                    'model_name': 'model_a',
                    'metrics': baseline_metrics
                }
            },
            'pipeline_aggregations': {}
        }
        
        candidate = {
            'pipeline': 'test',
            'captured_at': '20240115_110000',
            'per_model': {
                'model_a': {
                    'model_name': 'model_a',
                    'metrics': candidate_metrics
                }
            },
            'pipeline_aggregations': {}
        }
        
        comparison = engine.compare_pipeline(baseline, candidate)
        model_comp = comparison.model_comparisons['model_a']
        
        assert 'new_metric' in model_comp.metrics_missing_in_baseline
        logger.info(f"✓ Missing metric detected: {model_comp.metrics_missing_in_baseline}")


def run_all_tests():
    """Run all tests."""
    logger.info("\n" + "=" * 80)
    logger.info("RUNNING COMPARISON ENGINE TEST SUITE")
    logger.info("=" * 80)
    
    tests = [
        test_threshold_manager_loading,
        test_metrics_comparer_delta_calculation,
        test_metrics_comparer_threshold_evaluation,
        test_metrics_comparer_severity_classification,
        test_metrics_comparer_absolute_thresholds,
        test_comparison_engine_pipeline_comparison,
        test_comparison_engine_exit_codes,
        test_comparison_engine_report_generation,
        test_comparison_engine_missing_metrics
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            logger.error(f"✗ Test failed: {str(e)}")
            failed += 1
    
    logger.info("\n" + "=" * 80)
    logger.info(f"TEST SUMMARY: {passed} passed, {failed} failed")
    logger.info("=" * 80)
    
    return failed == 0


if __name__ == '__main__':
    import sys
    success = run_all_tests()
    sys.exit(0 if success else 1)
