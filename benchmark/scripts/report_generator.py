"""
Benchmark Report Generator

Generates comprehensive JSON reports of benchmark results combining metrics,
validation results, and comparison data.

Features:
- Structured JSON reports with schema validation
- Per-model and aggregated metrics with raw and formatted values
- Validation results with hash comparison and row counts
- Comparison results against baseline with delta calculations and violations
- Performance summary with top regressions and improvements
- Atomic file writing to ensure complete reports only
- Report merging for cross-pipeline analysis
- Support for incremental report building
"""

import json
import logging
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
import os


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReportStatus(Enum):
    """Overall report status."""
    PASS = "pass"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class FormattedMetric:
    """Metric with both raw and formatted values."""
    raw_value: Optional[float] = None
    formatted_value: Optional[str] = None
    metric_type: str = "generic"  # generic, time, bytes, percentage, count
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'raw': self.raw_value,
            'formatted': self.formatted_value,
            'type': self.metric_type
        }


class MetricFormatter:
    """Utility class for formatting metric values to human-readable strings."""
    
    @staticmethod
    def format_milliseconds(value: Optional[float]) -> Optional[str]:
        """Format milliseconds to human-readable time string."""
        if value is None:
            return None
        
        if value < 1000:
            return f"{value:.2f} ms"
        elif value < 60000:
            seconds = value / 1000
            return f"{seconds:.2f} s"
        else:
            minutes = value / 60000
            return f"{minutes:.2f} min"
    
    @staticmethod
    def format_bytes(value: Optional[float]) -> Optional[str]:
        """Format bytes to human-readable size string."""
        if value is None:
            return None
        
        value = float(value)
        
        if value < 1024:
            return f"{value:.2f} B"
        elif value < 1024 ** 2:
            kb = value / 1024
            return f"{kb:.2f} KB"
        elif value < 1024 ** 3:
            mb = value / (1024 ** 2)
            return f"{mb:.2f} MB"
        elif value < 1024 ** 4:
            gb = value / (1024 ** 3)
            return f"{gb:.2f} GB"
        else:
            tb = value / (1024 ** 4)
            return f"{tb:.2f} TB"
    
    @staticmethod
    def format_percentage(value: Optional[float]) -> Optional[str]:
        """Format percentage value."""
        if value is None:
            return None
        
        if abs(value) == float('inf'):
            return "inf %" if value > 0 else "-inf %"
        
        return f"{value:.2f}%"
    
    @staticmethod
    def format_count(value: Optional[float]) -> Optional[str]:
        """Format count value."""
        if value is None:
            return None
        
        return str(int(value)) if value == int(value) else f"{value:.2f}"
    
    @staticmethod
    def format_metric(value: Optional[float], metric_name: str) -> Tuple[Optional[str], str]:
        """
        Format metric based on its name.
        
        Args:
            value: Raw metric value
            metric_name: Name of the metric to infer formatting
        
        Returns:
            Tuple of (formatted_value, metric_type)
        """
        if value is None:
            return None, "generic"
        
        metric_lower = metric_name.lower()
        
        # Time metrics
        if any(x in metric_lower for x in ['_ms', '_time', 'duration']):
            return MetricFormatter.format_milliseconds(value), "time"
        
        # Byte metrics
        elif any(x in metric_lower for x in ['_bytes', 'spilling', 'scanned', 'memory']):
            return MetricFormatter.format_bytes(value), "bytes"
        
        # Percentage metrics
        elif 'percent' in metric_lower or 'ratio' in metric_lower:
            return MetricFormatter.format_percentage(value), "percentage"
        
        # Count metrics (joins, subqueries, window functions)
        elif any(x in metric_lower for x in ['_count', '_number', 'depth', 'partitions']):
            return MetricFormatter.format_count(value), "count"
        
        # Credit metrics
        elif 'credit' in metric_lower:
            return f"{value:.2f}", "generic"
        
        # Default
        else:
            return str(value), "generic"


class ReportGenerator:
    """
    Generates comprehensive JSON benchmark reports.
    
    Supports incremental building of report sections:
    1. Metadata (execution context)
    2. Metrics (per-model and aggregated)
    3. Validation (optional, output validation results)
    4. Comparison (optional, baseline comparison)
    5. Summary (auto-generated from other sections)
    """
    
    # Report schema version for compatibility tracking
    SCHEMA_VERSION = "1.0.0"
    
    def __init__(self,
                 pipeline_id: str,
                 pipeline_name: Optional[str] = None,
                 output_directory: str = "benchmark/results"):
        """
        Initialize ReportGenerator.
        
        Args:
            pipeline_id: Pipeline identifier (A, B, C, etc.)
            pipeline_name: Human-readable pipeline name
            output_directory: Directory for generated reports
        """
        self.pipeline_id = pipeline_id
        self.pipeline_name = pipeline_name or pipeline_id
        self.output_directory = Path(output_directory)
        
        # Ensure output directory exists
        self.output_directory.mkdir(parents=True, exist_ok=True)
        
        # Initialize report structure
        self.report: Dict[str, Any] = {
            'schema_version': self.SCHEMA_VERSION,
            'metadata': {},
            'metrics': {
                'per_model': {},
                'aggregated': {}
            }
        }
        
        logger.info(f"ReportGenerator initialized for pipeline {pipeline_id}")
    
    def add_metadata(self,
                    execution_start: Optional[datetime] = None,
                    execution_end: Optional[datetime] = None,
                    git_commit: Optional[str] = None,
                    dbt_version: Optional[str] = None,
                    warehouse_name: Optional[str] = None,
                    warehouse_size: Optional[str] = None,
                    database_name: Optional[str] = None,
                    schema_name: Optional[str] = None,
                    environment: str = "dev",
                    tags: Optional[Dict[str, str]] = None) -> None:
        """
        Add metadata to the report.
        
        Args:
            execution_start: Pipeline execution start time
            execution_end: Pipeline execution end time
            git_commit: Git commit SHA
            dbt_version: dbt version used
            warehouse_name: Snowflake warehouse name
            warehouse_size: Warehouse size (XS, S, M, L, XL)
            database_name: Database name
            schema_name: Schema name
            environment: Execution environment (dev, staging, prod, test, other)
            tags: Custom tags dictionary
        """
        # Set timestamps
        now = datetime.utcnow().isoformat() + 'Z'
        
        # Calculate execution duration
        execution_duration_ms = 0
        if execution_start and execution_end:
            duration = execution_end - execution_start
            execution_duration_ms = int(duration.total_seconds() * 1000)
        
        # Get git info if not provided
        if git_commit is None:
            git_commit = self._get_git_commit()
        
        git_branch = self._get_git_branch()
        
        # Build metadata section
        self.report['metadata'] = {
            'pipeline_id': self.pipeline_id,
            'pipeline_name': self.pipeline_name,
            'timestamp': now,
            'execution_start': execution_start.isoformat() + 'Z' if execution_start else None,
            'execution_end': execution_end.isoformat() + 'Z' if execution_end else None,
            'execution_duration_ms': execution_duration_ms,
            'git_commit': git_commit,
            'git_branch': git_branch,
            'dbt_version': dbt_version,
            'warehouse': {
                'name': warehouse_name,
                'size': warehouse_size,
                'region': None,
                'account': None
            },
            'database': {
                'name': database_name,
                'schema': schema_name
            },
            'environment': environment,
            'tags': tags or {}
        }
        
        logger.info(f"Added metadata to report")
    
    def add_metrics(self,
                   per_model_metrics: Optional[Dict[str, Dict[str, float]]] = None,
                   aggregated_metrics: Optional[Dict[str, float]] = None) -> None:
        """
        Add metrics to the report with raw and formatted values.
        
        Args:
            per_model_metrics: Dict of model_name -> {metric_name: value}
            aggregated_metrics: Dict of metric_name -> value for pipeline-level aggregations
        """
        # Add per-model metrics
        if per_model_metrics:
            for model_name, metrics in per_model_metrics.items():
                raw_metrics = {}
                formatted_metrics = {}
                
                for metric_name, value in metrics.items():
                    raw_metrics[metric_name] = value
                    formatted_value, _ = MetricFormatter.format_metric(value, metric_name)
                    formatted_metrics[metric_name] = formatted_value
                
                self.report['metrics']['per_model'][model_name] = {
                    'model_name': model_name,
                    'raw': raw_metrics,
                    'formatted': formatted_metrics
                }
        
        # Add aggregated metrics
        if aggregated_metrics:
            raw_metrics = {}
            formatted_metrics = {}
            
            for metric_name, value in aggregated_metrics.items():
                raw_metrics[metric_name] = value
                formatted_value, _ = MetricFormatter.format_metric(value, metric_name)
                formatted_metrics[metric_name] = formatted_value
            
            self.report['metrics']['aggregated'] = {
                'raw': raw_metrics,
                'formatted': formatted_metrics
            }
        
        logger.info(f"Added metrics: {len(per_model_metrics or {})} models, "
                   f"{len(aggregated_metrics or {})} aggregated metrics")
    
    def add_validation_results(self,
                              overall_status: str = "pass",
                              per_model_results: Optional[Dict[str, Dict[str, Any]]] = None,
                              summary: Optional[Dict[str, Any]] = None) -> None:
        """
        Add validation results to the report.
        
        Args:
            overall_status: Overall validation status (pass, warning, fail)
            per_model_results: Dict of model_name -> {status, row_count, hash, message}
            summary: Validation summary with total_models, models_passed, models_failed, issues
        """
        validation_section = {
            'status': overall_status,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'per_model': per_model_results or {},
            'summary': summary or {
                'total_models': len(per_model_results or {}),
                'models_passed': 0,
                'models_failed': 0,
                'issues': []
            }
        }
        
        # Calculate summary if not provided
        if not summary and per_model_results:
            passed = sum(1 for v in per_model_results.values() if v.get('status') == 'pass')
            failed = sum(1 for v in per_model_results.values() if v.get('status') == 'fail')
            validation_section['summary'] = {
                'total_models': len(per_model_results),
                'models_passed': passed,
                'models_failed': failed,
                'issues': []
            }
        
        self.report['validation'] = validation_section
        logger.info(f"Added validation results")
    
    def add_comparison_results(self,
                              overall_status: str = "pass",
                              baseline_timestamp: Optional[str] = None,
                              baseline_git_commit: Optional[str] = None,
                              per_model_comparisons: Optional[Dict[str, Dict[str, Any]]] = None,
                              aggregated_comparison: Optional[Dict[str, Any]] = None,
                              violations_summary: Optional[Dict[str, int]] = None) -> None:
        """
        Add comparison results against baseline to the report.
        
        Args:
            overall_status: Overall comparison status (pass, warning, error)
            baseline_timestamp: Timestamp of baseline used
            baseline_git_commit: Git commit of baseline
            per_model_comparisons: Dict of model_name -> comparison results
            aggregated_comparison: Pipeline-level comparison metrics
            violations_summary: Dict with total_violations and by_severity counts
        """
        comparison_section = {
            'status': overall_status,
            'baseline_timestamp': baseline_timestamp,
            'baseline_git_commit': baseline_git_commit,
            'per_model': per_model_comparisons or {},
            'aggregated': aggregated_comparison or {},
            'violation_summary': violations_summary or {
                'total_violations': 0,
                'by_severity': {'INFO': 0, 'WARNING': 0, 'ERROR': 0}
            }
        }
        
        self.report['comparison'] = comparison_section
        logger.info(f"Added comparison results")
    
    def generate_summary(self) -> None:
        """
        Generate summary section from other report sections.
        
        Includes:
        - Overall status based on all components
        - Performance overview (top metrics)
        - Top regressions and improvements
        - Notes and warnings
        """
        summary_section = {
            'overall_status': self._determine_overall_status(),
            'performance_overview': self._build_performance_overview(),
            'top_regressions': [],
            'top_improvements': [],
            'notes': []
        }
        
        # Add top regressions and improvements from comparison if available
        if 'comparison' in self.report and self.report['comparison'].get('per_model'):
            regressions = self._extract_top_regressions()
            improvements = self._extract_top_improvements()
            summary_section['top_regressions'] = regressions
            summary_section['top_improvements'] = improvements
        
        # Add notes based on report contents
        summary_section['notes'] = self._generate_notes()
        
        self.report['summary'] = summary_section
        logger.info(f"Generated summary section")
    
    def to_json(self,
               pretty_print: bool = True,
               output_file: Optional[str] = None) -> str:
        """
        Convert report to JSON string with optional file output.
        
        Ensures atomic writing (complete file written or no file).
        
        Args:
            pretty_print: If True, format JSON with indentation
            output_file: If provided, write to this file atomically
        
        Returns:
            JSON string representation of report
        """
        # Convert report to JSON
        json_str = json.dumps(
            self.report,
            indent=2 if pretty_print else None,
            default=str
        )
        
        # Write to file atomically if specified
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to temporary file first
            with tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.json',
                dir=output_path.parent,
                delete=False
            ) as tmp_file:
                tmp_file.write(json_str)
                tmp_path = tmp_file.name
            
            try:
                # Atomic rename
                Path(tmp_path).rename(output_path)
                logger.info(f"Report written to {output_file}")
            except Exception as e:
                # Clean up temp file if rename failed
                Path(tmp_path).unlink(missing_ok=True)
                logger.error(f"Failed to write report: {str(e)}")
                raise
        
        return json_str
    
    def save(self,
            filename: Optional[str] = None,
            pretty_print: bool = True) -> str:
        """
        Save report to file.
        
        Args:
            filename: Optional custom filename (defaults to generated name)
            pretty_print: If True, format JSON with indentation
        
        Returns:
            Path to saved file
        """
        if filename is None:
            # Generate filename from pipeline and timestamp
            now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"report_{self.pipeline_id}_{now}.json"
        
        output_file = self.output_directory / filename
        self.to_json(pretty_print=pretty_print, output_file=str(output_file))
        
        return str(output_file)
    
    # Private helper methods
    
    def _get_git_commit(self) -> Optional[str]:
        """Get current git commit hash."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=".",
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return result.stdout.strip()
            return None
        
        except Exception as e:
            logger.debug(f"Unable to get git commit: {str(e)}")
            return None
    
    def _get_git_branch(self) -> Optional[str]:
        """Get current git branch name."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=".",
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                return result.stdout.strip()
            return None
        
        except Exception as e:
            logger.debug(f"Unable to get git branch: {str(e)}")
            return None
    
    def _determine_overall_status(self) -> str:
        """Determine overall report status from all sections."""
        # Check comparison status first (highest priority)
        if 'comparison' in self.report:
            comp_status = self.report['comparison'].get('status', 'pass')
            if comp_status == 'error':
                return 'error'
        
        # Check validation status
        if 'validation' in self.report:
            val_status = self.report['validation'].get('status', 'pass')
            if val_status == 'fail':
                return 'error'
        
        # Check comparison again for warning
        if 'comparison' in self.report:
            comp_status = self.report['comparison'].get('status', 'pass')
            if comp_status == 'warning':
                return 'warning'
        
        return 'pass'
    
    def _build_performance_overview(self) -> Dict[str, Dict[str, Any]]:
        """Build performance overview from metrics."""
        overview = {}
        
        aggregated = self.report.get('metrics', {}).get('aggregated', {})
        if not aggregated:
            return overview
        
        # Focus on key metrics
        key_metrics = [
            'total_execution_time_ms',
            'total_bytes_scanned',
            'total_warehouse_credits',
            'model_count'
        ]
        
        raw_metrics = aggregated.get('raw', {})
        formatted_metrics = aggregated.get('formatted', {})
        
        for metric in key_metrics:
            if metric in raw_metrics and raw_metrics[metric] is not None:
                overview[metric] = {
                    'value': raw_metrics[metric],
                    'formatted': formatted_metrics.get(metric),
                    'percentile': 'baseline'
                }
        
        return overview
    
    def _extract_top_regressions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Extract top metric regressions from comparison results."""
        regressions = []
        
        per_model = self.report.get('comparison', {}).get('per_model', {})
        
        for model_name, model_comp in per_model.items():
            violations = model_comp.get('violations', [])
            for violation in violations:
                if violation.get('severity') in ['WARNING', 'ERROR']:
                    # Try to extract delta percent
                    metric_name = violation.get('metric_name')
                    metric_data = model_comp.get('metrics', {}).get(metric_name, {})
                    delta_pct = metric_data.get('delta_percent')
                    
                    if delta_pct and delta_pct > 0:
                        regressions.append({
                            'model': model_name,
                            'metric': metric_name,
                            'delta_percent': delta_pct,
                            'severity': violation.get('severity')
                        })
        
        # Sort by delta percent descending
        regressions.sort(key=lambda x: x['delta_percent'], reverse=True)
        
        return regressions[:limit]
    
    def _extract_top_improvements(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Extract top metric improvements from comparison results."""
        improvements = []
        
        per_model = self.report.get('comparison', {}).get('per_model', {})
        
        for model_name, model_comp in per_model.items():
            metrics = model_comp.get('metrics', {})
            for metric_name, metric_data in metrics.items():
                delta_pct = metric_data.get('delta_percent')
                status = metric_data.get('status')
                
                # Look for negative deltas that represent improvements
                if delta_pct and delta_pct < 0 and status == 'pass_improvement':
                    improvements.append({
                        'model': model_name,
                        'metric': metric_name,
                        'delta_percent': abs(delta_pct)
                    })
        
        # Sort by delta percent descending
        improvements.sort(key=lambda x: x['delta_percent'], reverse=True)
        
        return improvements[:limit]
    
    def _generate_notes(self) -> List[str]:
        """Generate notes based on report contents."""
        notes = []
        
        # Check for missing sections
        if 'comparison' not in self.report:
            notes.append("No baseline comparison available")
        
        if 'validation' not in self.report:
            notes.append("Output validation not performed")
        
        # Check for validation issues
        if 'validation' in self.report:
            summary = self.report['validation'].get('summary', {})
            failed = summary.get('models_failed', 0)
            if failed > 0:
                notes.append(f"WARNING: {failed} model(s) failed validation")
            
            issues = summary.get('issues', [])
            notes.extend(issues)
        
        # Check for violations
        if 'comparison' in self.report:
            viol_summary = self.report['comparison'].get('violation_summary', {})
            errors = viol_summary.get('by_severity', {}).get('ERROR', 0)
            if errors > 0:
                notes.append(f"WARNING: {errors} error-level violation(s) detected")
        
        return notes


def merge_reports(reports: List[Dict[str, Any]],
                  output_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Merge multiple individual pipeline reports into a cross-pipeline summary.
    
    Args:
        reports: List of report dictionaries
        output_file: Optional file to save merged report
    
    Returns:
        Merged report dictionary
    """
    merged = {
        'schema_version': ReportGenerator.SCHEMA_VERSION,
        'merge_timestamp': datetime.utcnow().isoformat() + 'Z',
        'pipeline_reports': reports,
        'cross_pipeline_summary': {
            'total_pipelines': len(reports),
            'overall_status': 'pass',
            'aggregated_metrics': {},
            'aggregated_violations': {
                'total': 0,
                'by_severity': {'INFO': 0, 'WARNING': 0, 'ERROR': 0}
            }
        }
    }
    
    # Determine overall status
    statuses = [r.get('summary', {}).get('overall_status', 'pass') for r in reports]
    if 'error' in statuses:
        merged['cross_pipeline_summary']['overall_status'] = 'error'
    elif 'warning' in statuses:
        merged['cross_pipeline_summary']['overall_status'] = 'warning'
    
    # Aggregate violation counts
    for report in reports:
        if 'comparison' in report:
            viol_summary = report['comparison'].get('violation_summary', {})
            merged['cross_pipeline_summary']['aggregated_violations']['total'] += \
                viol_summary.get('total_violations', 0)
            by_sev = viol_summary.get('by_severity', {})
            for severity in ['INFO', 'WARNING', 'ERROR']:
                merged['cross_pipeline_summary']['aggregated_violations']['by_severity'][severity] += \
                    by_sev.get(severity, 0)
    
    # Save to file if specified
    if output_file:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.json',
            dir=output_path.parent,
            delete=False
        ) as tmp_file:
            json.dump(merged, tmp_file, indent=2, default=str)
            tmp_path = tmp_file.name
        
        try:
            Path(tmp_path).rename(output_path)
            logger.info(f"Merged report written to {output_file}")
        except Exception as e:
            Path(tmp_path).unlink(missing_ok=True)
            logger.error(f"Failed to write merged report: {str(e)}")
            raise
    
    return merged
