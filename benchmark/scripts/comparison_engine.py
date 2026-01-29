"""
Benchmark Comparison Engine

Evaluates candidate benchmark results against baseline benchmarks with
configurable regression thresholds and severity classification.

Features:
- Load baseline and candidate benchmark data
- Per-model metric comparison with delta calculation
- Pipeline-level aggregation and comparison
- Exit code determination for CI/CD integration
- Detailed comparison reports with violations and context
"""

import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
from statistics import mean, stdev

from thresholds import ThresholdManager, MetricsComparer, Violation, SeverityLevel


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ComparisonStatus(Enum):
    """Overall comparison status."""
    PASS = 0        # No violations
    WARNING = 1     # Minor violations detected
    ERROR = 2       # Major violations detected


@dataclass
class ModelComparison:
    """Represents comparison results for a single model."""
    model_name: str
    baseline_metrics: Dict[str, Any]
    candidate_metrics: Dict[str, Any]
    violations: List[Violation] = field(default_factory=list)
    metrics_missing_in_baseline: List[str] = field(default_factory=list)
    metrics_missing_in_candidate: List[str] = field(default_factory=list)
    
    def get_max_severity(self) -> SeverityLevel:
        """Get the maximum severity level among violations."""
        if not self.violations:
            return SeverityLevel.INFO
        return max((v.severity for v in self.violations), key=lambda s: s.value)


@dataclass
class PipelineComparison:
    """Represents comparison results for an entire pipeline."""
    pipeline_name: str
    baseline_timestamp: str
    candidate_timestamp: str
    model_comparisons: Dict[str, ModelComparison] = field(default_factory=dict)
    pipeline_violations: List[Violation] = field(default_factory=list)
    
    def get_all_violations(self) -> List[Violation]:
        """Get all violations across all models and pipeline level."""
        violations = list(self.pipeline_violations)
        for model_comp in self.model_comparisons.values():
            violations.extend(model_comp.violations)
        return violations
    
    def get_max_severity(self) -> SeverityLevel:
        """Get the maximum severity level among all violations."""
        all_violations = self.get_all_violations()
        if not all_violations:
            return SeverityLevel.INFO
        return max((v.severity for v in all_violations), key=lambda s: s.value)
    
    def count_violations_by_severity(self) -> Dict[str, int]:
        """Count violations by severity level."""
        all_violations = self.get_all_violations()
        counts = {
            'INFO': 0,
            'WARNING': 0,
            'ERROR': 0
        }
        for violation in all_violations:
            if violation.severity == SeverityLevel.INFO:
                counts['INFO'] += 1
            elif violation.severity == SeverityLevel.WARNING:
                counts['WARNING'] += 1
            else:
                counts['ERROR'] += 1
        return counts


class ComparisonEngine:
    """
    Main comparison engine for benchmark evaluation.
    
    Orchestrates baseline loading, candidate comparison, and report generation.
    """
    
    def __init__(self,
                 threshold_config_path: str = "benchmark/config/thresholds.yaml",
                 ignore_improvements: bool = False):
        """
        Initialize ComparisonEngine.
        
        Args:
            threshold_config_path: Path to thresholds configuration
            ignore_improvements: If True, don't report improvements as violations
        """
        self.threshold_manager = ThresholdManager(threshold_config_path)
        self.comparer = MetricsComparer(
            self.threshold_manager,
            ignore_improvements=ignore_improvements
        )
        self.ignore_improvements = ignore_improvements
    
    def load_baseline(self, baseline_path: str) -> Optional[Dict[str, Any]]:
        """
        Load baseline data from JSON file.
        
        Args:
            baseline_path: Path to baseline JSON file
        
        Returns:
            Baseline data dictionary or None if load fails
        """
        try:
            path = Path(baseline_path)
            if not path.exists():
                logger.error(f"Baseline file not found: {baseline_path}")
                return None
            
            with open(path, 'r') as f:
                baseline = json.load(f)
            
            logger.info(f"Loaded baseline: {baseline_path}")
            return baseline
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse baseline JSON: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error loading baseline: {str(e)}")
            return None
    
    def load_candidate(self, candidate_path: str) -> Optional[Dict[str, Any]]:
        """
        Load candidate data from JSON file.
        
        Args:
            candidate_path: Path to candidate JSON file
        
        Returns:
            Candidate data dictionary or None if load fails
        """
        try:
            path = Path(candidate_path)
            if not path.exists():
                logger.error(f"Candidate file not found: {candidate_path}")
                return None
            
            with open(path, 'r') as f:
                candidate = json.load(f)
            
            logger.info(f"Loaded candidate: {candidate_path}")
            return candidate
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse candidate JSON: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error loading candidate: {str(e)}")
            return None
    
    def compare_models(self,
                      baseline: Dict[str, Any],
                      candidate: Dict[str, Any]) -> ModelComparison:
        """
        Compare metrics for a single model.
        
        Args:
            baseline: Baseline metrics for model
            candidate: Candidate metrics for model
        
        Returns:
            ModelComparison object with violations and missing metrics
        """
        model_name = baseline.get('model_name', 'unknown')
        
        # Extract per-model metrics
        baseline_metrics = baseline.get('metrics', {})
        candidate_metrics = candidate.get('metrics', {})
        
        # Identify missing metrics
        baseline_keys = set(baseline_metrics.keys())
        candidate_keys = set(candidate_metrics.keys())
        
        missing_in_baseline = list(candidate_keys - baseline_keys)
        missing_in_candidate = list(baseline_keys - candidate_keys)
        
        # Compare common metrics
        violations = self.comparer.compare_metrics(
            baseline_metrics,
            candidate_metrics,
            metric_names=list(baseline_keys & candidate_keys)
        )
        
        return ModelComparison(
            model_name=model_name,
            baseline_metrics=baseline_metrics,
            candidate_metrics=candidate_metrics,
            violations=violations,
            metrics_missing_in_baseline=missing_in_baseline,
            metrics_missing_in_candidate=missing_in_candidate
        )
    
    def compare_pipeline(self,
                        baseline: Dict[str, Any],
                        candidate: Dict[str, Any]) -> PipelineComparison:
        """
        Compare metrics between baseline and candidate benchmark runs.
        
        Args:
            baseline: Baseline benchmark data
            candidate: Candidate benchmark data
        
        Returns:
            PipelineComparison object with all results
        """
        pipeline_name = baseline.get('pipeline', 'unknown')
        baseline_ts = baseline.get('captured_at', 'unknown')
        candidate_ts = candidate.get('captured_at', 'unknown')
        
        logger.info(f"Comparing pipeline {pipeline_name}: {baseline_ts} vs {candidate_ts}")
        
        # Initialize comparison
        comparison = PipelineComparison(
            pipeline_name=pipeline_name,
            baseline_timestamp=baseline_ts,
            candidate_timestamp=candidate_ts
        )
        
        # Compare per-model metrics
        baseline_models = baseline.get('per_model', {})
        candidate_models = candidate.get('per_model', {})
        
        all_model_names = set(baseline_models.keys()) | set(candidate_models.keys())
        
        for model_name in all_model_names:
            baseline_model = baseline_models.get(model_name, {'model_name': model_name})
            candidate_model = candidate_models.get(model_name, {'model_name': model_name})
            
            model_comparison = self.compare_models(baseline_model, candidate_model)
            comparison.model_comparisons[model_name] = model_comparison
        
        # Compare pipeline-level aggregations
        baseline_agg = baseline.get('pipeline_aggregations', {})
        candidate_agg = candidate.get('pipeline_aggregations', {})
        
        pipeline_violations = self.comparer.compare_metrics(
            baseline_agg,
            candidate_agg
        )
        comparison.pipeline_violations = pipeline_violations
        
        # Log results
        logger.info(f"Comparison complete: {len(comparison.model_comparisons)} models compared")
        
        return comparison
    
    def generate_summary(self,
                        comparison: PipelineComparison) -> Tuple[ComparisonStatus, int]:
        """
        Generate comparison summary and determine exit code.
        
        Args:
            comparison: PipelineComparison object
        
        Returns:
            Tuple of (ComparisonStatus, exit_code)
                - exit_code: 0 for PASS, 1 for WARNING, 2 for ERROR
        """
        max_severity = comparison.get_max_severity()
        
        if max_severity == SeverityLevel.INFO:
            return ComparisonStatus.PASS, 0
        elif max_severity == SeverityLevel.WARNING:
            return ComparisonStatus.WARNING, 1
        else:  # ERROR
            return ComparisonStatus.ERROR, 2
    
    def format_report(self, comparison: PipelineComparison) -> str:
        """
        Format a detailed comparison report.
        
        Args:
            comparison: PipelineComparison object
        
        Returns:
            Formatted report string
        """
        lines = []
        
        lines.append("=" * 80)
        lines.append(f"BENCHMARK COMPARISON REPORT")
        lines.append("=" * 80)
        lines.append(f"Pipeline: {comparison.pipeline_name}")
        lines.append(f"Baseline: {comparison.baseline_timestamp}")
        lines.append(f"Candidate: {comparison.candidate_timestamp}")
        lines.append("")
        
        # Summary statistics
        violation_counts = comparison.count_violations_by_severity()
        lines.append("VIOLATION SUMMARY")
        lines.append("-" * 80)
        lines.append(f"  INFO:    {violation_counts['INFO']:>3} violations (improvements/neutral)")
        lines.append(f"  WARNING: {violation_counts['WARNING']:>3} violations (minor regressions)")
        lines.append(f"  ERROR:   {violation_counts['ERROR']:>3} violations (major regressions)")
        lines.append("")
        
        # Overall status
        status, exit_code = self.generate_summary(comparison)
        lines.append("OVERALL STATUS")
        lines.append("-" * 80)
        lines.append(f"  Status: {status.name} (exit code: {exit_code})")
        lines.append("")
        
        # Per-model results
        if comparison.model_comparisons:
            lines.append("MODEL-LEVEL RESULTS")
            lines.append("-" * 80)
            
            for model_name in sorted(comparison.model_comparisons.keys()):
                model_comp = comparison.model_comparisons[model_name]
                status_str = "PASS" if not model_comp.violations else model_comp.get_max_severity().name
                
                lines.append(f"  {model_name}: {status_str}")
                
                # Show violations for this model
                if model_comp.violations:
                    for violation in model_comp.violations:
                        severity_marker = {
                            SeverityLevel.INFO: "ℹ",
                            SeverityLevel.WARNING: "⚠",
                            SeverityLevel.ERROR: "✗"
                        }.get(violation.severity, "•")
                        
                        lines.append(f"    {severity_marker} {violation.get_message()}")
                
                # Show missing metrics
                if model_comp.metrics_missing_in_candidate:
                    lines.append(f"    ⚠ New metrics in candidate: {', '.join(model_comp.metrics_missing_in_candidate)}")
                if model_comp.metrics_missing_in_baseline:
                    lines.append(f"    ℹ Removed metrics: {', '.join(model_comp.metrics_missing_in_baseline)}")
            
            lines.append("")
        
        # Pipeline-level violations
        if comparison.pipeline_violations:
            lines.append("PIPELINE-LEVEL VIOLATIONS")
            lines.append("-" * 80)
            
            for violation in comparison.pipeline_violations:
                severity_marker = {
                    SeverityLevel.INFO: "ℹ",
                    SeverityLevel.WARNING: "⚠",
                    SeverityLevel.ERROR: "✗"
                }.get(violation.severity, "•")
                
                lines.append(f"  {severity_marker} {violation.get_message()}")
            
            lines.append("")
        
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    def generate_json_report(self, comparison: PipelineComparison) -> Dict[str, Any]:
        """
        Generate a machine-readable JSON report.
        
        Args:
            comparison: PipelineComparison object
        
        Returns:
            Dictionary suitable for JSON serialization
        """
        status, exit_code = self.generate_summary(comparison)
        
        violations_by_severity = {
            'INFO': [],
            'WARNING': [],
            'ERROR': []
        }
        
        for violation in comparison.get_all_violations():
            severity_name = violation.severity.name
            violations_by_severity[severity_name].append({
                'metric': violation.metric_name,
                'baseline': violation.baseline_value,
                'candidate': violation.candidate_value,
                'delta': violation.delta,
                'delta_percent': violation.delta_percent,
                'threshold': violation.threshold,
                'is_improvement': violation.is_improvement,
                'message': violation.get_message()
            })
        
        violation_counts = comparison.count_violations_by_severity()
        
        return {
            'pipeline': comparison.pipeline_name,
            'baseline_timestamp': comparison.baseline_timestamp,
            'candidate_timestamp': comparison.candidate_timestamp,
            'status': status.name,
            'exit_code': exit_code,
            'summary': {
                'total_violations': sum(violation_counts.values()),
                'info': violation_counts['INFO'],
                'warning': violation_counts['WARNING'],
                'error': violation_counts['ERROR']
            },
            'violations': violations_by_severity,
            'models': {
                model_name: {
                    'violations': [
                        {
                            'metric': v.metric_name,
                            'delta': v.delta,
                            'delta_percent': v.delta_percent,
                            'message': v.get_message()
                        }
                        for v in model_comp.violations
                    ],
                    'missing_in_baseline': model_comp.metrics_missing_in_baseline,
                    'missing_in_candidate': model_comp.metrics_missing_in_candidate
                }
                for model_name, model_comp in comparison.model_comparisons.items()
            }
        }
