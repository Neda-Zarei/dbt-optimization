"""
Threshold Management and Violation Detection Module

Evaluates metrics against configured thresholds, classifies violations by severity,
and provides detailed violation messages for regression detection.

Features:
- Load threshold configuration from YAML files
- Support both percentage-based and absolute thresholds
- Asymmetric thresholds (different limits for improvements vs regressions)
- Violation detection with severity classification
- Detailed violation messages with metrics and context
- Handle missing metrics, nulls, and edge cases gracefully
"""

import logging
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from enum import Enum


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SeverityLevel(Enum):
    """Severity classification for metric violations."""
    INFO = 0          # Improvement or informational
    WARNING = 1       # Minor regression
    ERROR = 2         # Major regression


class Violation:
    """Represents a single metric violation."""
    
    def __init__(self,
                 metric_name: str,
                 baseline_value: Optional[float],
                 candidate_value: Optional[float],
                 delta: float,
                 delta_percent: Optional[float],
                 threshold: float,
                 threshold_type: str,  # 'percent' or 'absolute'
                 severity: SeverityLevel,
                 is_improvement: bool = False):
        """
        Initialize a violation.
        
        Args:
            metric_name: Name of the metric
            baseline_value: Baseline metric value
            candidate_value: Candidate metric value
            delta: Absolute change
            delta_percent: Percentage change (if applicable)
            threshold: The threshold that was exceeded
            threshold_type: 'percent' for percentage-based, 'absolute' for absolute
            severity: Severity level of the violation
            is_improvement: True if this is an improvement (negative delta)
        """
        self.metric_name = metric_name
        self.baseline_value = baseline_value
        self.candidate_value = candidate_value
        self.delta = delta
        self.delta_percent = delta_percent
        self.threshold = threshold
        self.threshold_type = threshold_type
        self.severity = severity
        self.is_improvement = is_improvement
    
    def get_message(self) -> str:
        """Generate a detailed violation message."""
        improvement_text = " (IMPROVEMENT)" if self.is_improvement else ""
        
        if self.delta_percent is not None:
            return (
                f"{self.metric_name}: baseline={self.baseline_value}, "
                f"candidate={self.candidate_value}, "
                f"delta={self.delta:.2f} ({self.delta_percent:.2f}%), "
                f"threshold={self.threshold}% {improvement_text}"
            )
        else:
            return (
                f"{self.metric_name}: baseline={self.baseline_value}, "
                f"candidate={self.candidate_value}, "
                f"delta={self.delta:.2f} {self.threshold_type}, "
                f"threshold={self.threshold} {improvement_text}"
            )


class ThresholdManager:
    """
    Manages threshold configuration and evaluation.
    
    Attributes:
        thresholds: Dictionary of metric thresholds from configuration
        config_path: Path to thresholds configuration file
    """
    
    def __init__(self, config_path: str = "benchmark/config/thresholds.yaml"):
        """
        Initialize ThresholdManager.
        
        Args:
            config_path: Path to thresholds configuration file
        """
        self.config_path = Path(config_path)
        self.thresholds = {}
        self._load_config()
    
    def _load_config(self) -> bool:
        """
        Load threshold configuration from YAML file.
        
        Returns:
            bool: True if loaded successfully
        """
        try:
            if not self.config_path.exists():
                logger.warning(f"Thresholds config not found: {self.config_path}")
                return False
            
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if config and 'thresholds' in config:
                self.thresholds = config['thresholds']
                logger.info(f"Loaded thresholds for {len(self.thresholds)} metrics")
                return True
            
            logger.warning("No thresholds found in configuration")
            return False
        
        except Exception as e:
            logger.error(f"Error loading thresholds config: {str(e)}")
            return False
    
    def get_threshold(self, metric_name: str) -> Optional[Dict[str, Any]]:
        """
        Get threshold configuration for a metric.
        
        Args:
            metric_name: Name of the metric
        
        Returns:
            Dictionary with threshold configuration or None if not found
        """
        return self.thresholds.get(metric_name)
    
    def is_metric_configured(self, metric_name: str) -> bool:
        """Check if a metric has a configured threshold."""
        return metric_name in self.thresholds
    
    def get_all_metrics(self) -> List[str]:
        """Get list of all configured metrics."""
        return list(self.thresholds.keys())


class MetricsComparer:
    """
    Compares metrics between baseline and candidate runs.
    
    Handles delta calculation, threshold evaluation, and violation detection
    with configurable severity classification.
    """
    
    def __init__(self,
                 threshold_manager: ThresholdManager,
                 ignore_improvements: bool = False):
        """
        Initialize MetricsComparer.
        
        Args:
            threshold_manager: ThresholdManager instance
            ignore_improvements: If True, don't fail on improvements (negative deltas)
        """
        self.threshold_manager = threshold_manager
        self.ignore_improvements = ignore_improvements
    
    def calculate_delta(self,
                       baseline_value: Optional[float],
                       candidate_value: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
        """
        Calculate absolute and percentage deltas.
        
        Handles nulls, zeros, and edge cases gracefully.
        
        Args:
            baseline_value: Baseline metric value
            candidate_value: Candidate metric value
        
        Returns:
            Tuple of (delta, delta_percent) or (None, None) if values unavailable
        """
        # Both values must be present to calculate delta
        if baseline_value is None or candidate_value is None:
            return None, None
        
        # Calculate absolute delta
        delta = candidate_value - baseline_value
        
        # Calculate percentage delta
        delta_percent = None
        if baseline_value != 0:
            delta_percent = (delta / baseline_value) * 100
        elif candidate_value == 0:
            # Both are zero, no change
            delta_percent = 0.0
        else:
            # Baseline is zero, candidate is non-zero
            # Treat as infinite increase (use a large percentage)
            delta_percent = float('inf') if candidate_value > 0 else float('-inf')
        
        return delta, delta_percent
    
    def evaluate_threshold(self,
                          metric_name: str,
                          baseline_value: Optional[float],
                          candidate_value: Optional[float],
                          ignore_improvements: Optional[bool] = None) -> Optional[Violation]:
        """
        Evaluate a metric against its threshold.
        
        Args:
            metric_name: Name of the metric
            baseline_value: Baseline metric value
            candidate_value: Candidate metric value
            ignore_improvements: Override instance setting to ignore improvements
        
        Returns:
            Violation object if threshold exceeded, None otherwise
        """
        # Use instance setting if not overridden
        ignore_impr = ignore_improvements if ignore_improvements is not None else self.ignore_improvements
        
        # Get threshold configuration
        threshold_config = self.threshold_manager.get_threshold(metric_name)
        if not threshold_config:
            return None
        
        # Calculate deltas
        delta, delta_percent = self.calculate_delta(baseline_value, candidate_value)
        if delta is None:
            return None
        
        # Determine if this is an improvement
        is_improvement = delta < 0
        
        # If ignoring improvements and this is an improvement, return None
        if ignore_impr and is_improvement:
            return None
        
        # Check percentage-based threshold
        if 'max_increase_percent' in threshold_config:
            threshold = threshold_config['max_increase_percent']
            
            # For improvements, check if we should report (only if not ignoring)
            if is_improvement and not ignore_impr:
                # Report improvements as violations with INFO severity
                if delta_percent is not None and delta_percent < -abs(threshold):
                    severity = SeverityLevel.INFO
                    return Violation(
                        metric_name=metric_name,
                        baseline_value=baseline_value,
                        candidate_value=candidate_value,
                        delta=delta,
                        delta_percent=delta_percent,
                        threshold=threshold,
                        threshold_type='percent',
                        severity=severity,
                        is_improvement=True
                    )
            
            # For regressions, check against threshold
            if not is_improvement and delta_percent is not None:
                if delta_percent > threshold:
                    # Classify by magnitude of violation
                    severity = self._classify_severity(metric_name, delta_percent, threshold)
                    return Violation(
                        metric_name=metric_name,
                        baseline_value=baseline_value,
                        candidate_value=candidate_value,
                        delta=delta,
                        delta_percent=delta_percent,
                        threshold=threshold,
                        threshold_type='percent',
                        severity=severity,
                        is_improvement=False
                    )
        
        # Check absolute threshold
        if 'max_increase_absolute' in threshold_config:
            threshold = threshold_config['max_increase_absolute']
            
            # For improvements, report if ignore_improvements is False
            if is_improvement and not ignore_impr:
                if delta < -abs(threshold):
                    severity = SeverityLevel.INFO
                    return Violation(
                        metric_name=metric_name,
                        baseline_value=baseline_value,
                        candidate_value=candidate_value,
                        delta=delta,
                        delta_percent=delta_percent,
                        threshold=threshold,
                        threshold_type='absolute',
                        severity=severity,
                        is_improvement=True
                    )
            
            # For regressions
            if not is_improvement and delta > threshold:
                severity = self._classify_severity(metric_name, delta, threshold, is_absolute=True)
                return Violation(
                    metric_name=metric_name,
                    baseline_value=baseline_value,
                    candidate_value=candidate_value,
                    delta=delta,
                    delta_percent=delta_percent,
                    threshold=threshold,
                    threshold_type='absolute',
                    severity=severity,
                    is_improvement=False
                )
        
        return None
    
    def _classify_severity(self,
                          metric_name: str,
                          delta: float,
                          threshold: float,
                          is_absolute: bool = False) -> SeverityLevel:
        """
        Classify violation severity based on magnitude and metric configuration.
        
        Args:
            metric_name: Name of the metric
            delta: The delta value (absolute or percentage)
            threshold: The configured threshold
            is_absolute: True if delta is absolute, False if percentage
        
        Returns:
            SeverityLevel enum value
        """
        threshold_config = self.threshold_manager.get_threshold(metric_name)
        if not threshold_config:
            return SeverityLevel.WARNING
        
        # Get severity hint from config
        severity_hint = threshold_config.get('severity', 'medium').lower()
        
        # Calculate magnitude of violation
        if is_absolute:
            magnitude = delta / threshold if threshold != 0 else float('inf')
        else:
            magnitude = delta / threshold if threshold != 0 else float('inf')
        
        # Classify based on severity hint and magnitude
        if severity_hint == 'critical':
            return SeverityLevel.ERROR
        elif severity_hint == 'high':
            # High severity is ERROR if violation is >50% of threshold, else WARNING
            return SeverityLevel.ERROR if magnitude > 0.5 else SeverityLevel.WARNING
        else:  # medium or low
            return SeverityLevel.WARNING
    
    def compare_metrics(self,
                       baseline_metrics: Dict[str, Any],
                       candidate_metrics: Dict[str, Any],
                       metric_names: Optional[List[str]] = None,
                       ignore_improvements: Optional[bool] = None) -> List[Violation]:
        """
        Compare metrics between baseline and candidate.
        
        Args:
            baseline_metrics: Dictionary of baseline metric values
            candidate_metrics: Dictionary of candidate metric values
            metric_names: List of metric names to compare (if None, compare all configured)
            ignore_improvements: Override instance setting
        
        Returns:
            List of Violation objects for all metrics that exceed thresholds
        """
        violations = []
        
        # Determine which metrics to compare
        if metric_names is None:
            metric_names = list(set(
                list(baseline_metrics.keys()) + 
                list(candidate_metrics.keys())
            ))
        
        # Compare each metric
        for metric_name in metric_names:
            baseline_value = baseline_metrics.get(metric_name)
            candidate_value = candidate_metrics.get(metric_name)
            
            violation = self.evaluate_threshold(
                metric_name,
                baseline_value,
                candidate_value,
                ignore_improvements=ignore_improvements
            )
            
            if violation:
                violations.append(violation)
        
        return violations
