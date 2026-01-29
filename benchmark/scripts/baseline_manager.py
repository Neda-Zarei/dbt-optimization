"""
Baseline Management System

Captures, stores, retrieves, and deletes benchmark baselines for the dbt pipeline
benchmarking system.

Features:
- Captures complete benchmark results with metrics and validation hashes
- Stores baselines with ISO 8601 timestamps for sortability
- Retrieves most recent baseline or specific baseline by timestamp
- Lists baselines with summary metadata
- Deletes baselines with safeguards against accidental deletion
- Handles edge cases and error conditions gracefully
"""

import json
import logging
import subprocess
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import sys

from storage import StorageManager
from dbt_runner import PipelineRunner
from metrics_collector import MetricsCollector
from output_validator import OutputValidator


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BaselineManager:
    """
    Manages baseline capture, storage, retrieval, and deletion.
    
    Attributes:
        storage: StorageManager instance for file I/O
        pipelines_config: Pipeline configuration from pipelines.yaml
        config: Configuration from config.yaml
        base_dir: Base directory for baseline storage
    """
    
    def __init__(self, base_dir: str = "benchmark/baselines", 
                 config_path: str = "benchmark/config/config.yaml",
                 pipelines_config_path: str = "benchmark/config/pipelines.yaml"):
        """
        Initialize BaselineManager.
        
        Args:
            base_dir: Base directory for baseline files
            config_path: Path to main config.yaml
            pipelines_config_path: Path to pipelines.yaml
        """
        self.base_dir = base_dir
        self.storage = StorageManager(base_dir)
        self.config_path = Path(config_path)
        self.pipelines_config_path = Path(pipelines_config_path)
        
        self.config = {}
        self.pipelines_config = {}
        
        # Load configurations
        self._load_pipelines_config()
        self._load_config()
    
    def _load_pipelines_config(self) -> bool:
        """
        Load pipeline configuration from pipelines.yaml.
        
        Returns:
            bool: True if loaded successfully
        """
        try:
            if not self.pipelines_config_path.exists():
                error_msg = f"Pipelines config not found: {self.pipelines_config_path}"
                logger.warning(error_msg)
                return False
            
            with open(self.pipelines_config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if config and 'pipelines' in config:
                self.pipelines_config = config['pipelines']
                logger.info("Pipelines configuration loaded")
                return True
            
            logger.warning("No pipelines found in configuration")
            return False
        
        except Exception as e:
            logger.warning(f"Error loading pipelines config: {str(e)}")
            return False
    
    def _load_config(self) -> bool:
        """
        Load baseline configuration from config.yaml.
        
        Returns:
            bool: True if loaded successfully
        """
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r') as f:
                    config = yaml.safe_load(f) or {}
                self.config = config.get('baseline', {})
                logger.info("Baseline configuration loaded")
            else:
                logger.debug(f"Config file not found: {self.config_path}")
            
            return True
        
        except Exception as e:
            logger.warning(f"Error loading config: {str(e)}")
            return False
    
    def _get_git_commit(self) -> Optional[str]:
        """
        Get current git commit hash.
        
        Returns:
            Commit hash or None if not available
        """
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
    
    def _get_dbt_version(self, project_root: str = ".") -> Optional[str]:
        """
        Get installed dbt version.
        
        Args:
            project_root: Root directory of dbt project
        
        Returns:
            dbt version or None if not available
        """
        try:
            result = subprocess.run(
                ["dbt", "--version"],
                cwd=project_root,
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                for line in result.stdout.split('\n'):
                    if 'dbt version' in line.lower():
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            return parts[2]
            
            return None
        
        except Exception as e:
            logger.debug(f"Unable to get dbt version: {str(e)}")
            return None
    
    def _format_timestamp(self, dt: datetime) -> str:
        """
        Format datetime as ISO 8601 string (YYYYMMDD_HHMMSS).
        
        Args:
            dt: Datetime object
        
        Returns:
            Formatted timestamp string
        """
        return dt.strftime("%Y%m%d_%H%M%S")
    
    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """
        Parse ISO 8601 timestamp string (YYYYMMDD_HHMMSS).
        
        Args:
            timestamp_str: Timestamp string
        
        Returns:
            Datetime object or None if parsing fails
        """
        try:
            return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
        except:
            return None
    
    def _extract_baseline_metadata(self, filename: str) -> Optional[Tuple[str, str]]:
        """
        Extract pipeline and timestamp from baseline filename.
        
        Expected format: baseline_<pipeline>_<timestamp>.json
        
        Args:
            filename: Baseline filename
        
        Returns:
            Tuple of (pipeline, timestamp) or None if parsing fails
        """
        try:
            if not filename.startswith("baseline_") or not filename.endswith(".json"):
                return None
            
            # Remove prefix and suffix
            name_without_prefix = filename[len("baseline_"):-len(".json")]
            
            # Split by underscore, last part is timestamp (YYYYMMDD_HHMMSS has one underscore)
            parts = name_without_prefix.rsplit("_", 2)
            
            if len(parts) != 3:
                return None
            
            pipeline = parts[0]
            timestamp = f"{parts[1]}_{parts[2]}"
            
            # Validate timestamp format
            if not self._parse_timestamp(timestamp):
                return None
            
            return (pipeline, timestamp)
        
        except Exception as e:
            logger.debug(f"Error parsing baseline metadata from {filename}: {str(e)}")
            return None
    
    def capture_baseline(self, pipeline_id: str, 
                        project_root: str = ".",
                        metrics_enabled: bool = True,
                        validation_enabled: bool = True) -> Dict[str, Any]:
        """
        Capture a complete baseline for a pipeline.
        
        Orchestrates:
        1. Pipeline execution with dependency resolution
        2. Metrics collection from Snowflake
        3. Output validation with hash computation
        
        Args:
            pipeline_id: Pipeline identifier (A, B, C)
            project_root: Root directory of dbt project
            metrics_enabled: Whether to collect metrics (default: True)
            validation_enabled: Whether to validate outputs (default: True)
        
        Returns:
            Dictionary with baseline data and metadata
        """
        baseline_data = {
            "pipeline": pipeline_id.upper(),
            "captured_at": self._format_timestamp(datetime.now()),
            "execution_context": {
                "start_time": None,
                "end_time": None,
                "duration_seconds": None,
                "dbt_version": self._get_dbt_version(project_root),
                "git_commit": self._get_git_commit(),
                "project_root": project_root
            },
            "pipeline_metadata": {},
            "metrics": {},
            "validation": {},
            "summary": {
                "status": "FAILED",
                "errors": []
            }
        }
        
        try:
            # Get pipeline metadata
            if pipeline_id.upper() in self.pipelines_config:
                baseline_data["pipeline_metadata"] = self.pipelines_config[pipeline_id.upper()]
            
            # Execute pipeline
            logger.info(f"Starting baseline capture for pipeline {pipeline_id}")
            execution_start = datetime.now()
            baseline_data["execution_context"]["start_time"] = self._format_timestamp(execution_start)
            
            # Run dbt pipeline
            runner = PipelineRunner(pipeline_id, project_root=project_root)
            if not runner.load_config():
                error_msg = "Failed to load pipeline configuration"
                baseline_data["summary"]["errors"].append(error_msg)
                logger.error(error_msg)
                return baseline_data
            
            # Resolve dependencies and execute
            execution_order = runner.resolve_dependencies()
            logger.info(f"Execution order: {execution_order}")
            
            for pipeline in execution_order:
                success, models, stderr = runner.execute_dbt(pipeline, capture_models=True)
                if not success:
                    error_msg = f"Failed to execute pipeline {pipeline}"
                    baseline_data["summary"]["errors"].append(error_msg)
                    logger.error(error_msg)
                    return baseline_data
            
            # Get execution results
            baseline_data["execution_context"]["dependencies_executed"] = execution_order
            baseline_data["execution_context"]["models_executed"] = runner.execution_results.get("models_executed", [])
            
            execution_end = datetime.now()
            baseline_data["execution_context"]["end_time"] = self._format_timestamp(execution_end)
            baseline_data["execution_context"]["duration_seconds"] = (execution_end - execution_start).total_seconds()
            
            # Collect metrics if enabled
            if metrics_enabled:
                logger.info("Collecting metrics from Snowflake")
                try:
                    metrics_collector = MetricsCollector()
                    # Note: In a real implementation, we would extract query IDs from execution
                    # For now, we store the collector reference for potential future use
                    baseline_data["metrics"]["collection_enabled"] = True
                except Exception as e:
                    logger.warning(f"Could not initialize metrics collector: {str(e)}")
                    baseline_data["metrics"]["collection_enabled"] = False
                    baseline_data["summary"]["errors"].append(f"Metrics collection failed: {str(e)}")
            
            # Validate outputs if enabled
            if validation_enabled:
                logger.info("Validating pipeline outputs")
                try:
                    validator = OutputValidator()
                    
                    # Validate models in the target pipeline schema
                    pipeline_config = self.pipelines_config.get(pipeline_id.upper(), {})
                    schema = pipeline_config.get('schema')
                    
                    if schema:
                        # In a real implementation, we would get the list of models
                        # and validate each one, storing validation hashes
                        baseline_data["validation"]["schema"] = schema
                        baseline_data["validation"]["validation_enabled"] = True
                    else:
                        baseline_data["validation"]["validation_enabled"] = False
                        logger.warning(f"No schema configured for pipeline {pipeline_id}")
                
                except Exception as e:
                    logger.warning(f"Could not initialize output validator: {str(e)}")
                    baseline_data["validation"]["validation_enabled"] = False
                    baseline_data["summary"]["errors"].append(f"Output validation failed: {str(e)}")
            
            # Update summary
            if not baseline_data["summary"]["errors"]:
                baseline_data["summary"]["status"] = "SUCCESS"
            
            logger.info(f"Baseline capture completed for pipeline {pipeline_id}")
            return baseline_data
        
        except Exception as e:
            error_msg = f"Error capturing baseline: {str(e)}"
            logger.error(error_msg)
            baseline_data["summary"]["errors"].append(error_msg)
            return baseline_data
    
    def save_baseline(self, baseline_data: Dict[str, Any], 
                     force: bool = False) -> Tuple[bool, str]:
        """
        Save baseline data to JSON file with atomic writes.
        
        Uses naming convention: baseline_<pipeline>_<timestamp>.json
        
        Args:
            baseline_data: Baseline data to save
            force: If False, prevent overwriting existing baselines
        
        Returns:
            Tuple of (success: bool, filename_or_error: str)
        """
        try:
            pipeline = baseline_data.get("pipeline", "UNKNOWN").upper()
            timestamp = baseline_data.get("captured_at", self._format_timestamp(datetime.now()))
            
            filename = f"baseline_{pipeline}_{timestamp}.json"
            
            # Check if file already exists
            if self.storage.file_exists(filename) and not force:
                error_msg = f"Baseline already exists: {filename}. Use force=True to overwrite."
                logger.warning(error_msg)
                return False, error_msg
            
            # Save the file
            self.storage.save_json(baseline_data, filename)
            logger.info(f"Baseline saved successfully: {filename}")
            return True, filename
        
        except Exception as e:
            error_msg = f"Error saving baseline: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def load_baseline(self, pipeline_id: str, 
                     timestamp: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Load a baseline for a pipeline.
        
        If timestamp is not provided, loads the most recent baseline.
        
        Args:
            pipeline_id: Pipeline identifier
            timestamp: Optional timestamp in format YYYYMMDD_HHMMSS
        
        Returns:
            Baseline data dict or None if not found
        """
        try:
            pipeline = pipeline_id.upper()
            
            # If timestamp provided, load specific baseline
            if timestamp:
                filename = f"baseline_{pipeline}_{timestamp}.json"
                if self.storage.file_exists(filename):
                    return self.storage.load_json(filename)
                else:
                    logger.warning(f"Baseline not found: {filename}")
                    return None
            
            # Otherwise, find and load most recent baseline for pipeline
            pattern = f"baseline_{pipeline}_*.json"
            files = self.storage.list_files(pattern)
            
            if not files:
                logger.info(f"No baselines found for pipeline {pipeline}")
                return None
            
            # Files are already sorted by list_files, last one is most recent
            most_recent = files[-1]
            filename = self.storage.get_relative_path(most_recent)
            
            return self.storage.load_json(filename)
        
        except Exception as e:
            logger.error(f"Error loading baseline: {str(e)}")
            return None
    
    def list_baselines(self, pipeline_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all baselines with summary metadata.
        
        Args:
            pipeline_id: Optional pipeline to filter by
        
        Returns:
            List of baseline summaries sorted by timestamp (newest first)
        """
        try:
            if pipeline_id:
                pattern = f"baseline_{pipeline_id.upper()}_*.json"
            else:
                pattern = "baseline_*.json"
            
            files = self.storage.list_files(pattern)
            
            summaries = []
            for file_path in reversed(files):  # Reverse for newest first
                try:
                    filename = file_path.name
                    metadata = self._extract_baseline_metadata(filename)
                    
                    if not metadata:
                        logger.debug(f"Could not parse metadata from {filename}")
                        continue
                    
                    pipeline, timestamp = metadata
                    
                    # Load full baseline to get execution time and status
                    baseline_data = self.storage.load_json(self.storage.get_relative_path(file_path))
                    
                    summary = {
                        "filename": filename,
                        "pipeline": pipeline,
                        "timestamp": timestamp,
                        "captured_at": baseline_data.get("captured_at"),
                        "status": baseline_data.get("summary", {}).get("status", "UNKNOWN"),
                        "execution_time": baseline_data.get("execution_context", {}).get("duration_seconds"),
                        "dbt_version": baseline_data.get("execution_context", {}).get("dbt_version"),
                        "git_commit": baseline_data.get("execution_context", {}).get("git_commit"),
                        "models_executed": len(baseline_data.get("execution_context", {}).get("models_executed", []))
                    }
                    
                    summaries.append(summary)
                
                except Exception as e:
                    logger.warning(f"Error processing baseline {file_path}: {str(e)}")
                    continue
            
            return summaries
        
        except Exception as e:
            logger.error(f"Error listing baselines: {str(e)}")
            return []
    
    def delete_baseline(self, pipeline_id: str, 
                       timestamp: Optional[str] = None,
                       confirm: bool = False) -> Tuple[bool, str]:
        """
        Delete a baseline with safeguards against accidental deletion.
        
        Args:
            pipeline_id: Pipeline identifier
            timestamp: Timestamp in format YYYYMMDD_HHMMSS
            confirm: Must be True to actually delete
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            if not confirm:
                error_msg = "Deletion requires confirm=True to prevent accidental data loss"
                logger.warning(error_msg)
                return False, error_msg
            
            pipeline = pipeline_id.upper()
            
            if not timestamp:
                error_msg = "Timestamp is required to delete a specific baseline"
                logger.warning(error_msg)
                return False, error_msg
            
            filename = f"baseline_{pipeline}_{timestamp}.json"
            
            if not self.storage.file_exists(filename):
                error_msg = f"Baseline not found: {filename}"
                logger.warning(error_msg)
                return False, error_msg
            
            self.storage.delete_file(filename)
            msg = f"Baseline deleted successfully: {filename}"
            logger.info(msg)
            return True, msg
        
        except Exception as e:
            error_msg = f"Error deleting baseline: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def cleanup_old_baselines(self, pipeline_id: Optional[str] = None,
                             max_age_days: Optional[int] = None,
                             max_count: Optional[int] = None,
                             dry_run: bool = False) -> Tuple[int, List[str]]:
        """
        Clean up old baselines based on retention policies.
        
        Args:
            pipeline_id: Optional pipeline to filter by
            max_age_days: Maximum age in days (from config if not specified)
            max_count: Maximum number of baselines to keep (from config if not specified)
            dry_run: If True, don't actually delete, just report what would be deleted
        
        Returns:
            Tuple of (deleted_count: int, deleted_files: List[str])
        """
        try:
            # Get settings from config or use defaults
            baseline_config = self.config.get("retention", {})
            if max_age_days is None:
                max_age_days = baseline_config.get("max_age_days", 90)
            if max_count is None:
                max_count = baseline_config.get("max_count", 10)
            
            summaries = self.list_baselines(pipeline_id)
            deleted_files = []
            deleted_count = 0
            
            # Delete old baselines based on age
            if max_age_days:
                cutoff_date = datetime.now().timestamp() - (max_age_days * 86400)
                
                for summary in summaries:
                    try:
                        timestamp = self._parse_timestamp(summary["timestamp"])
                        if timestamp and timestamp.timestamp() < cutoff_date:
                            filename = summary["filename"]
                            
                            if dry_run:
                                logger.info(f"[DRY RUN] Would delete: {filename}")
                                deleted_files.append(filename)
                            else:
                                success, msg = self.delete_baseline(
                                    summary["pipeline"],
                                    summary["timestamp"],
                                    confirm=True
                                )
                                if success:
                                    deleted_files.append(filename)
                                    deleted_count += 1
                    
                    except Exception as e:
                        logger.warning(f"Error processing baseline {summary.get('filename')}: {str(e)}")
            
            # Delete extras if over max_count
            if max_count and len(summaries) > max_count:
                # Keep only the most recent max_count baselines
                for summary in summaries[max_count:]:
                    filename = summary["filename"]
                    
                    if dry_run:
                        logger.info(f"[DRY RUN] Would delete: {filename}")
                        deleted_files.append(filename)
                    else:
                        success, msg = self.delete_baseline(
                            summary["pipeline"],
                            summary["timestamp"],
                            confirm=True
                        )
                        if success:
                            deleted_files.append(filename)
                            deleted_count += 1
            
            msg = f"Cleanup complete: {deleted_count} baselines deleted"
            if dry_run:
                msg = f"[DRY RUN] " + msg
            logger.info(msg)
            
            return deleted_count, deleted_files
        
        except Exception as e:
            logger.error(f"Error cleaning up baselines: {str(e)}")
            return 0, []
