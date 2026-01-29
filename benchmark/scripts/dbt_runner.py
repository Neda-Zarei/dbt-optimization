"""
dbt Pipeline Execution Wrapper

Orchestrates dbt runs for specific pipelines with automatic dependency resolution.
Handles pipeline selection, dependency execution, and metadata capture.
"""

import subprocess
import yaml
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineRunner:
    """
    Orchestrates dbt execution for specific pipelines with dependency resolution.
    
    Attributes:
        pipeline_id: Pipeline identifier (A, B, or C)
        config_path: Path to pipelines.yaml configuration
        project_root: Root directory of dbt project
    """
    
    def __init__(self, pipeline_id: str, config_path: str = None, project_root: str = None):
        """
        Initialize the PipelineRunner.
        
        Args:
            pipeline_id: Pipeline identifier (A, B, or C)
            config_path: Path to pipelines.yaml (defaults to benchmark/config/pipelines.yaml)
            project_root: Root directory of dbt project (defaults to current directory)
        """
        self.pipeline_id = pipeline_id.upper()
        
        # Set default paths
        if config_path is None:
            config_path = "benchmark/config/pipelines.yaml"
        if project_root is None:
            project_root = "."
        
        self.config_path = Path(config_path)
        self.project_root = Path(project_root)
        
        self.config = None
        self.pipelines_config = None
        self.execution_results = {
            "pipeline": self.pipeline_id,
            "target_schema": None,
            "dependencies_executed": [],
            "execution_start": None,
            "execution_end": None,
            "dbt_version": None,
            "models_executed": [],
            "success": False,
            "errors": []
        }
    
    def load_config(self) -> bool:
        """
        Load and validate pipeline configuration from pipelines.yaml.
        
        Returns:
            bool: True if config loaded successfully, False otherwise
        """
        logger.info(f"Loading pipeline configuration from {self.config_path}")
        
        try:
            if not self.config_path.exists():
                error_msg = f"Configuration file not found: {self.config_path}"
                logger.error(error_msg)
                self.execution_results["errors"].append(error_msg)
                return False
            
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            
            if not self.config or 'pipelines' not in self.config:
                error_msg = "Invalid configuration: 'pipelines' key not found"
                logger.error(error_msg)
                self.execution_results["errors"].append(error_msg)
                return False
            
            self.pipelines_config = self.config['pipelines']
            
            # Validate pipeline exists
            if self.pipeline_id not in self.pipelines_config:
                error_msg = f"Pipeline {self.pipeline_id} not found in configuration"
                logger.error(error_msg)
                self.execution_results["errors"].append(error_msg)
                return False
            
            # Validate all dependencies exist
            pipeline_config = self.pipelines_config[self.pipeline_id]
            dependencies = pipeline_config.get('dependencies', [])
            for dep in dependencies:
                if dep not in self.pipelines_config:
                    error_msg = f"Dependency pipeline {dep} not found in configuration"
                    logger.error(error_msg)
                    self.execution_results["errors"].append(error_msg)
                    return False
            
            # Validate required keys in pipeline config
            required_keys = ['name', 'schema', 'models', 'dependencies']
            for key in required_keys:
                if key not in pipeline_config:
                    error_msg = f"Pipeline {self.pipeline_id} missing required key: {key}"
                    logger.error(error_msg)
                    self.execution_results["errors"].append(error_msg)
                    return False
            
            logger.info(f"Pipeline configuration loaded successfully for {self.pipeline_id}")
            self.execution_results["target_schema"] = pipeline_config.get('schema')
            return True
        
        except yaml.YAMLError as e:
            error_msg = f"YAML parsing error: {str(e)}"
            logger.error(error_msg)
            self.execution_results["errors"].append(error_msg)
            return False
        except Exception as e:
            error_msg = f"Error loading configuration: {str(e)}"
            logger.error(error_msg)
            self.execution_results["errors"].append(error_msg)
            return False
    
    def resolve_dependencies(self) -> List[str]:
        """
        Resolve the dependency chain for the target pipeline.
        
        Returns:
            List[str]: Ordered list of pipeline IDs to execute (including target pipeline)
        """
        logger.info(f"Resolving dependencies for pipeline {self.pipeline_id}")
        
        execution_order = []
        visited = set()
        
        def visit_pipeline(pipeline_id: str):
            """Recursively visit pipeline and its dependencies."""
            if pipeline_id in visited:
                return
            
            visited.add(pipeline_id)
            
            if pipeline_id not in self.pipelines_config:
                logger.warning(f"Pipeline {pipeline_id} not found in configuration")
                return
            
            # Process dependencies first
            dependencies = self.pipelines_config[pipeline_id].get('dependencies', [])
            for dep in dependencies:
                visit_pipeline(dep)
            
            # Then add the pipeline itself
            execution_order.append(pipeline_id)
        
        # Start with target pipeline
        visit_pipeline(self.pipeline_id)
        
        logger.info(f"Execution order determined: {execution_order}")
        return execution_order
    
    def get_dbt_version(self) -> Optional[str]:
        """
        Get the installed dbt version.
        
        Returns:
            str: dbt version or None if unable to retrieve
        """
        try:
            result = subprocess.run(
                ["dbt", "--version"],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                # dbt version output typically starts with "dbt version X.Y.Z"
                for line in result.stdout.split('\n'):
                    if 'dbt version' in line.lower():
                        # Extract version string (format: "dbt version X.Y.Z")
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            return parts[2]
                return None
            
            logger.warning("Unable to retrieve dbt version")
            return None
        
        except Exception as e:
            logger.warning(f"Error retrieving dbt version: {str(e)}")
            return None
    
    def execute_dbt(self, pipeline_id: str, capture_models: bool = False) -> tuple[bool, List[str], List[str]]:
        """
        Execute a dbt run for a specific pipeline.
        
        Args:
            pipeline_id: Pipeline identifier to execute
            capture_models: Whether to capture executed model names
        
        Returns:
            tuple: (success: bool, models_executed: List[str], stderr_lines: List[str])
        """
        pipeline_config = self.pipelines_config[pipeline_id]
        model_selector = pipeline_config.get('models')
        schema_name = pipeline_config.get('schema')
        
        dbt_command = ["dbt", "run", "--select", model_selector]
        
        logger.info(f"Executing pipeline {pipeline_id}: {' '.join(dbt_command)}")
        logger.info(f"Target schema: {schema_name}")
        
        try:
            result = subprocess.run(
                dbt_command,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
            
            # Capture stderr for debugging
            stderr_lines = result.stderr.split('\n') if result.stderr else []
            
            # Parse stdout to extract model names if requested
            models_executed = []
            if capture_models and result.stdout:
                models_executed = self._parse_dbt_output(result.stdout)
            
            if result.returncode == 0:
                logger.info(f"Pipeline {pipeline_id} executed successfully")
                return True, models_executed, stderr_lines
            else:
                error_msg = f"dbt run failed for pipeline {pipeline_id}"
                logger.error(error_msg)
                logger.error(f"stderr: {result.stderr}")
                return False, models_executed, stderr_lines
        
        except subprocess.TimeoutExpired:
            error_msg = f"dbt execution for pipeline {pipeline_id} timed out"
            logger.error(error_msg)
            return False, [], []
        except Exception as e:
            error_msg = f"Error executing dbt for pipeline {pipeline_id}: {str(e)}"
            logger.error(error_msg)
            return False, [], []
    
    def _parse_dbt_output(self, dbt_output: str) -> List[str]:
        """
        Parse dbt stdout to extract executed model names.
        
        dbt output format includes lines like:
        - "1 of 5 START sql view model schema.model_name"
        - "1 of 5 OK created sql view model schema.model_name"
        
        Args:
            dbt_output: stdout from dbt run
        
        Returns:
            List[str]: List of executed model names
        """
        models = []
        
        for line in dbt_output.split('\n'):
            # Look for lines with model execution info
            if 'sql view model' in line or 'sql table model' in line or 'sql model' in line:
                # Try to extract model name (appears after the last dot before status)
                parts = line.split()
                for i, part in enumerate(parts):
                    # Look for qualified model name pattern (schema.model_name)
                    if '.' in part and not part.startswith('('):
                        # Extract just the model name (after the last dot)
                        model_name = part.split('.')[-1]
                        if model_name and model_name not in models:
                            models.append(model_name)
                            break
        
        if models:
            logger.info(f"Extracted {len(models)} model names from dbt output")
        
        return models
    
    def run(self) -> Dict[str, Any]:
        """
        Execute the full pipeline run with dependency resolution.
        
        Returns:
            Dict containing execution results with keys:
            - pipeline: Target pipeline ID
            - target_schema: Target schema name
            - dependencies_executed: List of dependency pipeline IDs executed
            - execution_start: ISO format timestamp of start
            - execution_end: ISO format timestamp of end
            - dbt_version: Installed dbt version
            - models_executed: List of model names executed (for target pipeline)
            - success: Boolean success status
            - errors: List of error messages
        """
        logger.info(f"Starting pipeline execution for {self.pipeline_id}")
        
        # Record start time
        self.execution_results["execution_start"] = datetime.now().isoformat() + "Z"
        
        # Load configuration
        if not self.load_config():
            self.execution_results["execution_end"] = datetime.now().isoformat() + "Z"
            return self.execution_results
        
        # Get dbt version
        dbt_version = self.get_dbt_version()
        if dbt_version:
            self.execution_results["dbt_version"] = dbt_version
            logger.info(f"dbt version: {dbt_version}")
        
        # Resolve dependencies
        execution_order = self.resolve_dependencies()
        
        # Execute each pipeline in order
        dependency_ids = execution_order[:-1]  # All except the last (target pipeline)
        self.execution_results["dependencies_executed"] = dependency_ids
        
        for i, pipeline_id in enumerate(execution_order):
            is_target_pipeline = (pipeline_id == self.pipeline_id)
            
            # Execute without metrics capture for dependencies
            success, models, stderr_lines = self.execute_dbt(
                pipeline_id,
                capture_models=is_target_pipeline
            )
            
            if is_target_pipeline:
                # Store results for target pipeline
                self.execution_results["models_executed"] = models
            
            if not success:
                error_msg = f"Execution failed for pipeline {pipeline_id}"
                logger.error(error_msg)
                self.execution_results["errors"].append(error_msg)
                if stderr_lines:
                    self.execution_results["errors"].extend(stderr_lines)
                self.execution_results["execution_end"] = datetime.now().isoformat() + "Z"
                return self.execution_results
        
        # All pipelines executed successfully
        self.execution_results["success"] = True
        self.execution_results["execution_end"] = datetime.now().isoformat() + "Z"
        
        logger.info(f"Pipeline {self.pipeline_id} execution completed successfully")
        logger.info(f"Execution took {len(execution_order)} pipeline(s) and {len(self.execution_results['models_executed'])} models")
        
        return self.execution_results


def main():
    """
    CLI entry point for dbt_runner script.
    
    Usage:
        python dbt_runner.py <pipeline_id> [config_path] [project_root]
    
    Arguments:
        pipeline_id: Pipeline identifier (A, B, or C)
        config_path: Optional path to pipelines.yaml
        project_root: Optional path to dbt project root
    """
    if len(sys.argv) < 2:
        print("Usage: python dbt_runner.py <pipeline_id> [config_path] [project_root]")
        print("Example: python dbt_runner.py C")
        sys.exit(1)
    
    pipeline_id = sys.argv[1]
    config_path = sys.argv[2] if len(sys.argv) > 2 else "benchmark/config/pipelines.yaml"
    project_root = sys.argv[3] if len(sys.argv) > 3 else "."
    
    runner = PipelineRunner(pipeline_id, config_path, project_root)
    results = runner.run()
    
    # Print results as structured output
    import json
    print("\n" + "="*60)
    print("EXECUTION RESULTS")
    print("="*60)
    print(json.dumps(results, indent=2))
    
    # Exit with appropriate code
    sys.exit(0 if results["success"] else 1)


if __name__ == "__main__":
    main()
