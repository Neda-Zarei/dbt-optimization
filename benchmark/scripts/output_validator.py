"""
Snowflake Output Validation Module

Verifies model outputs are identical before and after SQL optimizations
using order-agnostic hash comparison.

Validates datasets by:
1. Comparing row counts (fast check)
2. Computing order-agnostic row hashes using Snowflake HASH()
3. Aggregating row hashes for deterministic comparison
4. Storing and comparing baseline hashes
"""

import snowflake.connector
import json
import logging
import os
import hashlib
import re
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from pathlib import Path
import yaml


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OutputValidator:
    """
    Validates model outputs using order-agnostic hash comparison.
    
    Attributes:
        connection: Snowflake connection object
        database: Target database name
        warehouse: Target warehouse name
        config_path: Path to snowflake.yaml configuration
        pipelines_config: Pipeline configuration from pipelines.yaml
    """
    
    def __init__(self, config_path: str = None, pipelines_config_path: str = None):
        """
        Initialize OutputValidator with Snowflake connection.
        
        Args:
            config_path: Path to snowflake.yaml (defaults to benchmark/config/snowflake.yaml)
            pipelines_config_path: Path to pipelines.yaml (defaults to benchmark/config/pipelines.yaml)
        """
        if config_path is None:
            config_path = "benchmark/config/snowflake.yaml"
        if pipelines_config_path is None:
            pipelines_config_path = "benchmark/config/pipelines.yaml"
        
        self.config_path = Path(config_path)
        self.pipelines_config_path = Path(pipelines_config_path)
        self.connection = None
        self.database = None
        self.warehouse = None
        self.config = None
        self.pipelines_config = None
        
        # Load configurations and establish connection
        self._load_config()
        self._load_pipelines_config()
        self._connect()
    
    def _load_config(self) -> bool:
        """
        Load Snowflake configuration from snowflake.yaml.
        
        Returns:
            bool: True if config loaded successfully, False otherwise
        """
        logger.info(f"Loading Snowflake configuration from {self.config_path}")
        
        try:
            if not self.config_path.exists():
                error_msg = f"Configuration file not found: {self.config_path}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)
            
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if not config or 'snowflake' not in config:
                error_msg = "Invalid configuration: 'snowflake' key not found"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            self.config = config['snowflake']
            self.database = self.config.get('database')
            self.warehouse = self.config.get('warehouse')
            
            logger.info(f"Configuration loaded: database={self.database}, warehouse={self.warehouse}")
            return True
        
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise
    
    def _load_pipelines_config(self) -> bool:
        """
        Load pipeline configuration from pipelines.yaml.
        
        Returns:
            bool: True if config loaded successfully, False otherwise
        """
        logger.info(f"Loading pipeline configuration from {self.pipelines_config_path}")
        
        try:
            if not self.pipelines_config_path.exists():
                error_msg = f"Pipeline configuration file not found: {self.pipelines_config_path}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)
            
            with open(self.pipelines_config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if not config or 'pipelines' not in config:
                error_msg = "Invalid pipeline configuration: 'pipelines' key not found"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            self.pipelines_config = config['pipelines']
            logger.info(f"Pipeline configuration loaded successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error loading pipeline configuration: {str(e)}")
            raise
    
    def _resolve_env_var(self, value: str) -> str:
        """
        Resolve environment variables in config values.
        
        Handles format: "{{ env_var('VAR_NAME') }}"
        
        Args:
            value: Config value that may contain env var reference
        
        Returns:
            str: Resolved value
        """
        pattern = r'\{\{\s*env_var\([\'"](\w+)[\'"]\)\s*\}\}'
        match = re.match(pattern, value)
        
        if match:
            var_name = match.group(1)
            resolved = os.getenv(var_name)
            if not resolved:
                raise ValueError(f"Environment variable {var_name} not set")
            return resolved
        
        return value
    
    def _connect(self) -> bool:
        """
        Establish Snowflake connection using config credentials.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        logger.info("Establishing Snowflake connection")
        
        try:
            account = self._resolve_env_var(self.config.get('account'))
            user = self._resolve_env_var(self.config.get('user'))
            password = self._resolve_env_var(self.config.get('password'))
            
            self.connection = snowflake.connector.connect(
                account=account,
                user=user,
                password=password,
                database=self.database,
                warehouse=self.warehouse,
                role=self.config.get('role', 'ACCOUNTADMIN')
            )
            
            logger.info("Snowflake connection established successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {str(e)}")
            raise
    
    def _execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Execute a query against Snowflake.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
        
        Returns:
            List[Dict]: Query results as list of dictionaries
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            
            # Fetch column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all rows and convert to dicts
            rows = []
            for row in cursor.fetchall():
                rows.append(dict(zip(columns, row)))
            
            cursor.close()
            return rows
        
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def _get_schema_for_pipeline(self, pipeline_id: str) -> Optional[str]:
        """
        Get the schema name for a pipeline.
        
        Args:
            pipeline_id: Pipeline identifier (A, B, or C)
        
        Returns:
            Schema name or None if pipeline not found
        """
        if pipeline_id not in self.pipelines_config:
            logger.warning(f"Pipeline {pipeline_id} not found in configuration")
            return None
        
        schema = self.pipelines_config[pipeline_id].get('schema')
        return schema
    
    def _get_table_columns(self, schema: str, table: str) -> List[str]:
        """
        Get list of columns for a table using Snowflake metadata.
        
        Args:
            schema: Schema name
            table: Table/view name
        
        Returns:
            List of column names
        """
        try:
            query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema.upper()}'
              AND TABLE_NAME = '{table.upper()}'
            ORDER BY ORDINAL_POSITION
            """
            
            results = self._execute_query(query)
            columns = [row['COLUMN_NAME'] for row in results]
            
            if not columns:
                logger.warning(f"No columns found for {schema}.{table}")
            
            return columns
        
        except Exception as e:
            logger.error(f"Error getting table columns for {schema}.{table}: {str(e)}")
            raise
    
    def _get_row_count(self, schema: str, table: str) -> Optional[int]:
        """
        Get row count for a table.
        
        Args:
            schema: Schema name
            table: Table/view name
        
        Returns:
            Row count or None if query fails
        """
        try:
            query = f"SELECT COUNT(*) as row_count FROM {schema}.{table}"
            results = self._execute_query(query)
            
            if results:
                return results[0]['ROW_COUNT']
            
            return None
        
        except Exception as e:
            logger.error(f"Error getting row count for {schema}.{table}: {str(e)}")
            raise
    
    def _get_row_hashes(self, schema: str, table: str) -> List[str]:
        """
        Get order-agnostic row hashes using Snowflake HASH() function.
        
        Uses HASH() to hash entire rows, then sorts the hashes to make
        comparison order-agnostic.
        
        Args:
            schema: Schema name
            table: Table/view name
        
        Returns:
            Sorted list of row hashes
        """
        try:
            # Use HASH(*) to hash all columns in each row
            query = f"""
            SELECT HASH(*) as row_hash
            FROM {schema}.{table}
            ORDER BY row_hash
            """
            
            results = self._execute_query(query)
            
            # Extract hashes and convert to strings
            row_hashes = [str(row['ROW_HASH']) for row in results]
            
            logger.info(f"Retrieved {len(row_hashes)} row hashes from {schema}.{table}")
            return row_hashes
        
        except Exception as e:
            logger.error(f"Error getting row hashes for {schema}.{table}: {str(e)}")
            raise
    
    def _compute_aggregate_hash(self, row_hashes: List[str]) -> str:
        """
        Compute order-agnostic aggregate hash from sorted row hashes.
        
        Args:
            row_hashes: Sorted list of row hash strings
        
        Returns:
            Aggregate hash as hex string
        """
        # Concatenate all sorted hashes and compute SHA256
        concatenated = ''.join(row_hashes)
        aggregate_hash = hashlib.sha256(concatenated.encode()).hexdigest()
        
        return aggregate_hash
    
    def _get_baseline_path(self, pipeline_id: str, model_name: str) -> Path:
        """
        Get the file path for baseline hashes.
        
        Args:
            pipeline_id: Pipeline identifier
            model_name: Model name
        
        Returns:
            Path to baseline file
        """
        baselines_dir = Path("benchmark/baselines")
        baselines_dir.mkdir(parents=True, exist_ok=True)
        
        filename = f"baseline_{pipeline_id.lower()}_{model_name.lower()}.json"
        return baselines_dir / filename
    
    def _save_baseline(self, pipeline_id: str, model_name: str, schema: str, table: str, 
                      row_count: int, aggregate_hash: str) -> bool:
        """
        Save baseline hash and metadata to JSON file.
        
        Args:
            pipeline_id: Pipeline identifier
            model_name: Model name
            schema: Schema name
            table: Table name
            row_count: Row count
            aggregate_hash: Aggregate hash value
        
        Returns:
            bool: True if saved successfully
        """
        try:
            baseline_path = self._get_baseline_path(pipeline_id, model_name)
            
            baseline_data = {
                "pipeline": pipeline_id,
                "model": model_name,
                "schema": schema,
                "table": table,
                "captured_at": datetime.now().isoformat() + "Z",
                "row_count": row_count,
                "aggregate_hash": aggregate_hash
            }
            
            with open(baseline_path, 'w') as f:
                json.dump(baseline_data, f, indent=2)
            
            logger.info(f"Baseline saved for {pipeline_id}.{model_name} at {baseline_path}")
            return True
        
        except Exception as e:
            logger.error(f"Error saving baseline for {pipeline_id}.{model_name}: {str(e)}")
            raise
    
    def _load_baseline(self, pipeline_id: str, model_name: str) -> Optional[Dict[str, Any]]:
        """
        Load baseline hash and metadata from JSON file.
        
        Args:
            pipeline_id: Pipeline identifier
            model_name: Model name
        
        Returns:
            Baseline data dict or None if not found
        """
        try:
            baseline_path = self._get_baseline_path(pipeline_id, model_name)
            
            if not baseline_path.exists():
                logger.debug(f"No baseline found for {pipeline_id}.{model_name}")
                return None
            
            with open(baseline_path, 'r') as f:
                baseline_data = json.load(f)
            
            logger.debug(f"Baseline loaded for {pipeline_id}.{model_name}")
            return baseline_data
        
        except Exception as e:
            logger.error(f"Error loading baseline for {pipeline_id}.{model_name}: {str(e)}")
            raise
    
    def validate_model(self, pipeline_id: str, model_name: str) -> Dict[str, Any]:
        """
        Validate a single model by comparing with baseline.
        
        Performs:
        1. Row count comparison (fast check)
        2. Row hash comparison (detailed check)
        
        Args:
            pipeline_id: Pipeline identifier
            model_name: Model name (inferred from schema naming)
        
        Returns:
            Validation result dict with keys:
            - model: Model name
            - schema: Schema name
            - row_count_baseline: Baseline row count
            - row_count_candidate: Current row count
            - row_count_match: Boolean
            - hash_baseline: Baseline aggregate hash
            - hash_candidate: Current aggregate hash
            - hash_match: Boolean
            - status: 'PASS' or 'FAIL'
            - error: Optional error message
        """
        result = {
            "model": model_name,
            "schema": None,
            "row_count_baseline": None,
            "row_count_candidate": None,
            "row_count_match": False,
            "hash_baseline": None,
            "hash_candidate": None,
            "hash_match": False,
            "status": "FAIL",
            "error": None
        }
        
        try:
            # Get schema for pipeline
            schema = self._get_schema_for_pipeline(pipeline_id)
            if not schema:
                result["error"] = f"Schema not found for pipeline {pipeline_id}"
                return result
            
            result["schema"] = schema
            
            # Load baseline
            baseline = self._load_baseline(pipeline_id, model_name)
            if not baseline:
                result["error"] = f"No baseline found for {pipeline_id}.{model_name}"
                return result
            
            result["hash_baseline"] = baseline.get('aggregate_hash')
            result["row_count_baseline"] = baseline.get('row_count')
            
            # Get current row count
            row_count = self._get_row_count(schema, model_name)
            result["row_count_candidate"] = row_count
            
            # Compare row counts
            if row_count != result["row_count_baseline"]:
                result["row_count_match"] = False
                result["error"] = f"Row count mismatch: baseline {result['row_count_baseline']}, candidate {row_count}"
                return result
            
            result["row_count_match"] = True
            
            # Get current row hashes
            row_hashes = self._get_row_hashes(schema, model_name)
            
            # Compute aggregate hash
            aggregate_hash = self._compute_aggregate_hash(row_hashes)
            result["hash_candidate"] = aggregate_hash
            
            # Compare hashes
            if aggregate_hash == result["hash_baseline"]:
                result["hash_match"] = True
                result["status"] = "PASS"
            else:
                result["hash_match"] = False
                result["error"] = f"Hash mismatch: baseline {result['hash_baseline']}, candidate {aggregate_hash}"
            
            return result
        
        except Exception as e:
            result["error"] = f"Error validating model: {str(e)}"
            logger.error(result["error"])
            return result
    
    def capture_baseline(self, pipeline_id: str, model_names: List[str]) -> Dict[str, Any]:
        """
        Capture baseline hashes for a list of models.
        
        Args:
            pipeline_id: Pipeline identifier
            model_names: List of model names to capture
        
        Returns:
            Dict with capture results for each model
        """
        logger.info(f"Capturing baselines for pipeline {pipeline_id}")
        
        capture_results = {
            "pipeline": pipeline_id,
            "timestamp": datetime.now().isoformat() + "Z",
            "models_captured": 0,
            "results": {}
        }
        
        try:
            # Get schema for pipeline
            schema = self._get_schema_for_pipeline(pipeline_id)
            if not schema:
                logger.error(f"Schema not found for pipeline {pipeline_id}")
                return capture_results
            
            for model_name in model_names:
                logger.info(f"Capturing baseline for {pipeline_id}.{model_name}")
                
                try:
                    # Get row count
                    row_count = self._get_row_count(schema, model_name)
                    
                    # Get row hashes
                    row_hashes = self._get_row_hashes(schema, model_name)
                    
                    # Compute aggregate hash
                    aggregate_hash = self._compute_aggregate_hash(row_hashes)
                    
                    # Save baseline
                    self._save_baseline(pipeline_id, model_name, schema, model_name, row_count, aggregate_hash)
                    
                    capture_results["results"][model_name] = {
                        "status": "SUCCESS",
                        "row_count": row_count,
                        "aggregate_hash": aggregate_hash
                    }
                    capture_results["models_captured"] += 1
                
                except Exception as e:
                    error_msg = f"Error capturing baseline for {model_name}: {str(e)}"
                    logger.error(error_msg)
                    capture_results["results"][model_name] = {
                        "status": "FAILED",
                        "error": error_msg
                    }
            
            return capture_results
        
        except Exception as e:
            logger.error(f"Error in capture_baseline: {str(e)}")
            return capture_results
    
    def validate(self, pipeline_id: str, model_names: List[str]) -> Dict[str, Any]:
        """
        Validate a list of models against baselines.
        
        Args:
            pipeline_id: Pipeline identifier (A, B, or C)
            model_names: List of model names to validate
        
        Returns:
            Validation report dict with keys:
            - validation_timestamp: ISO format timestamp
            - pipeline: Pipeline ID
            - models_validated: Count of models validated
            - overall_status: 'PASS' if all models pass, 'FAIL' otherwise
            - results: List of per-model validation results
        """
        logger.info(f"Starting validation for pipeline {pipeline_id}")
        
        report = {
            "validation_timestamp": datetime.now().isoformat() + "Z",
            "pipeline": pipeline_id,
            "models_validated": 0,
            "overall_status": "PASS",
            "results": []
        }
        
        if not model_names:
            logger.warning("No models provided for validation")
            return report
        
        all_passed = True
        
        for model_name in model_names:
            logger.info(f"Validating model {model_name}")
            
            result = self.validate_model(pipeline_id, model_name)
            report["results"].append(result)
            report["models_validated"] += 1
            
            if result["status"] != "PASS":
                all_passed = False
                logger.warning(f"Validation FAILED for {model_name}: {result.get('error', 'Unknown error')}")
            else:
                logger.info(f"Validation PASSED for {model_name}")
        
        # Set overall status
        report["overall_status"] = "PASS" if all_passed else "FAIL"
        
        logger.info(f"Validation complete for pipeline {pipeline_id}: {report['overall_status']}")
        
        return report
    
    def close(self):
        """Close Snowflake connection."""
        try:
            if self.connection:
                self.connection.close()
                logger.info("Snowflake connection closed")
        except Exception as e:
            logger.warning(f"Error closing connection: {str(e)}")


def main():
    """
    CLI entry point for output validation.
    
    Usage:
        python output_validator.py --pipeline <id> --models <model1> [model2] ... [--baseline]
    
    Options:
        --pipeline <id>: Pipeline identifier (A, B, or C)
        --models <list>: Space-separated list of model names
        --baseline: Capture baseline instead of validating
        --config <path>: Path to snowflake.yaml (optional)
    """
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python output_validator.py --pipeline <id> --models <model1> [model2] ... [--baseline]")
        print("Example: python output_validator.py --pipeline C --models fact_portfolio_performance market_data --baseline")
        sys.exit(1)
    
    # Parse arguments
    pipeline_id = None
    model_names = []
    capture_baseline = False
    config_path = None
    
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        
        if arg == '--pipeline' and i + 1 < len(sys.argv):
            pipeline_id = sys.argv[i + 1]
            i += 2
        elif arg == '--models' and i + 1 < len(sys.argv):
            # Collect all model names until next flag
            i += 1
            while i < len(sys.argv) and not sys.argv[i].startswith('--'):
                model_names.append(sys.argv[i])
                i += 1
        elif arg == '--baseline':
            capture_baseline = True
            i += 1
        elif arg == '--config' and i + 1 < len(sys.argv):
            config_path = sys.argv[i + 1]
            i += 2
        else:
            i += 1
    
    # Validate arguments
    if not pipeline_id:
        print("Error: --pipeline argument required")
        sys.exit(1)
    
    if not model_names:
        print("Error: --models argument required with at least one model")
        sys.exit(1)
    
    try:
        # Initialize validator
        if config_path:
            validator = OutputValidator(config_path=config_path)
        else:
            validator = OutputValidator()
        
        # Perform operation
        if capture_baseline:
            logger.info(f"Capturing baseline for pipeline {pipeline_id}")
            results = validator.capture_baseline(pipeline_id, model_names)
        else:
            logger.info(f"Validating pipeline {pipeline_id}")
            results = validator.validate(pipeline_id, model_names)
        
        # Print results
        print("\n" + "="*60)
        print("OUTPUT VALIDATION RESULTS" if not capture_baseline else "BASELINE CAPTURE RESULTS")
        print("="*60)
        print(json.dumps(results, indent=2))
        
        validator.close()
        
        # Exit with appropriate code
        if capture_baseline:
            sys.exit(0)
        else:
            # Validation: exit 0 for PASS, 1 for FAIL
            sys.exit(0 if results.get('overall_status') == 'PASS' else 1)
    
    except Exception as e:
        logger.error(f"Error in validation: {str(e)}")
        print(json.dumps({"error": str(e)}, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
