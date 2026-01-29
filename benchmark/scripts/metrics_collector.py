"""
Snowflake Metrics Collection Module

Queries Snowflake system tables to extract comprehensive performance KPIs for executed dbt queries.
Implements correlation between dbt models and query IDs via query text JSON comments.
Includes retry logic for ACCOUNT_USAGE views with up to 45-minute latency.
"""

import snowflake.connector
import json
import logging
import time
import re
import os
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
import yaml


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Collects comprehensive performance metrics from Snowflake system tables.
    
    Attributes:
        connection: Snowflake connection object
        database: Target database name
        warehouse: Target warehouse name
        config_path: Path to snowflake.yaml configuration
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize MetricsCollector with Snowflake connection.
        
        Args:
            config_path: Path to snowflake.yaml (defaults to benchmark/config/snowflake.yaml)
        """
        if config_path is None:
            config_path = "benchmark/config/snowflake.yaml"
        
        self.config_path = Path(config_path)
        self.connection = None
        self.database = None
        self.warehouse = None
        self.config = None
        
        # Load configuration and establish connection
        self._load_config()
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
    
    def _get_basic_metrics(self, query_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Extract basic performance metrics from INFORMATION_SCHEMA.QUERY_HISTORY.
        
        Metrics extracted:
        - execution_time_ms (TOTAL_ELAPSED_TIME)
        - compilation_time_ms (COMPILATION_TIME)
        - bytes_scanned (BYTES_SCANNED)
        - rows_scanned (ROWS_PRODUCED)
        - partitions_scanned (PARTITIONS_SCANNED)
        - partitions_total (PARTITIONS_TOTAL)
        
        Args:
            query_ids: List of query IDs to extract metrics for
        
        Returns:
            Dict mapping query_id to metrics
        """
        logger.info(f"Extracting basic metrics for {len(query_ids)} queries")
        
        query = """
        SELECT
            QUERY_ID,
            TOTAL_ELAPSED_TIME as execution_time_ms,
            COMPILATION_TIME as compilation_time_ms,
            BYTES_SCANNED as bytes_scanned,
            ROWS_PRODUCED as rows_scanned,
            PARTITIONS_SCANNED as partitions_scanned,
            PARTITIONS_TOTAL as partitions_total,
            QUERY_TEXT
        FROM INFORMATION_SCHEMA.QUERY_HISTORY
        WHERE QUERY_ID IN ({})
        ORDER BY START_TIME DESC
        """.format(','.join(f"'{qid}'" for qid in query_ids))
        
        try:
            results = self._execute_query(query)
            metrics_dict = {}
            
            for row in results:
                query_id = row['QUERY_ID']
                metrics_dict[query_id] = {
                    'execution_time_ms': row['EXECUTION_TIME_MS'],
                    'compilation_time_ms': row['COMPILATION_TIME_MS'],
                    'bytes_scanned': row['BYTES_SCANNED'],
                    'rows_scanned': row['ROWS_SCANNED'],
                    'partitions_scanned': row['PARTITIONS_SCANNED'],
                    'partitions_total': row['PARTITIONS_TOTAL'],
                    'query_text': row['QUERY_TEXT']
                }
            
            logger.info(f"Extracted basic metrics for {len(metrics_dict)} queries")
            return metrics_dict
        
        except Exception as e:
            logger.error(f"Error extracting basic metrics: {str(e)}")
            raise
    
    def _get_credit_metrics(self, query_ids: List[str], max_retries: int = 3) -> Dict[str, Dict[str, Any]]:
        """
        Extract credit metrics from ACCOUNT_USAGE.QUERY_HISTORY with retry logic.
        
        ACCOUNT_USAGE has 45-minute latency, so implements exponential backoff.
        
        Args:
            query_ids: List of query IDs to extract metrics for
            max_retries: Maximum number of retry attempts
        
        Returns:
            Dict mapping query_id to credit metrics
        """
        logger.info(f"Extracting credit metrics for {len(query_ids)} queries (max_retries={max_retries})")
        
        query = """
        SELECT
            QUERY_ID,
            CREDITS_USED_CLOUD_SERVICES as warehouse_credits,
            BYTES_SPILLED_TO_LOCAL_STORAGE as spilling_to_local_storage_bytes,
            BYTES_SPILLED_TO_REMOTE_STORAGE as spilling_to_remote_storage_bytes
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE QUERY_ID IN ({})
        ORDER BY START_TIME DESC
        """.format(','.join(f"'{qid}'" for qid in query_ids))
        
        metrics_dict = {}
        
        for attempt in range(max_retries):
            try:
                results = self._execute_query(query)
                
                for row in results:
                    query_id = row['QUERY_ID']
                    metrics_dict[query_id] = {
                        'warehouse_credits': row['WAREHOUSE_CREDITS'],
                        'spilling_to_local_storage_bytes': row['SPILLING_TO_LOCAL_STORAGE_BYTES'],
                        'spilling_to_remote_storage_bytes': row['SPILLING_TO_REMOTE_STORAGE_BYTES']
                    }
                
                logger.info(f"Extracted credit metrics for {len(metrics_dict)} queries")
                return metrics_dict
            
            except Exception as e:
                if attempt < max_retries - 1:
                    # Calculate exponential backoff: 2^attempt * 120 seconds
                    wait_time = (2 ** attempt) * 120
                    logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    # Final attempt failed, log and return empty dict
                    logger.warning(f"Failed to extract credit metrics after {max_retries} attempts. Continuing without credit data.")
                    return metrics_dict
        
        return metrics_dict
    
    def _get_query_profile(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve detailed query profile using SYSTEM$GET_QUERY_PROFILE.
        
        Args:
            query_id: Query ID to get profile for
        
        Returns:
            Parsed query profile JSON or None if unavailable
        """
        try:
            query = f"SELECT SYSTEM$GET_QUERY_PROFILE('{query_id}')"
            results = self._execute_query(query)
            
            if results and len(results) > 0:
                profile_json_str = results[0].get('SYSTEM$GET_QUERY_PROFILE(\'{}\')')
                if profile_json_str:
                    return json.loads(profile_json_str)
            
            return None
        
        except Exception as e:
            logger.warning(f"Error retrieving query profile for {query_id}: {str(e)}")
            return None
    
    def _parse_query_profile(self, profile: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse query profile JSON to extract complexity metrics.
        
        Extracts:
        - join_count: Count of Join operators
        - subquery_depth: Maximum nesting level
        - window_function_count: Count of WindowFunction operators
        
        Args:
            profile: Query profile JSON from SYSTEM$GET_QUERY_PROFILE
        
        Returns:
            Dict with complexity metrics
        """
        metrics = {
            'join_count': 0,
            'subquery_depth': 0,
            'window_function_count': 0
        }
        
        try:
            if not profile or 'data' not in profile:
                return metrics
            
            data = profile['data']
            if 'plan' not in data or 'operators' not in data['plan']:
                return metrics
            
            operators = data['plan']['operators']
            
            # Count joins and window functions
            for operator in operators:
                operator_type = operator.get('type', '')
                
                if 'Join' in operator_type:
                    metrics['join_count'] += 1
                elif 'WindowFunction' in operator_type:
                    metrics['window_function_count'] += 1
            
            # Calculate subquery depth by analyzing nested references
            # This is determined by the depth property in the plan
            if 'subqueries' in data:
                # Subquery depth is number of nested subqueries + 1
                metrics['subquery_depth'] = len(data.get('subqueries', []))
            
            return metrics
        
        except Exception as e:
            logger.warning(f"Error parsing query profile: {str(e)}")
            return metrics
    
    def _extract_dbt_model_id(self, query_text: str) -> Optional[str]:
        """
        Parse dbt model name from query text JSON comment.
        
        dbt inserts comments like:
        /* {"app": "dbt", "dbt_version": "1.7.0", "node_id": "model.project.fact_portfolio_performance"} */
        
        Args:
            query_text: SQL query text
        
        Returns:
            Extracted node_id or None
        """
        try:
            # Look for JSON comment at start of query
            # Pattern: /* {...} */
            pattern = r'/\*\s*(\{[^}]*"node_id"[^}]*\})\s*\*/'
            match = re.search(pattern, query_text)
            
            if match:
                json_str = match.group(1)
                comment_json = json.loads(json_str)
                node_id = comment_json.get('node_id')
                
                if node_id:
                    # Extract model name from node_id
                    # Format: model.project.model_name
                    parts = node_id.split('.')
                    if len(parts) >= 3:
                        return '.'.join(parts[-2:])  # Return project.model_name
                    return node_id
            
            return None
        
        except Exception as e:
            logger.debug(f"Error extracting dbt model ID: {str(e)}")
            return None
    
    def _calculate_partition_pruning_ratio(self, partitions_scanned: Optional[int], partitions_total: Optional[int]) -> Optional[float]:
        """
        Calculate partition pruning ratio.
        
        Formula: 1 - (partitions_scanned / partitions_total)
        
        Args:
            partitions_scanned: Number of partitions scanned
            partitions_total: Total number of partitions
        
        Returns:
            Partition pruning ratio (0-1) or None if not applicable
        """
        if partitions_scanned is None or partitions_total is None:
            return None
        
        if partitions_total == 0:
            return None
        
        try:
            ratio = 1 - (partitions_scanned / partitions_total)
            return max(0, min(1, ratio))  # Clamp between 0 and 1
        
        except Exception as e:
            logger.debug(f"Error calculating partition pruning ratio: {str(e)}")
            return None
    
    def collect_metrics(self, query_ids: List[str], pipeline_name: str = None) -> Dict[str, Any]:
        """
        Collect comprehensive metrics for a list of query IDs.
        
        Returns metrics aggregated per model and per pipeline.
        
        Args:
            query_ids: List of Snowflake query IDs to collect metrics for
            pipeline_name: Optional name for pipeline-level aggregation
        
        Returns:
            Dict with structure:
            {
                'pipeline_name': str,
                'timestamp': str,
                'per_model': {
                    'model_name': {
                        'query_id': str,
                        'execution_time_ms': int,
                        'compilation_time_ms': int,
                        'bytes_scanned': int,
                        'rows_scanned': int,
                        'warehouse_credits': float,
                        'spilling_to_local_storage_bytes': int,
                        'spilling_to_remote_storage_bytes': int,
                        'partitions_scanned': int,
                        'partitions_total': int,
                        'partition_pruning_ratio': float,
                        'join_count': int,
                        'subquery_depth': int,
                        'window_function_count': int
                    }
                },
                'pipeline_aggregations': {
                    'total_execution_time_ms': int,
                    'total_compilation_time_ms': int,
                    'total_bytes_scanned': int,
                    'total_rows_scanned': int,
                    'total_warehouse_credits': float,
                    'total_spilling_bytes': int,
                    'model_count': int,
                    'avg_execution_time_ms': float,
                    'avg_join_count': float,
                    'avg_subquery_depth': float,
                    'avg_window_function_count': float
                }
            }
        """
        logger.info(f"Collecting metrics for {len(query_ids)} queries")
        
        if not query_ids:
            logger.warning("No query IDs provided")
            return {
                'pipeline_name': pipeline_name,
                'timestamp': datetime.now().isoformat() + 'Z',
                'per_model': {},
                'pipeline_aggregations': {}
            }
        
        # Collect metrics from different sources
        basic_metrics = self._get_basic_metrics(query_ids)
        credit_metrics = self._get_credit_metrics(query_ids)
        
        per_model_metrics = {}
        
        # Combine metrics per model
        for query_id, basic_metric in basic_metrics.items():
            # Extract dbt model name from query text
            model_name = self._extract_dbt_model_id(basic_metric.get('query_text', ''))
            
            if not model_name:
                # Fallback to query_id if model name not found
                model_name = f"unknown_{query_id[:8]}"
            
            # Get query profile
            profile = self._get_query_profile(query_id)
            profile_metrics = self._parse_query_profile(profile) if profile else {
                'join_count': 0,
                'subquery_depth': 0,
                'window_function_count': 0
            }
            
            # Get credit metrics for this query
            credit_metric = credit_metrics.get(query_id, {})
            
            # Calculate partition pruning ratio
            partition_pruning = self._calculate_partition_pruning_ratio(
                basic_metric.get('partitions_scanned'),
                basic_metric.get('partitions_total')
            )
            
            # Build per-model metrics
            model_metrics = {
                'query_id': query_id,
                'execution_time_ms': basic_metric.get('execution_time_ms'),
                'compilation_time_ms': basic_metric.get('compilation_time_ms'),
                'bytes_scanned': basic_metric.get('bytes_scanned'),
                'rows_scanned': basic_metric.get('rows_scanned'),
                'warehouse_credits': credit_metric.get('warehouse_credits'),
                'spilling_to_local_storage_bytes': credit_metric.get('spilling_to_local_storage_bytes'),
                'spilling_to_remote_storage_bytes': credit_metric.get('spilling_to_remote_storage_bytes'),
                'partitions_scanned': basic_metric.get('partitions_scanned'),
                'partitions_total': basic_metric.get('partitions_total'),
                'partition_pruning_ratio': partition_pruning,
                'join_count': profile_metrics.get('join_count', 0),
                'subquery_depth': profile_metrics.get('subquery_depth', 0),
                'window_function_count': profile_metrics.get('window_function_count', 0)
            }
            
            per_model_metrics[model_name] = model_metrics
        
        # Calculate pipeline-level aggregations
        pipeline_aggregations = self._aggregate_pipeline_metrics(per_model_metrics)
        
        return {
            'pipeline_name': pipeline_name,
            'timestamp': datetime.now().isoformat() + 'Z',
            'per_model': per_model_metrics,
            'pipeline_aggregations': pipeline_aggregations
        }
    
    def _aggregate_pipeline_metrics(self, per_model_metrics: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate per-model metrics to pipeline level.
        
        Args:
            per_model_metrics: Dictionary of model_name -> metrics
        
        Returns:
            Dict with pipeline-level aggregations
        """
        if not per_model_metrics:
            return {}
        
        aggregations = {
            'total_execution_time_ms': 0,
            'total_compilation_time_ms': 0,
            'total_bytes_scanned': 0,
            'total_rows_scanned': 0,
            'total_warehouse_credits': 0,
            'total_spilling_bytes': 0,
            'model_count': len(per_model_metrics),
            'avg_execution_time_ms': 0,
            'avg_join_count': 0,
            'avg_subquery_depth': 0,
            'avg_window_function_count': 0
        }
        
        join_counts = []
        subquery_depths = []
        window_counts = []
        
        for metrics in per_model_metrics.values():
            # Sum totals
            if metrics.get('execution_time_ms') is not None:
                aggregations['total_execution_time_ms'] += metrics['execution_time_ms']
            
            if metrics.get('compilation_time_ms') is not None:
                aggregations['total_compilation_time_ms'] += metrics['compilation_time_ms']
            
            if metrics.get('bytes_scanned') is not None:
                aggregations['total_bytes_scanned'] += metrics['bytes_scanned']
            
            if metrics.get('rows_scanned') is not None:
                aggregations['total_rows_scanned'] += metrics['rows_scanned']
            
            if metrics.get('warehouse_credits') is not None:
                aggregations['total_warehouse_credits'] += metrics['warehouse_credits']
            
            spill_local = metrics.get('spilling_to_local_storage_bytes') or 0
            spill_remote = metrics.get('spilling_to_remote_storage_bytes') or 0
            aggregations['total_spilling_bytes'] += spill_local + spill_remote
            
            # Collect for averages
            if metrics.get('join_count') is not None:
                join_counts.append(metrics['join_count'])
            
            if metrics.get('subquery_depth') is not None:
                subquery_depths.append(metrics['subquery_depth'])
            
            if metrics.get('window_function_count') is not None:
                window_counts.append(metrics['window_function_count'])
        
        # Calculate averages
        if per_model_metrics:
            aggregations['avg_execution_time_ms'] = aggregations['total_execution_time_ms'] / len(per_model_metrics)
        
        if join_counts:
            aggregations['avg_join_count'] = sum(join_counts) / len(join_counts)
        
        if subquery_depths:
            aggregations['avg_subquery_depth'] = sum(subquery_depths) / len(subquery_depths)
        
        if window_counts:
            aggregations['avg_window_function_count'] = sum(window_counts) / len(window_counts)
        
        return aggregations
    
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
    CLI entry point for metrics collection.
    
    Usage:
        python metrics_collector.py <query_id1> [query_id2] ... [--pipeline <name>] [--config <path>]
    """
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python metrics_collector.py <query_id1> [query_id2] ... [--pipeline <name>] [--config <path>]")
        sys.exit(1)
    
    # Parse arguments
    query_ids = []
    pipeline_name = None
    config_path = None
    
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--pipeline' and i + 1 < len(sys.argv):
            pipeline_name = sys.argv[i + 1]
            i += 2
        elif arg == '--config' and i + 1 < len(sys.argv):
            config_path = sys.argv[i + 1]
            i += 2
        elif not arg.startswith('--'):
            query_ids.append(arg)
            i += 1
        else:
            i += 1
    
    if not query_ids:
        print("Error: No query IDs provided")
        sys.exit(1)
    
    # Collect metrics
    try:
        collector = MetricsCollector(config_path=config_path)
        metrics = collector.collect_metrics(query_ids, pipeline_name=pipeline_name)
        
        # Print results
        print("\n" + "="*60)
        print("METRICS COLLECTION RESULTS")
        print("="*60)
        print(json.dumps(metrics, indent=2))
        
        collector.close()
    
    except Exception as e:
        logger.error(f"Error collecting metrics: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
