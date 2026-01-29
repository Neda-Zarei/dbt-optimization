"""
Benchmark Command-Line Interface

Provides user-friendly commands for all benchmarking operations:
- Capture baselines for pipelines
- Run benchmarks and compare against baselines
- List, delete, and compare baselines
- Generate reports with metrics and violations
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import Optional, List
from datetime import datetime
import json

from baseline_manager import BaselineManager
from comparison_engine import ComparisonEngine
from report_generator import ReportGenerator


# Color codes for terminal output
class Colors:
    """ANSI color codes for terminal output."""
    RESET = '\033[0m'
    BOLD = '\033[1m'
    DIM = '\033[2m'
    
    # Foreground colors
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'


class OutputFormatter:
    """Formats output with colors and styles."""
    
    def __init__(self, use_color: bool = True):
        """
        Initialize OutputFormatter.
        
        Args:
            use_color: Whether to use colored output
        """
        self.use_color = use_color
    
    def _colorize(self, text: str, color: str) -> str:
        """Apply color to text if color is enabled."""
        if not self.use_color:
            return text
        return f"{color}{text}{Colors.RESET}"
    
    def success(self, text: str) -> str:
        """Format success message in green."""
        return self._colorize(text, Colors.GREEN)
    
    def error(self, text: str) -> str:
        """Format error message in red."""
        return self._colorize(text, Colors.RED)
    
    def warning(self, text: str) -> str:
        """Format warning message in yellow."""
        return self._colorize(text, Colors.YELLOW)
    
    def info(self, text: str) -> str:
        """Format info message in blue."""
        return self._colorize(text, Colors.BLUE)
    
    def header(self, text: str) -> str:
        """Format header in bold cyan."""
        return self._colorize(f"\n{text}\n{'=' * len(text)}", Colors.CYAN + Colors.BOLD)
    
    def subheader(self, text: str) -> str:
        """Format subheader in bold."""
        return self._colorize(f"{text}", Colors.BOLD)
    
    def dim(self, text: str) -> str:
        """Format text in dim color."""
        return self._colorize(text, Colors.DIM)


class ProgressIndicator:
    """Simple progress indicator for long-running operations."""
    
    def __init__(self, use_color: bool = True):
        """Initialize ProgressIndicator."""
        self.use_color = use_color
        self.formatter = OutputFormatter(use_color)
    
    def step(self, step_num: int, total: int, message: str) -> None:
        """Print a progress step."""
        progress = f"[{step_num}/{total}]"
        print(f"{self.formatter.dim(progress)} {message}")


class BenchmarkCLI:
    """Main CLI class for benchmark operations."""
    
    def __init__(self, use_color: bool = True, verbose: bool = False):
        """
        Initialize BenchmarkCLI.
        
        Args:
            use_color: Whether to use colored output
            verbose: Whether to enable verbose logging
        """
        self.use_color = use_color
        self.verbose = verbose
        self.formatter = OutputFormatter(use_color)
        self.progress = ProgressIndicator(use_color)
        
        # Configure logging
        log_level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def _validate_pipeline(self, pipeline: str) -> bool:
        """
        Validate that pipeline is A, B, or C.
        
        Args:
            pipeline: Pipeline identifier
        
        Returns:
            bool: True if valid, False otherwise
        """
        if pipeline.upper() not in ['A', 'B', 'C']:
            print(self.formatter.error(f"Error: Invalid pipeline '{pipeline}'"))
            print(f"Valid pipelines are: A, B, C")
            return False
        return True
    
    def _format_baseline_summary(self, summary: dict) -> str:
        """
        Format a baseline summary for display.
        
        Args:
            summary: Baseline summary dictionary
        
        Returns:
            Formatted string for display
        """
        lines = []
        
        # Basic info
        lines.append(f"  Pipeline:  {summary.get('pipeline', 'unknown')}")
        lines.append(f"  Timestamp: {summary.get('captured_at', 'unknown')}")
        lines.append(f"  Status:    {self.formatter.success(summary.get('status', 'unknown')) if summary.get('status') == 'SUCCESS' else self.formatter.error(summary.get('status', 'unknown'))}")
        
        # Execution details
        exec_time = summary.get('execution_time')
        if exec_time:
            lines.append(f"  Duration:  {exec_time:.2f}s")
        
        models_count = summary.get('models_executed', 0)
        lines.append(f"  Models:    {models_count}")
        
        # Optional metadata
        dbt_version = summary.get('dbt_version')
        if dbt_version:
            lines.append(f"  dbt:       {dbt_version}")
        
        git_commit = summary.get('git_commit')
        if git_commit:
            lines.append(f"  Git:       {git_commit[:8]}...")
        
        filename = summary.get('filename')
        if filename:
            lines.append(f"  Filename:  {filename}")
        
        return "\n".join(lines)
    
    def capture_baseline_command(self, pipeline: str, output_dir: Optional[str] = None) -> int:
        """
        Capture a baseline for a pipeline.
        
        Args:
            pipeline: Pipeline identifier (A, B, or C)
            output_dir: Optional output directory override
        
        Returns:
            Exit code (0 for success, 1 for failure)
        """
        # Validate pipeline
        if not self._validate_pipeline(pipeline):
            return 1
        
        pipeline = pipeline.upper()
        
        print(self.formatter.header(f"Capturing Baseline for Pipeline {pipeline}"))
        
        try:
            # Initialize baseline manager
            self.progress.step(1, 4, "Initializing baseline manager...")
            manager = BaselineManager()
            
            # Capture baseline
            self.progress.step(2, 4, f"Running pipeline {pipeline}...")
            baseline_data = manager.capture_baseline(
                pipeline,
                project_root=".",
                metrics_enabled=True,
                validation_enabled=True
            )
            
            # Check if capture was successful
            if baseline_data.get('summary', {}).get('status') != 'SUCCESS':
                errors = baseline_data.get('summary', {}).get('errors', [])
                print(self.formatter.error("❌ Failed to capture baseline"))
                for error in errors:
                    print(f"  - {error}")
                return 1
            
            # Save baseline
            self.progress.step(3, 4, "Saving baseline...")
            success, result = manager.save_baseline(baseline_data, force=False)
            
            if not success:
                print(self.formatter.error(f"❌ Failed to save baseline: {result}"))
                return 1
            
            # Display results
            self.progress.step(4, 4, "Baseline captured successfully")
            print()
            print(self.formatter.success("✓ Baseline captured successfully"))
            print(f"  Filename: {result}")
            print(f"  Pipeline: {pipeline}")
            
            # Show execution details
            exec_context = baseline_data.get('execution_context', {})
            if exec_context.get('duration_seconds'):
                print(f"  Duration: {exec_context['duration_seconds']:.2f}s")
            
            models_executed = exec_context.get('models_executed', [])
            print(f"  Models:   {len(models_executed)}")
            
            return 0
        
        except Exception as e:
            print(self.formatter.error(f"❌ Error capturing baseline: {str(e)}"))
            if self.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    def run_benchmark_command(self, pipeline: str, output_dir: Optional[str] = None) -> int:
        """
        Run benchmark and compare against latest baseline.
        
        Args:
            pipeline: Pipeline identifier (A, B, or C)
            output_dir: Optional output directory override
        
        Returns:
            Exit code (0 for pass, 1 for warning, 2 for error)
        """
        # Validate pipeline
        if not self._validate_pipeline(pipeline):
            return 1
        
        pipeline = pipeline.upper()
        
        print(self.formatter.header(f"Running Benchmark for Pipeline {pipeline}"))
        
        try:
            # Initialize managers
            self.progress.step(1, 5, "Initializing managers...")
            baseline_manager = BaselineManager()
            comparison_engine = ComparisonEngine()
            
            # Load latest baseline
            self.progress.step(2, 5, f"Loading baseline for pipeline {pipeline}...")
            baseline = baseline_manager.load_baseline(pipeline)
            
            if baseline is None:
                print(self.formatter.warning(f"⚠ No baseline found for pipeline {pipeline}"))
                print(f"  Run 'capture-baseline --pipeline {pipeline}' first")
                return 1
            
            baseline_timestamp = baseline.get('captured_at', 'unknown')
            print(f"  Baseline: {baseline_timestamp}")
            
            # Run candidate
            self.progress.step(3, 5, f"Running pipeline {pipeline}...")
            candidate_data = baseline_manager.capture_baseline(
                pipeline,
                project_root=".",
                metrics_enabled=True,
                validation_enabled=True
            )
            
            if candidate_data.get('summary', {}).get('status') != 'SUCCESS':
                errors = candidate_data.get('summary', {}).get('errors', [])
                print(self.formatter.error("❌ Failed to run pipeline"))
                for error in errors:
                    print(f"  - {error}")
                return 2
            
            candidate_timestamp = candidate_data.get('captured_at', 'unknown')
            print(f"  Candidate: {candidate_timestamp}")
            
            # Compare results
            self.progress.step(4, 5, "Comparing results...")
            comparison = comparison_engine.compare_pipeline(baseline, candidate_data)
            status, exit_code = comparison_engine.generate_summary(comparison)
            
            # Generate report
            self.progress.step(5, 5, "Generating report...")
            report_output_dir = output_dir or "benchmark/results"
            report_generator = ReportGenerator(pipeline, output_directory=report_output_dir)
            
            # Add metadata and results to report
            exec_context = candidate_data.get('execution_context', {})
            report_generator.add_metadata(
                git_commit=exec_context.get('git_commit'),
                dbt_version=exec_context.get('dbt_version')
            )
            
            # Add comparison results
            violation_counts = comparison.count_violations_by_severity()
            report_generator.add_comparison_results(
                overall_status=status.name.lower(),
                baseline_timestamp=baseline_timestamp,
                baseline_git_commit=baseline.get('execution_context', {}).get('git_commit'),
                violations_summary={
                    'total_violations': sum(violation_counts.values()),
                    'by_severity': violation_counts
                }
            )
            
            report_generator.generate_summary()
            report_path = report_generator.save()
            
            print()
            print(self.formatter.header("Benchmark Results"))
            print(f"Status: {self._format_status(status)}")
            
            # Show violation summary
            print()
            print(self.formatter.subheader("Violation Summary"))
            print(f"  Info:    {violation_counts['INFO']:>3} violations")
            print(f"  Warning: {violation_counts['WARNING']:>3} violations")
            print(f"  Error:   {violation_counts['ERROR']:>3} violations")
            
            if exit_code == 0:
                print()
                print(self.formatter.success("✓ Benchmark passed - no regressions detected"))
            elif exit_code == 1:
                print()
                print(self.formatter.warning("⚠ Benchmark completed with warnings"))
            else:
                print()
                print(self.formatter.error("✗ Benchmark failed - regressions detected"))
            
            print()
            print(f"Report: {report_path}")
            
            return exit_code
        
        except Exception as e:
            print(self.formatter.error(f"❌ Error running benchmark: {str(e)}"))
            if self.verbose:
                import traceback
                traceback.print_exc()
            return 2
    
    def list_baselines_command(self, pipeline: Optional[str] = None) -> int:
        """
        List available baselines.
        
        Args:
            pipeline: Optional pipeline filter (A, B, or C)
        
        Returns:
            Exit code (0 for success, 1 for failure)
        """
        if pipeline and not self._validate_pipeline(pipeline):
            return 1
        
        pipeline = pipeline.upper() if pipeline else None
        
        print(self.formatter.header("Available Baselines"))
        
        try:
            manager = BaselineManager()
            baselines = manager.list_baselines(pipeline)
            
            if not baselines:
                print("No baselines found.")
                if pipeline:
                    print(f"  Run 'capture-baseline --pipeline {pipeline}' to create one")
                else:
                    print(f"  Run 'capture-baseline --pipeline <A|B|C>' to create one")
                return 0
            
            # Group by pipeline
            by_pipeline = {}
            for baseline in baselines:
                p = baseline.get('pipeline', 'unknown')
                if p not in by_pipeline:
                    by_pipeline[p] = []
                by_pipeline[p].append(baseline)
            
            # Display baselines
            for p in sorted(by_pipeline.keys()):
                print()
                print(self.formatter.subheader(f"Pipeline {p}"))
                print("-" * 80)
                
                for i, baseline in enumerate(by_pipeline[p], 1):
                    print(self._format_baseline_summary(baseline))
                    if i < len(by_pipeline[p]):
                        print()
            
            return 0
        
        except Exception as e:
            print(self.formatter.error(f"❌ Error listing baselines: {str(e)}"))
            if self.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    def delete_baseline_command(self, baseline_id: str) -> int:
        """
        Delete a baseline by filename.
        
        Args:
            baseline_id: Baseline filename or identifier
        
        Returns:
            Exit code (0 for success, 1 for failure)
        """
        print(self.formatter.header("Delete Baseline"))
        
        try:
            manager = BaselineManager()
            
            # Check if baseline exists
            if not manager.storage.file_exists(baseline_id):
                print(self.formatter.error(f"❌ Baseline not found: {baseline_id}"))
                print(f"Use 'list-baselines' to see available baselines.")
                return 1
            
            # Show baseline details before deletion
            print(f"Baseline to delete:")
            print(f"  {baseline_id}")
            print()
            
            # Confirm deletion
            try:
                response = input(self.formatter.warning("Are you sure? (yes/no): ")).strip().lower()
            except KeyboardInterrupt:
                print()
                print("Deletion cancelled.")
                return 0
            
            if response != 'yes':
                print("Deletion cancelled.")
                return 0
            
            # Delete baseline
            try:
                manager.storage.delete_file(baseline_id)
                print()
                print(self.formatter.success(f"✓ Baseline deleted: {baseline_id}"))
                return 0
            except FileNotFoundError:
                print(self.formatter.error(f"❌ Baseline not found: {baseline_id}"))
                return 1
            except Exception as e:
                print(self.formatter.error(f"❌ Failed to delete baseline: {str(e)}"))
                return 1
        
        except Exception as e:
            print(self.formatter.error(f"❌ Error deleting baseline: {str(e)}"))
            if self.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    def compare_command(self, baseline_id: str, candidate_id: str, output_dir: Optional[str] = None) -> int:
        """
        Compare two specific baselines.
        
        Args:
            baseline_id: Baseline filename
            candidate_id: Candidate filename
            output_dir: Optional output directory override
        
        Returns:
            Exit code (0 for pass, 1 for warning, 2 for error)
        """
        print(self.formatter.header("Compare Baselines"))
        
        try:
            manager = BaselineManager()
            comparison_engine = ComparisonEngine()
            
            # Load files
            self.progress.step(1, 4, f"Loading baseline: {baseline_id}...")
            try:
                baseline = manager.storage.load_json(baseline_id)
            except FileNotFoundError:
                print(self.formatter.error(f"❌ Baseline not found: {baseline_id}"))
                return 2
            except Exception as e:
                print(self.formatter.error(f"❌ Failed to load baseline: {str(e)}"))
                return 2
            
            self.progress.step(2, 4, f"Loading candidate: {candidate_id}...")
            try:
                candidate = manager.storage.load_json(candidate_id)
            except FileNotFoundError:
                print(self.formatter.error(f"❌ Candidate not found: {candidate_id}"))
                return 2
            except Exception as e:
                print(self.formatter.error(f"❌ Failed to load candidate: {str(e)}"))
                return 2
            
            # Compare
            self.progress.step(3, 4, "Comparing results...")
            comparison = comparison_engine.compare_pipeline(baseline, candidate)
            status, exit_code = comparison_engine.generate_summary(comparison)
            
            # Generate report
            self.progress.step(4, 4, "Generating report...")
            pipeline = comparison.pipeline_name
            report_output_dir = output_dir or "benchmark/results"
            report_generator = ReportGenerator(pipeline, output_directory=report_output_dir)
            
            violation_counts = comparison.count_violations_by_severity()
            report_generator.add_comparison_results(
                overall_status=status.name.lower(),
                baseline_timestamp=baseline.get('captured_at'),
                baseline_git_commit=baseline.get('execution_context', {}).get('git_commit'),
                violations_summary={
                    'total_violations': sum(violation_counts.values()),
                    'by_severity': violation_counts
                }
            )
            
            report_generator.generate_summary()
            report_path = report_generator.save()
            
            print()
            print(self.formatter.header("Comparison Results"))
            print(f"Baseline:  {baseline_id}")
            print(f"Candidate: {candidate_id}")
            print()
            print(f"Status: {self._format_status(status)}")
            
            # Show violation summary
            print()
            print(self.formatter.subheader("Violation Summary"))
            print(f"  Info:    {violation_counts['INFO']:>3} violations")
            print(f"  Warning: {violation_counts['WARNING']:>3} violations")
            print(f"  Error:   {violation_counts['ERROR']:>3} violations")
            
            print()
            print(f"Report: {report_path}")
            
            return exit_code
        
        except Exception as e:
            print(self.formatter.error(f"❌ Error comparing baselines: {str(e)}"))
            if self.verbose:
                import traceback
                traceback.print_exc()
            return 2
    
    def _format_status(self, status) -> str:
        """
        Format comparison status with color.
        
        Args:
            status: ComparisonStatus enum value
        
        Returns:
            Formatted status string
        """
        status_name = status.name
        if status_name == 'PASS':
            return self.formatter.success(f"✓ {status_name}")
        elif status_name == 'WARNING':
            return self.formatter.warning(f"⚠ {status_name}")
        else:  # ERROR
            return self.formatter.error(f"✗ {status_name}")


def main() -> int:
    """
    Main entry point for the CLI.
    
    Returns:
        Exit code
    """
    parser = argparse.ArgumentParser(
        description="dbt Benchmark Management Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Capture a baseline for Pipeline A
  %(prog)s capture-baseline --pipeline A
  
  # Run benchmark for Pipeline B and compare to baseline
  %(prog)s run-benchmark --pipeline B
  
  # List all available baselines
  %(prog)s list-baselines
  
  # List baselines for Pipeline C only
  %(prog)s list-baselines --pipeline C
  
  # Delete a specific baseline
  %(prog)s delete-baseline --id baseline_A_20240101_120000.json
  
  # Compare two baselines
  %(prog)s compare --baseline baseline_A_20240101_120000.json --candidate baseline_A_20240102_120000.json
  
  # Run with verbose output and custom config
  %(prog)s --verbose run-benchmark --pipeline A --output-dir /tmp/reports
        """
    )
    
    # Global flags
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output for debugging'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Override default output directory for reports (default: benchmark/results)'
    )
    
    parser.add_argument(
        '--config-file',
        type=str,
        default="benchmark/config/config.yaml",
        help='Path to configuration file (default: benchmark/config/config.yaml)'
    )
    
    parser.add_argument(
        '--no-color',
        action='store_true',
        help='Disable colored output'
    )
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # capture-baseline command
    capture_parser = subparsers.add_parser(
        'capture-baseline',
        help='Execute and save a baseline for a pipeline'
    )
    capture_parser.add_argument(
        '--pipeline', '-p',
        required=True,
        type=str,
        help='Pipeline to baseline (A, B, or C)'
    )
    
    # run-benchmark command
    run_parser = subparsers.add_parser(
        'run-benchmark',
        help='Execute a benchmark and compare against baseline'
    )
    run_parser.add_argument(
        '--pipeline', '-p',
        required=True,
        type=str,
        help='Pipeline to benchmark (A, B, or C)'
    )
    
    # list-baselines command
    list_parser = subparsers.add_parser(
        'list-baselines',
        help='Show available baselines'
    )
    list_parser.add_argument(
        '--pipeline', '-p',
        type=str,
        default=None,
        help='Filter by pipeline (A, B, or C) (optional)'
    )
    
    # delete-baseline command
    delete_parser = subparsers.add_parser(
        'delete-baseline',
        help='Remove a baseline'
    )
    delete_parser.add_argument(
        '--id',
        required=True,
        type=str,
        help='Baseline filename to delete'
    )
    
    # compare command
    compare_parser = subparsers.add_parser(
        'compare',
        help='Compare two specific baselines'
    )
    compare_parser.add_argument(
        '--baseline',
        required=True,
        type=str,
        help='Baseline filename'
    )
    compare_parser.add_argument(
        '--candidate',
        required=True,
        type=str,
        help='Candidate filename'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Handle no command
    if not args.command:
        parser.print_help()
        return 1
    
    # Initialize CLI
    use_color = not args.no_color
    cli = BenchmarkCLI(use_color=use_color, verbose=args.verbose)
    
    # Execute command
    try:
        if args.command == 'capture-baseline':
            return cli.capture_baseline_command(args.pipeline, args.output_dir)
        
        elif args.command == 'run-benchmark':
            return cli.run_benchmark_command(args.pipeline, args.output_dir)
        
        elif args.command == 'list-baselines':
            return cli.list_baselines_command(args.pipeline)
        
        elif args.command == 'delete-baseline':
            return cli.delete_baseline_command(args.id)
        
        elif args.command == 'compare':
            return cli.compare_command(args.baseline, args.candidate, args.output_dir)
        
        else:
            parser.print_help()
            return 1
    
    except KeyboardInterrupt:
        print()
        print(cli.formatter.warning("⚠ Interrupted by user"))
        return 130
    except Exception as e:
        print(cli.formatter.error(f"❌ Unexpected error: {str(e)}"))
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
