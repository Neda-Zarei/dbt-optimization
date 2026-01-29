"""
Entry point for python -m benchmark execution.

Enables running the benchmark CLI via:
  python -m benchmark <command> [options]
"""

import sys
from pathlib import Path

# Add scripts directory to Python path
scripts_dir = Path(__file__).parent / 'scripts'
sys.path.insert(0, str(scripts_dir))

from cli import main

if __name__ == '__main__':
    sys.exit(main())
