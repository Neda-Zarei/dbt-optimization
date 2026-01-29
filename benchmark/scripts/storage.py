"""
File Storage Operations Module

Provides file I/O operations, JSON serialization, and directory management
for baseline files.

Features:
- Atomic writes to prevent corruption
- Directory creation and validation
- JSON serialization with proper formatting
- File validation and error handling
"""

import json
import logging
import tempfile
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StorageManager:
    """
    Manages file I/O operations for baseline JSON files.
    
    Features:
    - Atomic writes (write to temp file, then rename)
    - Directory creation and validation
    - JSON serialization with formatting
    - File validation and corruption detection
    """
    
    def __init__(self, base_dir: str = "benchmark/baselines"):
        """
        Initialize StorageManager.
        
        Args:
            base_dir: Base directory for storing baseline files
        """
        self.base_dir = Path(base_dir)
        self._ensure_directory_exists()
    
    def _ensure_directory_exists(self) -> bool:
        """
        Create base directory if it doesn't exist.
        
        Returns:
            bool: True if directory exists or was created successfully
        """
        try:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured directory exists: {self.base_dir}")
            return True
        except Exception as e:
            logger.error(f"Error creating directory {self.base_dir}: {str(e)}")
            raise
    
    def save_json(self, data: Dict[str, Any], file_path: str) -> bool:
        """
        Save data to JSON file with atomic writes.
        
        Writes to temporary file first, then atomically renames to final location.
        This prevents corruption if the process is interrupted.
        
        Args:
            data: Dictionary to serialize as JSON
            file_path: Relative path from base_dir for the file
        
        Returns:
            bool: True if saved successfully
        
        Raises:
            Exception: If save operation fails
        """
        try:
            full_path = self.base_dir / file_path
            
            # Create parent directories if needed
            full_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write to temporary file in same directory (ensures same filesystem)
            temp_fd, temp_path = tempfile.mkstemp(dir=full_path.parent, text=True)
            
            try:
                # Write JSON data to temp file
                with os.fdopen(temp_fd, 'w') as f:
                    json.dump(data, f, indent=2)
                
                # Atomically rename temp file to final location
                os.replace(temp_path, full_path)
                
                logger.info(f"Saved JSON to {full_path}")
                return True
            
            except Exception as e:
                # Clean up temp file if something went wrong
                try:
                    os.unlink(temp_path)
                except:
                    pass
                raise
        
        except Exception as e:
            logger.error(f"Error saving JSON to {file_path}: {str(e)}")
            raise
    
    def load_json(self, file_path: str) -> Dict[str, Any]:
        """
        Load data from JSON file with validation.
        
        Args:
            file_path: Relative path from base_dir for the file
        
        Returns:
            Dictionary with loaded JSON data
        
        Raises:
            FileNotFoundError: If file doesn't exist
            json.JSONDecodeError: If JSON is corrupted
            Exception: If load operation fails
        """
        try:
            full_path = self.base_dir / file_path
            
            if not full_path.exists():
                error_msg = f"File not found: {full_path}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)
            
            with open(full_path, 'r') as f:
                data = json.load(f)
            
            logger.debug(f"Loaded JSON from {full_path}")
            return data
        
        except json.JSONDecodeError as e:
            error_msg = f"Corrupted JSON file {file_path}: {str(e)}"
            logger.error(error_msg)
            raise
        except Exception as e:
            logger.error(f"Error loading JSON from {file_path}: {str(e)}")
            raise
    
    def list_files(self, pattern: Optional[str] = None) -> List[Path]:
        """
        List all files in the base directory, optionally filtered by pattern.
        
        Args:
            pattern: Optional glob pattern (e.g., "baseline_A_*.json")
        
        Returns:
            Sorted list of Path objects
        """
        try:
            if pattern:
                files = sorted(self.base_dir.glob(pattern))
            else:
                # Get all JSON files
                files = sorted(self.base_dir.glob("*.json"))
            
            logger.debug(f"Found {len(files)} files matching pattern '{pattern}'")
            return files
        
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            raise
    
    def file_exists(self, file_path: str) -> bool:
        """
        Check if a file exists.
        
        Args:
            file_path: Relative path from base_dir
        
        Returns:
            bool: True if file exists
        """
        full_path = self.base_dir / file_path
        return full_path.exists()
    
    def delete_file(self, file_path: str) -> bool:
        """
        Delete a file.
        
        Args:
            file_path: Relative path from base_dir
        
        Returns:
            bool: True if deleted successfully
        
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        try:
            full_path = self.base_dir / file_path
            
            if not full_path.exists():
                error_msg = f"File not found: {full_path}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)
            
            full_path.unlink()
            logger.info(f"Deleted file: {full_path}")
            return True
        
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {str(e)}")
            raise
    
    def get_full_path(self, file_path: str) -> Path:
        """
        Get the full path for a file.
        
        Args:
            file_path: Relative path from base_dir
        
        Returns:
            Full Path object
        """
        return self.base_dir / file_path
    
    def get_relative_path(self, full_path: Path) -> str:
        """
        Get relative path from a full path.
        
        Args:
            full_path: Full Path object
        
        Returns:
            Relative path string
        """
        return str(full_path.relative_to(self.base_dir))
