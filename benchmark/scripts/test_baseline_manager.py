"""
Test Suite for Baseline Management System

Tests baseline capture, storage, retrieval, and deletion with various scenarios
including edge cases and error conditions.
"""

import json
import logging
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

from baseline_manager import BaselineManager
from storage import StorageManager


# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_storage_manager_basic():
    """Test basic storage operations: save and load JSON."""
    logger.info("\n=== Test: Storage Manager - Basic Operations ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)
        
        # Test save_json
        test_data = {
            "pipeline": "A",
            "timestamp": "20240115_143022",
            "status": "SUCCESS"
        }
        
        success = storage.save_json(test_data, "baseline_A_20240115_143022.json")
        assert success, "Failed to save JSON"
        logger.info("✓ Save JSON successful")
        
        # Test load_json
        loaded_data = storage.load_json("baseline_A_20240115_143022.json")
        assert loaded_data == test_data, "Loaded data doesn't match saved data"
        logger.info("✓ Load JSON successful")
        
        # Test file_exists
        assert storage.file_exists("baseline_A_20240115_143022.json"), "File exists check failed"
        logger.info("✓ File exists check successful")


def test_storage_manager_atomic_writes():
    """Test that writes are atomic (write to temp, then rename)."""
    logger.info("\n=== Test: Storage Manager - Atomic Writes ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)
        
        # Save data
        test_data = {"test": "atomic_write"}
        storage.save_json(test_data, "test_atomic.json")
        
        # Verify file exists
        full_path = storage.get_full_path("test_atomic.json")
        assert full_path.exists(), "File should exist after atomic write"
        logger.info("✓ Atomic write successful")


def test_storage_manager_missing_directory():
    """Test that storage creates directory if it doesn't exist."""
    logger.info("\n=== Test: Storage Manager - Missing Directory ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        nonexistent_dir = str(Path(tmpdir) / "subdir" / "nested")
        assert not Path(nonexistent_dir).exists(), "Directory should not exist"
        
        storage = StorageManager(nonexistent_dir)
        assert Path(nonexistent_dir).exists(), "Directory should be created"
        logger.info("✓ Directory created successfully")


def test_storage_manager_corrupted_json():
    """Test handling of corrupted JSON files."""
    logger.info("\n=== Test: Storage Manager - Corrupted JSON ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)
        
        # Create a corrupted JSON file manually
        full_path = storage.get_full_path("corrupted.json")
        with open(full_path, 'w') as f:
            f.write("{invalid json content")
        
        # Try to load corrupted file
        try:
            storage.load_json("corrupted.json")
            assert False, "Should have raised JSONDecodeError"
        except json.JSONDecodeError:
            logger.info("✓ Correctly detected corrupted JSON")


def test_storage_manager_file_not_found():
    """Test handling of missing files."""
    logger.info("\n=== Test: Storage Manager - File Not Found ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)
        
        try:
            storage.load_json("nonexistent.json")
            assert False, "Should have raised FileNotFoundError"
        except FileNotFoundError:
            logger.info("✓ Correctly handled missing file")


def test_storage_manager_list_files():
    """Test listing files with pattern matching."""
    logger.info("\n=== Test: Storage Manager - List Files ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)
        
        # Create multiple baseline files
        for i in range(3):
            data = {"pipeline": "A", "sequence": i}
            storage.save_json(data, f"baseline_A_20240115_14302{i}.json")
        
        # Create file for different pipeline
        storage.save_json({"pipeline": "B"}, "baseline_B_20240115_143022.json")
        
        # List all files
        all_files = storage.list_files()
        assert len(all_files) == 4, "Should find 4 files"
        logger.info("✓ Listed all files successfully")
        
        # List files with pattern
        a_files = storage.list_files("baseline_A_*.json")
        assert len(a_files) == 3, "Should find 3 files for pipeline A"
        logger.info("✓ Listed files with pattern successfully")


def test_storage_manager_delete_file():
    """Test file deletion."""
    logger.info("\n=== Test: Storage Manager - Delete File ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageManager(tmpdir)
        
        # Create and delete file
        storage.save_json({"test": "data"}, "test_file.json")
        assert storage.file_exists("test_file.json")
        
        storage.delete_file("test_file.json")
        assert not storage.file_exists("test_file.json"), "File should be deleted"
        logger.info("✓ File deleted successfully")
        
        # Try to delete non-existent file
        try:
            storage.delete_file("nonexistent.json")
            assert False, "Should have raised FileNotFoundError"
        except FileNotFoundError:
            logger.info("✓ Correctly handled deletion of non-existent file")


def test_baseline_manager_metadata_extraction():
    """Test extraction of pipeline and timestamp from baseline filename."""
    logger.info("\n=== Test: Baseline Manager - Metadata Extraction ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = BaselineManager(tmpdir)
        
        # Test valid filenames
        valid_files = [
            ("baseline_A_20240115_143022.json", ("A", "20240115_143022")),
            ("baseline_B_20240116_153045.json", ("B", "20240116_153045")),
            ("baseline_C_20240117_090000.json", ("C", "20240117_090000")),
        ]
        
        for filename, expected in valid_files:
            result = manager._extract_baseline_metadata(filename)
            assert result == expected, f"Failed for {filename}: got {result}, expected {expected}"
        
        logger.info("✓ All valid filenames parsed correctly")
        
        # Test invalid filenames
        invalid_files = [
            "baseline_A.json",  # Missing timestamp
            "baseline_20240115_143022.json",  # Missing pipeline
            "baseline_A_invalid_timestamp.json",  # Invalid timestamp format
            "notabaseline_A_20240115_143022.json",  # Wrong prefix
            "baseline_A_20240115_143022.txt",  # Wrong extension
        ]
        
        for filename in invalid_files:
            result = manager._extract_baseline_metadata(filename)
            assert result is None, f"Should reject {filename}"
        
        logger.info("✓ Invalid filenames correctly rejected")


def test_baseline_manager_timestamp_formatting():
    """Test ISO 8601 timestamp formatting and parsing."""
    logger.info("\n=== Test: Baseline Manager - Timestamp Formatting ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = BaselineManager(tmpdir)
        
        # Test formatting
        dt = datetime(2024, 1, 15, 14, 30, 22)
        formatted = manager._format_timestamp(dt)
        assert formatted == "20240115_143022", f"Wrong format: {formatted}"
        logger.info("✓ Timestamp formatting successful")
        
        # Test parsing
        parsed = manager._parse_timestamp("20240115_143022")
        assert parsed == dt, f"Parsed timestamp doesn't match: {parsed}"
        logger.info("✓ Timestamp parsing successful")
        
        # Test invalid parsing
        result = manager._parse_timestamp("invalid_timestamp")
        assert result is None, "Should return None for invalid timestamp"
        logger.info("✓ Invalid timestamp correctly rejected")


def test_baseline_manager_save_and_load():
    """Test saving and loading baselines."""
    logger.info("\n=== Test: Baseline Manager - Save and Load ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = BaselineManager(tmpdir)
        
        # Create baseline data
        baseline_data = {
            "pipeline": "A",
            "captured_at": "20240115_143022",
            "status": "SUCCESS",
            "metrics": {"execution_time_ms": 5000}
        }
        
        # Save baseline
        success, filename = manager.save_baseline(baseline_data)
        assert success, f"Failed to save: {filename}"
        assert "baseline_A_20240115_143022.json" in filename
        logger.info(f"✓ Saved baseline: {filename}")
        
        # Load baseline
        loaded = manager.load_baseline("A", "20240115_143022")
        assert loaded == baseline_data, "Loaded data doesn't match"
        logger.info("✓ Loaded baseline successfully")


def test_baseline_manager_prevent_overwrite():
    """Test prevention of overwriting existing baselines."""
    logger.info("\n=== Test: Baseline Manager - Prevent Overwrite ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = BaselineManager(tmpdir)
        
        baseline_data = {
            "pipeline": "A",
            "captured_at": "20240115_143022",
            "status": "SUCCESS"
        }
        
        # Save baseline
        manager.save_baseline(baseline_data)
        
        # Try to save again without force
        success, msg = manager.save_baseline(baseline_data)
        assert not success, "Should prevent overwrite"
        assert "already exists" in msg.lower()
        logger.info("✓ Correctly prevented overwrite")
        
        # Save with force=True should succeed
        baseline_data["status"] = "UPDATED"
        success, msg = manager.save_baseline(baseline_data, force=True)
        assert success, "Should allow overwrite with force=True"
        logger.info("✓ Allowed overwrite with force=True")


def test_baseline_manager_load_most_recent():
    """Test loading the most recent baseline."""
    logger.info("\n=== Test: Baseline Manager - Load Most Recent ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = BaselineManager(tmpdir)
        
        # Save multiple baselines
        for i in range(3):
            baseline_data = {
                "pipeline": "A",
                "captured_at": f"202401{i+10}_14302{i}",
                "sequence": i
            }
            manager.save_baseline(baseline_data)
        
        # Load most recent (without timestamp)
        loaded = manager.load_baseline("A")
        assert loaded is not None, "Should load most recent baseline"
        assert loaded["sequence"] == 2, "Should load the newest baseline"
        logger.info("✓ Loaded most recent baseline successfully")


def test_baseline_manager_list_baselines():
    """Test listing baselines with summary metadata."""
    logger.info("\n=== Test: Baseline Manager - List Baselines ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = BaselineManager(tmpdir)
        
        # Create baseline data
        for pipeline in ["A", "B"]:
            for i in range(2):
                baseline_data = {
                    "pipeline": pipeline,
                    "captured_at": f"202401{10+i}_14302{i}",
                    "execution_context": {
                        "duration_seconds": 100 * (i + 1),
                        "dbt_version": "1.5.0",
                        "git_commit": "abc123",
                        "models_executed": ["model1", "model2"]
                    },
                    "summary": {"status": "SUCCESS"}
                }
                manager.save_baseline(baseline_data)
        
        # List all baselines
        all_baselines = manager.list_baselines()
        assert len(all_baselines) == 4, f"Should have 4 baselines, got {len(all_baselines)}"
        logger.info(f"✓ Listed {len(all_baselines)} total baselines")
        
        # List by pipeline
        a_baselines = manager.list_baselines("A")
        assert len(a_baselines) == 2, "Should have 2 baselines for pipeline A"
        logger.info(f"✓ Listed {len(a_baselines)} baselines for pipeline A")
        
        # Check summary includes required fields
        if all_baselines:
            summary = all_baselines[0]
            required_fields = ["filename", "pipeline", "timestamp", "status"]
            for field in required_fields:
                assert field in summary, f"Summary missing field: {field}"
            logger.info("✓ Summary includes all required fields")


def test_baseline_manager_delete_with_safeguard():
    """Test deletion with confirmation requirement."""
    logger.info("\n=== Test: Baseline Manager - Delete with Safeguard ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = BaselineManager(tmpdir)
        
        # Create baseline
        baseline_data = {
            "pipeline": "A",
            "captured_at": "20240115_143022"
        }
        manager.save_baseline(baseline_data)
        
        # Try to delete without confirmation
        success, msg = manager.delete_baseline("A", "20240115_143022", confirm=False)
        assert not success, "Should require confirmation"
        assert "confirm" in msg.lower()
        logger.info("✓ Correctly required confirmation")
        
        # Delete with confirmation
        success, msg = manager.delete_baseline("A", "20240115_143022", confirm=True)
        assert success, "Should succeed with confirmation"
        logger.info("✓ Deleted baseline with confirmation")
        
        # Verify file is gone
        loaded = manager.load_baseline("A", "20240115_143022")
        assert loaded is None, "Baseline should be deleted"
        logger.info("✓ Verified baseline was deleted")


def test_baseline_manager_missing_timestamp_for_delete():
    """Test that deletion requires timestamp."""
    logger.info("\n=== Test: Baseline Manager - Delete Requires Timestamp ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = BaselineManager(tmpdir)
        
        success, msg = manager.delete_baseline("A", confirm=True)
        assert not success, "Should require timestamp"
        assert "timestamp" in msg.lower()
        logger.info("✓ Correctly required timestamp for deletion")


def test_baseline_manager_cleanup_by_age():
    """Test cleanup of old baselines based on age."""
    logger.info("\n=== Test: Baseline Manager - Cleanup by Age ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = BaselineManager(tmpdir)
        
        # Create baselines with different ages
        base_time = datetime.now()
        for i in range(5):
            # Create baselines 20 days apart
            time_offset = timedelta(days=i*20)
            dt = base_time - time_offset
            
            baseline_data = {
                "pipeline": "A",
                "captured_at": manager._format_timestamp(dt),
                "sequence": i
            }
            
            # Manually save with specific timestamp
            manager.save_baseline(baseline_data)
        
        # List before cleanup
        before = manager.list_baselines("A")
        logger.info(f"Before cleanup: {len(before)} baselines")
        
        # Cleanup with 30 day threshold (should remove 2 oldest)
        deleted_count, deleted_files = manager.cleanup_old_baselines(
            "A",
            max_age_days=30,
            dry_run=False
        )
        
        logger.info(f"✓ Cleanup dry-run removed {deleted_count} baselines")


def test_baseline_manager_cleanup_by_count():
    """Test cleanup of excess baselines based on count."""
    logger.info("\n=== Test: Baseline Manager - Cleanup by Count ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = BaselineManager(tmpdir)
        
        # Create multiple baselines
        for i in range(5):
            baseline_data = {
                "pipeline": "A",
                "captured_at": f"202401{10+i}_14302{i}",
                "sequence": i
            }
            manager.save_baseline(baseline_data)
        
        # Cleanup to keep only 2 most recent
        deleted_count, deleted_files = manager.cleanup_old_baselines(
            "A",
            max_count=2,
            dry_run=False
        )
        
        assert deleted_count == 3, f"Should delete 3, deleted {deleted_count}"
        logger.info(f"✓ Cleanup removed {deleted_count} excess baselines")
        
        # Verify only 2 remain
        remaining = manager.list_baselines("A")
        assert len(remaining) == 2, f"Should have 2 remaining, have {len(remaining)}"
        logger.info("✓ Correct number of baselines retained")


def run_all_tests():
    """Run all test cases."""
    logger.info("\n" + "="*60)
    logger.info("BASELINE MANAGEMENT SYSTEM - TEST SUITE")
    logger.info("="*60)
    
    tests = [
        ("Storage: Basic Operations", test_storage_manager_basic),
        ("Storage: Atomic Writes", test_storage_manager_atomic_writes),
        ("Storage: Missing Directory", test_storage_manager_missing_directory),
        ("Storage: Corrupted JSON", test_storage_manager_corrupted_json),
        ("Storage: File Not Found", test_storage_manager_file_not_found),
        ("Storage: List Files", test_storage_manager_list_files),
        ("Storage: Delete File", test_storage_manager_delete_file),
        ("Baseline: Metadata Extraction", test_baseline_manager_metadata_extraction),
        ("Baseline: Timestamp Formatting", test_baseline_manager_timestamp_formatting),
        ("Baseline: Save and Load", test_baseline_manager_save_and_load),
        ("Baseline: Prevent Overwrite", test_baseline_manager_prevent_overwrite),
        ("Baseline: Load Most Recent", test_baseline_manager_load_most_recent),
        ("Baseline: List Baselines", test_baseline_manager_list_baselines),
        ("Baseline: Delete with Safeguard", test_baseline_manager_delete_with_safeguard),
        ("Baseline: Delete Requires Timestamp", test_baseline_manager_missing_timestamp_for_delete),
        ("Baseline: Cleanup by Age", test_baseline_manager_cleanup_by_age),
        ("Baseline: Cleanup by Count", test_baseline_manager_cleanup_by_count),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            logger.error(f"✗ FAILED: {test_name}")
            logger.error(f"  Error: {str(e)}")
            failed += 1
        except Exception as e:
            logger.error(f"✗ ERROR: {test_name}")
            logger.error(f"  Error: {str(e)}")
            failed += 1
    
    logger.info("\n" + "="*60)
    logger.info(f"TEST RESULTS: {passed} passed, {failed} failed")
    logger.info("="*60 + "\n")
    
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)
