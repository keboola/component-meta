"""Test the output snapshot validator functionality."""
import json
import csv
from pathlib import Path
from tempfile import TemporaryDirectory
from output_validator import OutputSnapshot, SnapshotManager


def test_snapshot_capture_and_validation():
    """Test capturing and validating output snapshots."""

    # Create a temporary directory with mock component outputs
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create output structure
        tables_dir = tmpdir / "out" / "tables"
        tables_dir.mkdir(parents=True)

        # Create a sample CSV
        sample_csv = tables_dir / "campaigns.csv"
        with open(sample_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['id', 'name', 'impressions'])
            writer.writeheader()
            writer.writerow({'id': '123', 'name': 'Test Campaign', 'impressions': '1000'})
            writer.writerow({'id': '456', 'name': 'Another Campaign', 'impressions': '2000'})

        # Create a sample manifest
        manifest_file = tables_dir / "campaigns.csv.manifest"
        manifest_data = {
            "incremental": True,
            "primary_key": ["id"],
            "columns": ["id", "name", "impressions"]
        }
        with open(manifest_file, 'w', encoding='utf-8') as f:
            json.dump(manifest_data, f)

        # Test 1: Capture snapshot
        snapshot = OutputSnapshot("test_case_1", tmpdir)
        captured = snapshot.capture()

        # Verify capture
        assert "tables" in captured
        assert "campaigns.csv" in captured["tables"]
        assert captured["tables"]["campaigns.csv"]["row_count"] == 2
        assert captured["tables"]["campaigns.csv"]["column_count"] == 3
        assert set(captured["tables"]["campaigns.csv"]["columns"]) == {"id", "name", "impressions"}
        assert "hash" in captured["tables"]["campaigns.csv"]
        assert len(captured["tables"]["campaigns.csv"]["sample_rows"]) == 2

        # Verify manifest capture
        assert "campaigns.csv.manifest" in captured["tables"]
        assert captured["tables"]["campaigns.csv.manifest"]["incremental"] == True
        assert captured["tables"]["campaigns.csv.manifest"]["primary_key"] == ["id"]

        print("✓ Snapshot capture works correctly")

        # Test 2: Validation passes with same data
        errors = snapshot.validate_against(captured)
        assert errors == [], f"Validation should pass with same data, but got errors: {errors}"
        print("✓ Snapshot validation passes with identical data")

        # Test 3: Validation detects changes
        # Modify the snapshot to simulate changes
        import copy as copy_module
        modified_snapshot = copy_module.deepcopy(captured)
        modified_snapshot["tables"]["campaigns.csv"]["row_count"] = 5  # Wrong count

        errors = snapshot.validate_against(modified_snapshot)
        assert len(errors) > 0, "Validation should fail with different row count"
        assert any("Row count mismatch" in e for e in errors)
        print("✓ Snapshot validation detects row count changes")

        # Test 4: Validation detects column changes
        modified_snapshot = copy_module.deepcopy(captured)
        modified_snapshot["tables"]["campaigns.csv"]["columns"] = ["id", "name", "clicks"]  # Wrong columns

        errors = snapshot.validate_against(modified_snapshot)
        assert len(errors) > 0, "Validation should fail with different columns"
        assert any("Column mismatch" in e for e in errors)
        print("✓ Snapshot validation detects column changes")


def test_snapshot_manager():
    """Test the SnapshotManager for saving and loading snapshots."""

    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        snapshots_file = tmpdir / "snapshots.json"

        # Create manager
        manager = SnapshotManager(snapshots_file)
        assert manager.list_snapshots() == []

        # Create mock output directory
        output_dir = tmpdir / "output"
        tables_dir = output_dir / "out" / "tables"
        tables_dir.mkdir(parents=True)

        # Create sample CSV
        sample_csv = tables_dir / "test.csv"
        with open(sample_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['id', 'value'])
            writer.writerow(['1', '100'])

        # Capture snapshot
        manager.capture_snapshot("test_1", output_dir)
        assert manager.has_snapshot("test_1")
        assert "test_1" in manager.list_snapshots()

        # Save to file
        manager.save()
        assert snapshots_file.exists()

        # Load from file
        manager2 = SnapshotManager(snapshots_file)
        assert manager2.has_snapshot("test_1")
        snapshot = manager2.get_snapshot("test_1")
        assert "tables" in snapshot
        assert "test.csv" in snapshot["tables"]

        print("✓ SnapshotManager save/load works correctly")

        # Validate
        errors = manager2.validate_snapshot("test_1", output_dir)
        assert errors == [], f"Validation should pass, but got: {errors}"
        print("✓ SnapshotManager validation works")


if __name__ == "__main__":
    print("Testing OutputSnapshot and SnapshotManager...\n")
    test_snapshot_capture_and_validation()
    print()
    test_snapshot_manager()
    print("\n✅ All snapshot validator tests passed!")
