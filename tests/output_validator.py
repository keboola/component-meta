"""Output validation for VCR tests using snapshot testing."""

import json
import hashlib
import csv
import re
from pathlib import Path
from typing import Dict, Any, List


class OutputSnapshot:
    """Snapshot of component output for validation."""

    def __init__(self, test_name: str, output_dir: Path, full_output: bool = False):
        """
        Initialize output snapshot.

        Args:
            test_name: Name of the test case
            output_dir: Directory containing component outputs (KBC_DATADIR)
            full_output: If True, capture all rows instead of just samples (default: False)
        """
        self.test_name = test_name
        self.output_dir = Path(output_dir)
        self.full_output = full_output
        self.snapshot = {}

    def capture(self) -> Dict[str, Any]:
        """
        Capture snapshot of all outputs.

        Returns:
            Dict containing tables, files, and metadata snapshots
        """
        self.snapshot = {
            "tables": self._capture_tables(),
            "files": self._capture_files(),
            "metadata": self._capture_metadata(),
        }
        return self.snapshot

    def _capture_tables(self) -> Dict[str, Any]:
        """Capture table outputs with hashing."""
        tables = {}
        tables_dir = self.output_dir / "out" / "tables"

        if not tables_dir.exists():
            return tables

        for csv_file in tables_dir.glob("*.csv"):
            # Read CSV
            try:
                with open(csv_file, encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)

                # Capture metadata
                # Sort columns to ensure deterministic snapshots regardless of CSV column order
                columns = sorted(rows[0].keys()) if rows else []
                # Capture either full output or just samples based on flag
                sample_rows = rows if self.full_output else (rows[:3] if rows else [])
                tables[csv_file.name] = {
                    "row_count": len(rows),
                    "column_count": len(columns),
                    "columns": columns,
                    "hash": self._hash_csv_content(csv_file),
                    "sample_rows": sample_rows,
                }
            except Exception as e:
                tables[csv_file.name] = {"error": f"Failed to read CSV: {str(e)}"}

            # Capture manifest if exists
            manifest_file = csv_file.with_suffix(".csv.manifest")
            if manifest_file.exists():
                try:
                    with open(manifest_file, encoding="utf-8") as f:
                        manifest = json.load(f)
                    tables[manifest_file.name] = {
                        "hash": self._hash_file(manifest_file),
                        "incremental": manifest.get("incremental", False),
                        "primary_key": manifest.get("primary_key", []),
                        "columns": manifest.get("columns", []),
                    }
                except Exception as e:
                    tables[manifest_file.name] = {"error": f"Failed to read manifest: {str(e)}"}

        return tables

    def _capture_files(self) -> Dict[str, Any]:
        """Capture file outputs."""
        files = {}
        files_dir = self.output_dir / "out" / "files"

        if not files_dir.exists():
            return files

        for file_path in files_dir.iterdir():
            if file_path.is_file():
                files[file_path.name] = {
                    "size_bytes": file_path.stat().st_size,
                    "hash": self._hash_file(file_path),
                }

        return files

    def _capture_metadata(self) -> Dict[str, Any]:
        """Capture run metadata."""
        # Could extract from logs, state files, etc.
        # For now, just capture basic info
        return {"test_name": self.test_name}

    def _hash_file(self, file_path: Path) -> str:
        """
        Calculate SHA256 hash of file.

        Args:
            file_path: Path to file

        Returns:
            Hash string in format "sha256:hexdigest"
        """
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return f"sha256:{sha256.hexdigest()}"

    def _sanitize_url(self, url: str) -> str:
        """
        Sanitize URLs by removing dynamic/session-specific parameters.

        Facebook CDN URLs contain dynamic parameters like _nc_gid, _nc_tpa, oh, oe
        that change between requests but don't affect the actual resource.

        Args:
            url: URL string to sanitize

        Returns:
            Sanitized URL with dynamic parameters removed
        """
        if not url or not isinstance(url, str):
            return url

        # List of dynamic Facebook URL parameters to remove
        dynamic_params = [
            "_nc_gid",  # Session/group ID
            "_nc_tpa",  # Tracking parameter
            "_nc_oc",  # Cache parameter
            "oh",  # Hash/signature
            "oe",  # Expiry timestamp
        ]

        # Remove dynamic parameters from URL
        for param in dynamic_params:
            url = re.sub(f"[&?]{param}=[^&]*", "", url)

        # Clean up any trailing ? or & characters
        url = re.sub(r"[?&]+$", "", url)
        # Fix double && or &? patterns
        url = re.sub(r"&{2,}", "&", url)
        url = re.sub(r"\?&", "?", url)

        return url

    def _hash_csv_content(self, file_path: Path) -> str:
        """
        Calculate SHA256 hash of CSV content in a row-order-independent way.
        Sorts rows by all columns to ensure deterministic output regardless of
        async processing order. Also sanitizes URLs to remove dynamic parameters.

        Args:
            file_path: Path to CSV file

        Returns:
            Hash string in format "sha256:hexdigest"
        """
        try:
            with open(file_path, encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

                if not rows:
                    # Empty file - just hash empty content
                    return f"sha256:{hashlib.sha256(b'').hexdigest()}"

                # Get columns in sorted order for deterministic hashing
                columns = sorted(rows[0].keys())

                # Sanitize URL fields in rows to remove dynamic parameters
                # This prevents hash mismatches due to session-specific URL parameters
                url_columns = {
                    "image_url",
                    "thumbnail_url",
                    "url",
                    "link",
                    "picture",
                    "instagram_permalink_url",
                }
                sanitized_rows = []
                for row in rows:
                    sanitized_row = row.copy()
                    for col in columns:
                        if col in url_columns and sanitized_row.get(col):
                            sanitized_row[col] = self._sanitize_url(sanitized_row[col])
                    sanitized_rows.append(sanitized_row)

                # Sort rows by all column values to ensure deterministic order
                # This makes the hash independent of async processing order
                sorted_rows = sorted(
                    sanitized_rows,
                    key=lambda row: tuple(row.get(col, "") for col in columns),
                )

                # Create deterministic string representation
                sha256 = hashlib.sha256()

                # Hash the header
                header_line = ",".join(columns) + "\n"
                sha256.update(header_line.encode("utf-8"))

                # Hash each sorted row
                for row in sorted_rows:
                    row_line = ",".join(row.get(col, "") for col in columns) + "\n"
                    sha256.update(row_line.encode("utf-8"))

                return f"sha256:{sha256.hexdigest()}"
        except Exception as e:
            # Fall back to regular file hash if CSV parsing fails
            return self._hash_file(file_path)

    def validate_against(self, expected: Dict[str, Any]) -> List[str]:
        """
        Validate snapshot against expected values.

        Args:
            expected: Expected snapshot to validate against

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Validate tables
        for table_name, expected_meta in expected.get("tables", {}).items():
            if table_name not in self.snapshot["tables"]:
                errors.append(f"Missing table: {table_name}")
                continue

            actual_meta = self.snapshot["tables"][table_name]

            # Skip if there was an error reading the file
            if "error" in actual_meta or "error" in expected_meta:
                continue

            # Check row count (allow ±10% variance for data fluctuations)
            if "row_count" in expected_meta:
                expected_rows = expected_meta.get("row_count", 0)
                actual_rows = actual_meta.get("row_count", 0)
                if expected_rows > 0:
                    variance = abs(actual_rows - expected_rows) / expected_rows
                    if variance > 0.1:
                        errors.append(
                            f"{table_name}: Row count mismatch "
                            f"(expected ~{expected_rows}, got {actual_rows}, "
                            f"variance {variance * 100:.1f}%)"
                        )

            # Check columns match
            if "columns" in expected_meta:
                expected_cols = set(expected_meta.get("columns", []))
                actual_cols = set(actual_meta.get("columns", []))
                if expected_cols != actual_cols:
                    missing = expected_cols - actual_cols
                    extra = actual_cols - expected_cols
                    msg = f"{table_name}: Column mismatch"
                    if missing:
                        msg += f" (missing: {sorted(missing)})"
                    if extra:
                        msg += f" (extra: {sorted(extra)})"
                    errors.append(msg)

            # Hash validation (strict for manifests, optional for CSVs)
            if "hash" in expected_meta:
                if actual_meta.get("hash") != expected_meta["hash"]:
                    errors.append(f"{table_name}: Content changed (hash mismatch - use --update-snapshots to update)")

        # Validate files
        for file_name, expected_meta in expected.get("files", {}).items():
            if file_name not in self.snapshot["files"]:
                errors.append(f"Missing file: {file_name}")
                continue

            actual_meta = self.snapshot["files"][file_name]

            # Hash validation
            if "hash" in expected_meta:
                if actual_meta.get("hash") != expected_meta["hash"]:
                    errors.append(f"{file_name}: Content changed (hash mismatch)")

        return errors


class SnapshotManager:
    """Manage output snapshots for VCR tests."""

    def __init__(self, snapshots_file: Path):
        """
        Initialize snapshot manager.

        Args:
            snapshots_file: Path to snapshots JSON file
        """
        self.snapshots_file = Path(snapshots_file)
        self.snapshots = self._load_snapshots()

    def _load_snapshots(self) -> Dict[str, Any]:
        """Load existing snapshots from file."""
        if self.snapshots_file.exists():
            with open(self.snapshots_file, encoding="utf-8") as f:
                return json.load(f)
        return {}

    def save(self):
        """Save snapshots to file."""
        self.snapshots_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.snapshots_file, "w", encoding="utf-8") as f:
            json.dump(self.snapshots, f, indent=2, sort_keys=True)
        print(f"Saved snapshots to {self.snapshots_file}")

    def capture_snapshot(self, test_name: str, output_dir: Path, full_output: bool = False) -> Dict[str, Any]:
        """
        Capture and store snapshot for a test case.

        Args:
            test_name: Name of the test case
            output_dir: Output directory to snapshot
            full_output: If True, capture all rows instead of just samples

        Returns:
            Captured snapshot data
        """
        snapshot = OutputSnapshot(test_name, output_dir, full_output=full_output)
        captured = snapshot.capture()
        self.snapshots[test_name] = captured
        return captured

    def validate_snapshot(self, test_name: str, output_dir: Path) -> List[str]:
        """
        Validate output against stored snapshot.

        Args:
            test_name: Name of the test case
            output_dir: Output directory to validate

        Returns:
            List of validation errors (empty if valid)
        """
        if test_name not in self.snapshots:
            return [f"No snapshot exists for {test_name} - run with --update-snapshots"]

        snapshot = OutputSnapshot(test_name, output_dir)
        snapshot.capture()

        return snapshot.validate_against(self.snapshots[test_name])

    def has_snapshot(self, test_name: str) -> bool:
        """Check if snapshot exists for test case."""
        return test_name in self.snapshots

    def get_snapshot(self, test_name: str) -> Dict[str, Any]:
        """Get snapshot for test case."""
        return self.snapshots.get(test_name, {})

    def list_snapshots(self) -> List[str]:
        """List all test names with snapshots."""
        return list(self.snapshots.keys())

    def remove_snapshot(self, test_name: str):
        """Remove snapshot for test case."""
        if test_name in self.snapshots:
            del self.snapshots[test_name]
