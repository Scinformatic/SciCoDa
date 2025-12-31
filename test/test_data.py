"""Tests for the data module."""

import json
import pytest
import polars as pl
from pathlib import Path
from scicoda import data, exception


class TestGetFilepath:
    """Tests for get_filepath function."""

    def test_existing_file(self):
        """Test getting filepath for an existing data file."""
        # autodock_atom_types.json should exist
        filepath = data.get_filepath("atom", "autodock_atom_types", "json")
        assert isinstance(filepath, Path)
        assert filepath.exists()
        assert filepath.is_file()
        assert filepath.suffix == ".json"

    def test_nonexistent_file(self):
        """Test that FileNotFoundError is raised for non-existent file."""
        with pytest.raises(exception.ScicodaFileNotFoundError) as exc_info:
            data.get_filepath("atom", "nonexistent_file", "json")

        error = exc_info.value
        assert error.category == "atom"
        assert error.name == "nonexistent_file"

    def test_nonexistent_category(self):
        """Test that FileNotFoundError is raised for non-existent category."""
        with pytest.raises(exception.ScicodaFileNotFoundError):
            data.get_filepath("nonexistent_category", "some_file", "json")


class TestGetFile:
    """Tests for get_file function."""

    def test_json_file(self):
        """Test loading a JSON file."""
        result = data.get_file("atom", "autodock_atom_types", extension="json")
        assert isinstance(result, list)
        # Should contain atom type data
        assert len(result) > 0
        assert isinstance(result[0], dict)
        # Check expected keys
        assert "type" in result[0]
        assert "element" in result[0]

    def test_unsupported_extension(self):
        """Test that unsupported extensions raise an error.
        
        Note: Since file lookup happens before extension validation,
        unsupported extensions will raise ScicodaFileNotFoundError
        because the file with that extension doesn't exist.
        """
        with pytest.raises(exception.ScicodaFileNotFoundError):
            data.get_file("atom", "autodock_atom_types", extension="txt")


class TestParquetLoading:
    """Tests for loading Parquet files."""

    @pytest.mark.skipif(
        not (data._data_dir / "atom" / "periodic_table.parquet").exists(),
        reason="Periodic table parquet file not found"
    )
    def test_load_parquet(self):
        """Test loading a Parquet file."""
        result = data.get_file("atom", "periodic_table", extension="parquet")
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert "symbol" in result.columns

    @pytest.mark.skipif(
        not (data._data_dir / "atom" / "periodic_table.parquet").exists(),
        reason="Periodic table parquet file not found"
    )
    def test_load_parquet_with_filter(self):
        """Test loading a Parquet file with a filter expression."""
        filterby = pl.col("symbol").is_in(["H", "C", "N", "O"])
        result = data.get_file("atom", "periodic_table", extension="parquet", filterby=filterby)
        assert isinstance(result, pl.DataFrame)
        assert len(result) <= 4  # Should have at most 4 elements
        assert "symbol" in result.columns


class TestDataValidation:
    """Tests for validating data against schemas."""

    def load_schema(self, category: str, name: str) -> dict:
        """Load a schema from the test data_schema directory."""
        import yaml
        schema_dir = Path(__file__).parent / "data_schema" / category
        schema_file = schema_dir / f"{name}.yaml"

        if not schema_file.exists():
            pytest.skip(f"Schema file not found: {schema_file}")

        with open(schema_file) as f:
            return yaml.safe_load(f)

    def validate_against_schema(self, data: list | dict, schema: dict) -> list[str]:
        """Validate data against a JSON schema.

        Returns a list of validation errors (empty if valid).
        """
        try:
            import jsonschema
        except ImportError:
            pytest.skip("jsonschema package not installed")

        errors = []
        validator = jsonschema.Draft7Validator(schema)

        for error in validator.iter_errors(data):
            errors.append(f"{error.json_path}: {error.message}")

        return errors

    def test_autodock_atom_types_schema_validation(self):
        """Test that autodock_atom_types data validates against its schema."""
        # Load data
        data_content = data.get_file("atom", "autodock_atom_types", extension="json")

        # Load schema
        schema = self.load_schema("atom", "autodock_atom_types")

        # Validate
        errors = self.validate_against_schema(data_content, schema)

        assert len(errors) == 0, f"Validation errors: {errors}"

    def test_autodock_atom_types_data_structure(self):
        """Test the structure of autodock_atom_types data."""
        data_content = data.get_file("atom", "autodock_atom_types", extension="json")

        assert isinstance(data_content, list)
        assert len(data_content) > 0

        # Check first item structure
        first_item = data_content[0]
        assert "type" in first_item
        assert "element" in first_item
        assert "hbond_acceptor" in first_item
        assert "hbond_donor" in first_item

        # Check data types
        assert isinstance(first_item["type"], str)
        assert isinstance(first_item["element"], str)
        assert isinstance(first_item["hbond_acceptor"], bool)
        assert isinstance(first_item["hbond_donor"], bool)

    def test_radii_vdw_blue_obelisk_schema_validation(self):
        """Test that radii_vdw_blue_obelisk data validates against its schema."""
        # Load data
        data_content = data.get_file("atom", "radii_vdw_blue_obelisk", extension="json")

        # Load schema
        schema = self.load_schema("atom", "radii_vdw_blue_obelisk")

        # Validate
        errors = self.validate_against_schema(data_content, schema)

        assert len(errors) == 0, f"Validation errors: {errors}"

    def test_radii_vdw_blue_obelisk_data_structure(self):
        """Test the structure of radii_vdw_blue_obelisk data."""
        data_content = data.get_file("atom", "radii_vdw_blue_obelisk", extension="json")

        assert isinstance(data_content, list)
        assert len(data_content) == 118  # Should have all 118 elements

        # Check first item structure
        first_item = data_content[0]
        assert "element" in first_item
        assert "radius" in first_item

        # Check data types
        assert isinstance(first_item["element"], str)
        assert isinstance(first_item["radius"], int)

        # Check that radius values are positive
        for item in data_content:
            assert item["radius"] > 0

    def test_radii_vdw_blue_obelisk_element_coverage(self):
        """Test that radii_vdw_blue_obelisk has data for all elements."""
        data_content = data.get_file("atom", "radii_vdw_blue_obelisk", extension="json")

        # Extract element symbols
        elements = [item["element"] for item in data_content]

        # Should have 118 unique elements (H to Og)
        assert len(set(elements)) == 118
        assert len(elements) == 118  # No duplicates

        # Check that common elements are present
        common_elements = ["H", "C", "N", "O", "S", "P", "Fe", "Cu", "Zn"]
        for elem in common_elements:
            assert elem in elements, f"Missing element: {elem}"
