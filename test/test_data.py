"""Tests for the data module."""

import pytest
import polars as pl
from pathlib import Path
from scicoda import data, exception


class TestGetFilepath:
    """Tests for get_filepath function."""

    def test_existing_file(self):
        """Test getting filepath for an existing data file."""
        # autodock_atom_types.yaml should exist
        filepath = data.get_filepath("atom", "autodock_atom_types", "yaml")
        assert isinstance(filepath, Path)
        assert filepath.exists()
        assert filepath.is_file()
        assert filepath.suffix == ".yaml"

    def test_nonexistent_file(self):
        """Test that FileNotFoundError is raised for non-existent file."""
        with pytest.raises(exception.ScicodaFileNotFoundError) as exc_info:
            data.get_filepath("atom", "nonexistent_file", "yaml")

        error = exc_info.value
        assert error.category == "atom"
        assert error.name == "nonexistent_file"

    def test_nonexistent_category(self):
        """Test that FileNotFoundError is raised for non-existent category."""
        with pytest.raises(exception.ScicodaFileNotFoundError):
            data.get_filepath("nonexistent_category", "some_file", "yaml")


class TestGet:
    """Tests for get function."""

    def test_yaml_file(self):
        """Test loading a YAML file."""
        result = data.get_file("atom", "autodock_atom_types", extension="yaml", cache=False)
        assert isinstance(result, dict)
        assert "data" in result
        assert "schema" in result

    def test_get_data(self):
        """Test get_data helper function."""
        result = data.get_data("atom", "autodock_atom_types", cache=False)
        assert isinstance(result, list)
        # Should contain atom type data
        assert len(result) > 0
        assert isinstance(result[0], dict)

    def test_get_schema(self):
        """Test get_schema helper function."""
        result = data.get_schema("atom", "autodock_atom_types", cache=False)
        assert isinstance(result, dict)

    def test_unsupported_extension(self):
        """Test that unsupported extensions raise an error."""
        with pytest.raises(exception.ScicodaInputError) as exc_info:
            data.get_file("atom", "autodock_atom_types", extension="txt", cache=False)

        assert "extension" in str(exc_info.value)

    def test_caching(self):
        """Test that caching works correctly."""
        # Clear cache first
        data._cache.clear()

        # First call should load from file
        result1 = data.get_file("atom", "autodock_atom_types", extension="yaml", cache=True)

        # Second call should return cached version
        result2 = data.get_file("atom", "autodock_atom_types", extension="yaml", cache=True)

        # Should be the same object (cached)
        assert result1 is result2

        # Cache should contain the data
        assert "atom" in data._cache
        assert "autodock_atom_types" in data._cache["atom"]

    def test_no_caching(self):
        """Test that cache=False prevents caching."""
        # Clear cache first
        data._cache.clear()

        # Load without caching
        result = data.get_file("atom", "autodock_atom_types", extension="yaml", cache=False)

        # Cache should be empty
        assert len(data._cache.get("atom", {})) == 0


class TestParquetLoading:
    """Tests for loading Parquet files."""

    @pytest.mark.skipif(
        not (data._data_dir / "atom" / "periodic_table.parquet").exists(),
        reason="Periodic table parquet file not found"
    )
    def test_load_parquet_eager(self):
        """Test loading a Parquet file eagerly."""
        result = data.get_file("atom", "periodic_table", extension="parquet", cache=False, lazy=False)
        assert isinstance(result, pl.DataFrame)
        assert len(result) > 0
        assert "symbol" in result.columns

    @pytest.mark.skipif(
        not (data._data_dir / "atom" / "periodic_table.parquet").exists(),
        reason="Periodic table parquet file not found"
    )
    def test_load_parquet_lazy(self):
        """Test loading a Parquet file lazily."""
        result = data.get_file("atom", "periodic_table", extension="parquet", cache=False, lazy=True)
        assert isinstance(result, pl.LazyFrame)
        # Collect to verify it's valid
        df = result.collect()
        assert len(df) > 0
        assert "symbol" in df.columns
