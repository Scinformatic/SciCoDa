"""Tests for the update.atom module."""

import pytest
import polars as pl
from pathlib import Path
from scicoda.update import atom as update_atom
import tempfile


class TestUpdateAll:
    """Tests for update_all function."""

    @pytest.mark.online
    @pytest.mark.slow
    def test_returns_dict(self):
        """Test that update_all returns a dictionary."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = update_atom.update_all(data_dir=tmpdir)
            assert isinstance(result, dict)
            assert "periodic_table" in result

    @pytest.mark.online
    @pytest.mark.slow
    def test_creates_files(self):
        """Test that update_all creates parquet files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = update_atom.update_all(data_dir=tmpdir)

            # Check that files were created
            for dataset_name, file_dict in result.items():
                for filepath, df in file_dict.items():
                    assert filepath.exists()
                    assert filepath.suffix == ".parquet"
                    assert isinstance(df, pl.DataFrame)


class TestPeriodicTable:
    """Tests for periodic_table update function."""

    @pytest.mark.online
    @pytest.mark.slow
    def test_creates_parquet_file(self):
        """Test that periodic_table creates a parquet file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = update_atom.periodic_table(data_dir=tmpdir)

            assert isinstance(result, dict)
            assert len(result) == 1

            filepath, df = list(result.items())[0]
            assert filepath.exists()
            assert filepath.suffix == ".parquet"
            assert isinstance(df, pl.DataFrame)

    @pytest.mark.online
    @pytest.mark.slow
    def test_dataframe_content(self):
        """Test that the created DataFrame has expected content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = update_atom.periodic_table(data_dir=tmpdir)

            filepath, df = list(result.items())[0]

            # Should have 118 elements
            assert len(df) == 118

            # Should have expected columns
            assert "z" in df.columns
            assert "symbol" in df.columns
            assert "name" in df.columns

    @pytest.mark.online
    @pytest.mark.slow
    def test_custom_filepath(self):
        """Test that custom filepath works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            custom_path = "custom/path/my_table.parquet"
            result = update_atom.periodic_table(
                data_dir=tmpdir,
                filepath=custom_path
            )

            filepath = list(result.keys())[0]
            assert "custom" in str(filepath)
            assert "my_table.parquet" in str(filepath)
            assert filepath.exists()

    @pytest.mark.online
    @pytest.mark.slow
    def test_custom_url(self):
        """Test that custom URL parameter works."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = update_atom.periodic_table(
                data_dir=tmpdir,
                url="https://pubchem.ncbi.nlm.nih.gov/rest/pug/periodictable/CSV"
            )

            filepath, df = list(result.items())[0]
            assert isinstance(df, pl.DataFrame)
            assert len(df) == 118

    @pytest.mark.online
    @pytest.mark.slow
    def test_file_can_be_read(self):
        """Test that the created parquet file can be read."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = update_atom.periodic_table(data_dir=tmpdir)

            filepath = list(result.keys())[0]

            # Read the file back
            df_read = pl.read_parquet(filepath)
            assert isinstance(df_read, pl.DataFrame)
            assert len(df_read) == 118
