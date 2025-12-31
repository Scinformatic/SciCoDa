"""Tests for the update.atom module."""

import pytest
import polars as pl
from scicoda.update import atom as update_atom


@pytest.fixture(scope="class")
def update_all_data(tmp_path_factory):
    """Fixture that calls update_atom.update_all() once and caches the result for all tests in the class."""
    tmpdir = tmp_path_factory.mktemp("update_all")
    result = update_atom.update_all(data_dir=str(tmpdir))
    return result, tmpdir


@pytest.fixture(scope="class")
def periodic_table_data(tmp_path_factory):
    """Fixture that calls update_atom.periodic_table() once and caches the result for all tests in the class."""
    tmpdir = tmp_path_factory.mktemp("periodic_table")
    result = update_atom.periodic_table(data_dir=str(tmpdir))
    return result, tmpdir


@pytest.mark.online
@pytest.mark.slow
class TestUpdateAll:
    """Tests for update_all function.

    All tests in this class share the same update_all data loaded once via the update_all_data fixture.
    """

    def test_returns_dict(self, update_all_data):
        """Test that update_all returns a dictionary."""
        result, _ = update_all_data
        assert isinstance(result, dict)
        assert "periodic_table" in result

    def test_creates_files(self, update_all_data):
        """Test that update_all creates parquet files."""
        result, _ = update_all_data

        # Check that files were created
        for dataset_name, file_dict in result.items():
            for filepath, df in file_dict.items():
                assert filepath.exists()
                assert filepath.suffix == ".parquet"
                assert isinstance(df, pl.DataFrame)


@pytest.mark.online
@pytest.mark.slow
class TestPeriodicTable:
    """Tests for periodic_table update function.

    All tests in this class share the same periodic_table data loaded once via the periodic_table_data fixture.
    """

    def test_creates_parquet_file(self, periodic_table_data):
        """Test that periodic_table creates a parquet file."""
        result, _ = periodic_table_data

        assert isinstance(result, dict)
        assert len(result) == 1

        filepath, df = list(result.items())[0]
        assert filepath.exists()
        assert filepath.suffix == ".parquet"
        assert isinstance(df, pl.DataFrame)

    def test_dataframe_content(self, periodic_table_data):
        """Test that the created DataFrame has expected content."""
        result, _ = periodic_table_data

        filepath, df = list(result.items())[0]

        # Should have 118 elements
        assert len(df) == 118

        # Should have expected columns
        assert "z" in df.columns
        assert "symbol" in df.columns
        assert "name" in df.columns

    def test_custom_filepath(self, tmp_path):
        """Test that custom filepath works."""
        custom_path = "custom/path/my_table.parquet"
        result = update_atom.periodic_table(
            data_dir=str(tmp_path),
            filepath=custom_path
        )

        filepath = list(result.keys())[0]
        assert "custom" in str(filepath)
        assert "my_table.parquet" in str(filepath)
        assert filepath.exists()

    def test_custom_url(self, tmp_path):
        """Test that custom URL parameter works."""
        result = update_atom.periodic_table(
            data_dir=str(tmp_path),
            url="https://pubchem.ncbi.nlm.nih.gov/rest/pug/periodictable/CSV"
        )

        filepath, df = list(result.items())[0]
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 118

    def test_file_can_be_read(self, periodic_table_data):
        """Test that the created parquet file can be read."""
        result, _ = periodic_table_data

        filepath = list(result.keys())[0]

        # Read the file back
        df_read = pl.read_parquet(filepath)
        assert isinstance(df_read, pl.DataFrame)
        assert len(df_read) == 118
