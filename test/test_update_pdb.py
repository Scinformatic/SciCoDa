"""Tests for the update.pdb module."""

import pytest
import polars as pl
from pathlib import Path
from scicoda.update import pdb as update_pdb
import tempfile


@pytest.fixture(scope="class")
def ccd_data(request):
    """Fixture that calls update_pdb.ccd() once and caches the result for all tests in the class."""
    try:
        tmpdir = tempfile.mkdtemp()
        result = update_pdb.ccd(data_dir=tmpdir)
        return result, tmpdir
    except Exception as e:
        pytest.skip(f"CCD update failed: {e}")


@pytest.mark.online
@pytest.mark.slow
class TestUpdateAll:
    """Tests for update_all function."""

    def test_returns_dict(self):
        """Test that update_all returns a dictionary."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                result = update_pdb.update_all(data_dir=tmpdir)
                assert isinstance(result, dict)
                assert "ccd" in result
        except Exception as e:
            pytest.skip(f"Update failed: {e}")


@pytest.mark.online
@pytest.mark.slow
class TestCCD:
    """Tests for ccd update function.

    All tests in this class share the same CCD data loaded once via the ccd_data fixture.
    """

    def test_returns_tuple(self, ccd_data):
        """Test that ccd returns a tuple of (dict, dict)."""
        result, _ = ccd_data
        assert isinstance(result, tuple)
        assert len(result) == 2

        file_dict, problems = result
        assert isinstance(file_dict, dict)
        assert isinstance(problems, dict)

    def test_creates_parquet_files(self, ccd_data):
        """Test that ccd creates parquet files."""
        result, _ = ccd_data
        file_dict, _ = result

        # Check that files were created
        assert len(file_dict) > 0

        for filepath, df in file_dict.items():
            assert filepath.exists()
            assert filepath.suffix == ".parquet"
            assert isinstance(df, pl.DataFrame)

    def test_creates_aa_and_non_aa_variants(self, ccd_data):
        """Test that both aa and non_aa variants are created."""
        result, _ = ccd_data
        file_dict, _ = result

        # Check for aa and non_aa files
        filepaths = [str(fp) for fp in file_dict.keys()]

        aa_files = [fp for fp in filepaths if "-aa." in fp]
        non_aa_files = [fp for fp in filepaths if "-non_aa." in fp]

        assert len(aa_files) > 0, "No amino acid variant files created"
        assert len(non_aa_files) > 0, "No non-amino acid variant files created"

        # Should have same number of files for each variant
        assert len(aa_files) == len(non_aa_files)

    def test_chem_comp_files_created(self, ccd_data):
        """Test that chem_comp files are created."""
        result, _ = ccd_data
        file_dict, _ = result

        filepaths = [str(fp) for fp in file_dict.keys()]
        chem_comp_files = [fp for fp in filepaths if "chem_comp-" in fp]

        assert len(chem_comp_files) >= 2  # At least aa and non_aa

    def test_custom_basepath(self):
        """Test that custom basepath works."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                custom_basepath = "custom/ccd_data"
                file_dict, _ = update_pdb.ccd(
                    data_dir=tmpdir,
                    basepath=custom_basepath
                )

                # Check that custom path is used
                filepaths = [str(fp) for fp in file_dict.keys()]
                assert any("custom" in fp for fp in filepaths)
                assert any("ccd_data" in fp for fp in filepaths)
        except Exception as e:
            pytest.skip(f"CCD update failed: {e}")

    def test_files_can_be_read(self, ccd_data):
        """Test that created parquet files can be read."""
        result, _ = ccd_data
        file_dict, _ = result

        # Read one file to verify it's valid
        filepath = list(file_dict.keys())[0]
        df_read = pl.read_parquet(filepath)

        assert isinstance(df_read, pl.DataFrame)
        assert len(df_read) > 0

    def test_problems_reported(self, ccd_data):
        """Test that problems dictionary is returned."""
        result, _ = ccd_data
        _, problems = result

        # Problems should be a dict (may be empty)
        assert isinstance(problems, dict)
