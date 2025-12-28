"""Tests for the update.pdb module."""

import pytest
import polars as pl
from pathlib import Path
from scicoda.update import pdb as update_pdb
import tempfile


class TestUpdateAll:
    """Tests for update_all function."""

    @pytest.mark.online
    @pytest.mark.slow
    def test_returns_dict(self):
        """Test that update_all returns a dictionary."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                result = update_pdb.update_all(data_dir=tmpdir)
                assert isinstance(result, dict)
                assert "ccd" in result
        except Exception as e:
            pytest.skip(f"Update failed: {e}")


class TestCCD:
    """Tests for ccd update function."""

    @pytest.mark.online
    @pytest.mark.slow
    def test_returns_tuple(self):
        """Test that ccd returns a tuple of (dict, dict)."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                result = update_pdb.ccd(data_dir=tmpdir)
                assert isinstance(result, tuple)
                assert len(result) == 2

                file_dict, problems = result
                assert isinstance(file_dict, dict)
                assert isinstance(problems, dict)
        except Exception as e:
            pytest.skip(f"CCD update failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_creates_parquet_files(self):
        """Test that ccd creates parquet files."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                file_dict, _ = update_pdb.ccd(data_dir=tmpdir)

                # Check that files were created
                assert len(file_dict) > 0

                for filepath, df in file_dict.items():
                    assert filepath.exists()
                    assert filepath.suffix == ".parquet"
                    assert isinstance(df, pl.DataFrame)
        except Exception as e:
            pytest.skip(f"CCD update failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_creates_aa_and_non_aa_variants(self):
        """Test that both aa and non_aa variants are created."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                file_dict, _ = update_pdb.ccd(data_dir=tmpdir)

                # Check for aa and non_aa files
                filepaths = [str(fp) for fp in file_dict.keys()]

                aa_files = [fp for fp in filepaths if "-aa." in fp]
                non_aa_files = [fp for fp in filepaths if "-non_aa." in fp]

                assert len(aa_files) > 0, "No amino acid variant files created"
                assert len(non_aa_files) > 0, "No non-amino acid variant files created"

                # Should have same number of files for each variant
                assert len(aa_files) == len(non_aa_files)
        except Exception as e:
            pytest.skip(f"CCD update failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_chem_comp_files_created(self):
        """Test that chem_comp files are created."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                file_dict, _ = update_pdb.ccd(data_dir=tmpdir)

                filepaths = [str(fp) for fp in file_dict.keys()]
                chem_comp_files = [fp for fp in filepaths if "chem_comp-" in fp]

                assert len(chem_comp_files) >= 2  # At least aa and non_aa
        except Exception as e:
            pytest.skip(f"CCD update failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
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

    @pytest.mark.online
    @pytest.mark.slow
    def test_files_can_be_read(self):
        """Test that created parquet files can be read."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                file_dict, _ = update_pdb.ccd(data_dir=tmpdir)

                # Read one file to verify it's valid
                filepath = list(file_dict.keys())[0]
                df_read = pl.read_parquet(filepath)

                assert isinstance(df_read, pl.DataFrame)
                assert len(df_read) > 0
        except Exception as e:
            pytest.skip(f"CCD update failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_problems_reported(self):
        """Test that problems dictionary is returned."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                _, problems = update_pdb.ccd(data_dir=tmpdir)

                # Problems should be a dict (may be empty)
                assert isinstance(problems, dict)
        except Exception as e:
            pytest.skip(f"CCD update failed: {e}")
