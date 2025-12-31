"""Tests for the pdb module."""

import pytest
import polars as pl
from scicoda import pdb, data, exception
from scicoda.update import pdb as update_pdb


@pytest.fixture(scope="class")
def ccd_files(tmp_path_factory):
    """Fixture that creates CCD files in temp directory using update mechanism.

    Called once per test class; temp directory is cleaned up automatically by pytest.
    """
    tmpdir = tmp_path_factory.mktemp("ccd_data")
    update_pdb.ccd(data_dir=str(tmpdir))
    return tmpdir


@pytest.mark.online
@pytest.mark.slow
class TestCCD:
    """Tests for ccd function.

    All tests share CCD files created once via the ccd_files fixture.
    """

    @pytest.fixture(autouse=True)
    def _patch_data_path(self, ccd_files, monkeypatch):
        """Patch data.get_filepath to use temp CCD files for each test."""
        original_get_filepath = data.get_filepath

        def patched_get_filepath(category, name, extension):
            if category == "pdb":
                return ccd_files / f"{name}.{extension}"
            return original_get_filepath(category, name, extension)

        monkeypatch.setattr(data, "get_filepath", patched_get_filepath)

    def test_invalid_category(self):
        """Test that invalid category raises ScicodaInputError."""
        with pytest.raises(exception.ScicodaInputError) as exc_info:
            pdb.ccd(category="invalid_category")

        assert "category" in str(exc_info.value)

    def test_valid_categories(self):
        """Test that all valid CCD categories are accepted."""
        valid_categories = [
            "chem_comp",
            "chem_comp_atom",
            "chem_comp_bond",
            "pdbx_chem_comp_atom_related",
            "pdbx_chem_comp_audit",
            "pdbx_chem_comp_descriptor",
            "pdbx_chem_comp_feature",
            "pdbx_chem_comp_identifier",
            "pdbx_chem_comp_pcm",
            "pdbx_chem_comp_related",
            "pdbx_chem_comp_synonyms",
        ]

        # Should not raise errors for valid categories
        for cat in valid_categories:
            try:
                _ = pdb.ccd(comp_id="ATP", category=cat)
            except exception.ScicodaFileNotFoundError:
                # File not found is acceptable if category wasn't in CCD
                pass
            except exception.ScicodaInputError as e:
                # Should not raise input error for valid categories
                if "category" in str(e):
                    pytest.fail(f"Valid category '{cat}' raised InputError")

    def test_load_chem_comp(self):
        """Test loading chemical component data."""
        df = pdb.ccd(comp_id="ATP", category="chem_comp")
        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0
        assert "id" in df.columns

    def test_load_multiple_comp_ids(self):
        """Test loading multiple component IDs."""
        df = pdb.ccd(
            comp_id=["ATP", "ADP", "AMP"],
            category="chem_comp",
        )
        assert isinstance(df, pl.DataFrame)
        assert len(df) >= 3  # Should have at least 3 components

    def test_load_all_components(self):
        """Test loading all components (no filter)."""
        df = pdb.ccd(comp_id=None, category="chem_comp", variant="non_aa")
        assert isinstance(df, pl.DataFrame)
        assert len(df) > 100  # Should have many components

    def test_amino_acid_variant(self):
        """Test loading amino acid variant."""
        df = pdb.ccd(comp_id="ALA", category="chem_comp", variant="aa")
        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0

    def test_chem_comp_atom_columns(self):
        """Test that chem_comp_atom has expected columns."""
        df = pdb.ccd(comp_id="ATP", category="chem_comp_atom")
        expected_cols = {"comp_id", "atom_id", "type_symbol"}
        assert expected_cols.issubset(set(df.columns))

    def test_chem_comp_bond_columns(self):
        """Test that chem_comp_bond has expected columns."""
        df = pdb.ccd(comp_id="ATP", category="chem_comp_bond")
        expected_cols = {"comp_id", "atom_id_1", "atom_id_2"}
        assert expected_cols.issubset(set(df.columns))
