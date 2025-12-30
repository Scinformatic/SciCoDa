"""Tests for the pdb module."""

import pytest
import polars as pl
from scicoda import pdb, exception


class TestCCD:
    """Tests for ccd function."""

    def test_invalid_category(self):
        """Test that invalid category raises ScicodaInputError."""
        with pytest.raises(exception.ScicodaInputError) as exc_info:
            pdb.ccd(category="invalid_category")

        assert "category" in str(exc_info.value)

    def test_variant_any_without_comp_id(self):
        """Test that variant='any' without comp_id raises error."""
        with pytest.raises(exception.ScicodaInputError) as exc_info:
            pdb.ccd(variant="any", category="chem_comp")

        assert "comp_id" in str(exc_info.value)
        assert "variant" in str(exc_info.value)

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

        # Should not raise errors for valid categories (may raise file not found)
        for cat in valid_categories:
            try:
                _ = pdb.ccd(comp_id="ATP", category=cat)
            except exception.ScicodaFileNotFoundError:
                # File not found is acceptable in tests
                pass
            except exception.ScicodaInputError as e:
                # Should not raise input error for valid categories
                if "category" in str(e):
                    pytest.fail(f"Valid category '{cat}' raised InputError")

    @pytest.mark.skipif(
        True,
        reason="CCD files may not exist in test environment"
    )
    def test_load_chem_comp(self):
        """Test loading chemical component data."""
        try:
            df = pdb.ccd(comp_id="ATP", category="chem_comp")
            assert isinstance(df, pl.DataFrame)
            assert len(df) > 0
            assert "id" in df.columns
        except exception.ScicodaFileNotFoundError:
            pytest.skip("CCD data not available")
        except exception.ScicodaMissingDependencyError:
            pytest.skip("CCD dependencies not installed")

    @pytest.mark.skipif(
        True,
        reason="CCD files may not exist in test environment"
    )
    def test_load_multiple_comp_ids(self):
        """Test loading multiple component IDs."""
        try:
            df = pdb.ccd(
                comp_id=["ATP", "ADP", "AMP"],
                category="chem_comp",
            )
            assert isinstance(df, pl.DataFrame)
            assert len(df) >= 3  # Should have at least 3 components
        except exception.ScicodaFileNotFoundError:
            pytest.skip("CCD data not available")
        except exception.ScicodaMissingDependencyError:
            pytest.skip("CCD dependencies not installed")

    @pytest.mark.skipif(
        True,
        reason="CCD files may not exist in test environment"
    )
    def test_load_all_components(self):
        """Test loading all components (no filter)."""
        try:
            df = pdb.ccd(comp_id=None, category="chem_comp", variant="non_aa")
            assert isinstance(df, pl.DataFrame)
            assert len(df) > 100  # Should have many components
        except exception.ScicodaFileNotFoundError:
            pytest.skip("CCD data not available")
        except exception.ScicodaMissingDependencyError:
            pytest.skip("CCD dependencies not installed")

    @pytest.mark.skipif(
        True,
        reason="CCD files may not exist in test environment"
    )
    def test_amino_acid_variant(self):
        """Test loading amino acid variant."""
        try:
            df = pdb.ccd(comp_id="ALA", category="chem_comp", variant="aa")
            assert isinstance(df, pl.DataFrame)
            assert len(df) > 0
        except exception.ScicodaFileNotFoundError:
            pytest.skip("CCD data not available")
        except exception.ScicodaMissingDependencyError:
            pytest.skip("CCD dependencies not installed")

    @pytest.mark.skipif(
        True,
        reason="CCD files may not exist in test environment"
    )
    def test_chem_comp_atom_columns(self):
        """Test that chem_comp_atom has expected columns."""
        try:
            df = pdb.ccd(comp_id="ATP", category="chem_comp_atom")
            expected_cols = {"comp_id", "atom_id", "type_symbol"}
            assert expected_cols.issubset(set(df.columns))
        except exception.ScicodaFileNotFoundError:
            pytest.skip("CCD data not available")
        except exception.ScicodaMissingDependencyError:
            pytest.skip("CCD dependencies not installed")

    @pytest.mark.skipif(
        True,
        reason="CCD files may not exist in test environment"
    )
    def test_chem_comp_bond_columns(self):
        """Test that chem_comp_bond has expected columns."""
        try:
            df = pdb.ccd(comp_id="ATP", category="chem_comp_bond")
            expected_cols = {"comp_id", "atom_id_1", "atom_id_2"}
            assert expected_cols.issubset(set(df.columns))
        except exception.ScicodaFileNotFoundError:
            pytest.skip("CCD data not available")
        except exception.ScicodaMissingDependencyError:
            pytest.skip("CCD dependencies not installed")
