"""Tests for the create.pdb module."""

import pytest
import polars as pl
from scicoda.create import pdb as create_pdb


@pytest.fixture(scope="class")
def ccd_data(request):
    """Fixture that calls create_pdb.ccd() once and caches the result for all tests in the class."""
    return create_pdb.ccd()


@pytest.mark.online
@pytest.mark.slow
class TestCCD:
    """Tests for ccd function in create module.

    All tests in this class share the same CCD data loaded once via the ccd_data fixture.
    """

    def test_returns_tuple(self, ccd_data):
        """Test that the function returns a tuple of expected structure."""
        result = ccd_data
        assert isinstance(result, tuple)
        assert len(result) == 3

        aa_dfs, non_aa_dfs, problems = result
        assert isinstance(aa_dfs, dict)
        assert isinstance(non_aa_dfs, dict)
        assert isinstance(problems, dict)

    def test_amino_acid_dataframes(self, ccd_data):
        """Test that amino acid DataFrames are returned."""
        aa_dfs, _, _ = ccd_data

        # Should have chem_comp category at minimum
        assert "chem_comp" in aa_dfs
        assert isinstance(aa_dfs["chem_comp"], pl.DataFrame)
        assert len(aa_dfs["chem_comp"]) > 0

    def test_non_amino_acid_dataframes(self, ccd_data):
        """Test that non-amino acid DataFrames are returned."""
        _, non_aa_dfs, _ = ccd_data

        # Should have chem_comp category at minimum
        assert "chem_comp" in non_aa_dfs
        assert isinstance(non_aa_dfs["chem_comp"], pl.DataFrame)
        assert len(non_aa_dfs["chem_comp"]) > 0

    def test_all_categories_present(self, ccd_data):
        """Test that all expected categories are present."""
        aa_dfs, non_aa_dfs, _ = ccd_data

        # Both should have same categories
        assert set(aa_dfs.keys()) == set(non_aa_dfs.keys())

        # Should have at least these categories
        expected_categories = {
            "chem_comp",
            "chem_comp_atom",
            "chem_comp_bond",
        }
        assert expected_categories.issubset(set(aa_dfs.keys()))

    def test_chem_comp_has_id_column(self, ccd_data):
        """Test that chem_comp has id column."""
        aa_dfs, non_aa_dfs, _ = ccd_data

        assert "id" in aa_dfs["chem_comp"].columns
        assert "id" in non_aa_dfs["chem_comp"].columns

    def test_chem_comp_atom_has_comp_id(self, ccd_data):
        """Test that chem_comp_atom has comp_id column."""
        aa_dfs, non_aa_dfs, _ = ccd_data

        assert "comp_id" in aa_dfs["chem_comp_atom"].columns
        assert "comp_id" in non_aa_dfs["chem_comp_atom"].columns
        assert "atom_id" in aa_dfs["chem_comp_atom"].columns
        assert "atom_id" in non_aa_dfs["chem_comp_atom"].columns

    def test_chem_comp_bond_has_atom_ids(self, ccd_data):
        """Test that chem_comp_bond has atom ID columns."""
        aa_dfs, non_aa_dfs, _ = ccd_data

        assert "comp_id" in aa_dfs["chem_comp_bond"].columns
        assert "atom_id_1" in aa_dfs["chem_comp_bond"].columns
        assert "atom_id_2" in aa_dfs["chem_comp_bond"].columns
        assert "comp_id" in non_aa_dfs["chem_comp_bond"].columns
        assert "atom_id_1" in non_aa_dfs["chem_comp_bond"].columns
        assert "atom_id_2" in non_aa_dfs["chem_comp_bond"].columns

    def test_bond_atom_ordering(self, ccd_data):
        """Test that bond atoms are consistently ordered."""
        aa_dfs, non_aa_dfs, _ = ccd_data

        # For each bond, atom_id_1 should be <= atom_id_2 (alphabetically)
        for df in [aa_dfs["chem_comp_bond"], non_aa_dfs["chem_comp_bond"]]:
            # Check that atom_id_1 <= atom_id_2 for all bonds
            # Using string comparison
            comparison = df.select([
                (pl.col("atom_id_1") <= pl.col("atom_id_2")).alias("ordered")
            ])
            assert comparison["ordered"].all()

    def test_no_block_column(self, ccd_data):
        """Test that _block column is removed from all dataframes."""
        aa_dfs, non_aa_dfs, _ = ccd_data

        for cat_name, df in aa_dfs.items():
            assert "_block" not in df.columns, f"_block found in aa {cat_name}"

        for cat_name, df in non_aa_dfs.items():
            assert "_block" not in df.columns, f"_block found in non_aa {cat_name}"

    def test_no_esd_columns(self, ccd_data):
        """Test that ESD columns are removed."""
        aa_dfs, non_aa_dfs, _ = ccd_data

        for cat_name, df in aa_dfs.items():
            esd_cols = [col for col in df.columns if "_esd_digits" in col]
            assert len(esd_cols) == 0, f"ESD columns found in aa {cat_name}"

        for cat_name, df in non_aa_dfs.items():
            esd_cols = [col for col in df.columns if "_esd_digits" in col]
            assert len(esd_cols) == 0, f"ESD columns found in non_aa {cat_name}"

    def test_problems_structure(self, ccd_data):
        """Test that problems dictionary has expected structure."""
        _, _, problems = ccd_data

        # Problems dict should have specific keys if there are problems
        for key in problems.keys():
            assert key in ["main", "protonation", "merge"]

        # Each problem category should map to category names
        for variant_name, variant_problems in problems.items():
            assert isinstance(variant_problems, dict)
            for cat_name, cat_problems in variant_problems.items():
                assert isinstance(cat_problems, dict)
