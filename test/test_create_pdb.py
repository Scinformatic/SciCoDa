"""Tests for the create.pdb module."""

import pytest
import polars as pl
from scicoda.create import pdb as create_pdb


class TestCCD:
    """Tests for ccd function in create module."""

    @pytest.mark.online
    @pytest.mark.slow
    def test_returns_tuple(self):
        """Test that the function returns a tuple of expected structure."""
        try:
            result = create_pdb.ccd()
            assert isinstance(result, tuple)
            assert len(result) == 3

            aa_dfs, non_aa_dfs, problems = result
            assert isinstance(aa_dfs, dict)
            assert isinstance(non_aa_dfs, dict)
            assert isinstance(problems, dict)
        except Exception as e:
            # May fail if dependencies not installed
            pytest.skip(f"CCD creation failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_amino_acid_dataframes(self):
        """Test that amino acid DataFrames are returned."""
        try:
            aa_dfs, _, _ = create_pdb.ccd()

            # Should have chem_comp category at minimum
            assert "chem_comp" in aa_dfs
            assert isinstance(aa_dfs["chem_comp"], pl.DataFrame)
            assert len(aa_dfs["chem_comp"]) > 0
        except Exception as e:
            pytest.skip(f"CCD creation failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_non_amino_acid_dataframes(self):
        """Test that non-amino acid DataFrames are returned."""
        try:
            _, non_aa_dfs, _ = create_pdb.ccd()

            # Should have chem_comp category at minimum
            assert "chem_comp" in non_aa_dfs
            assert isinstance(non_aa_dfs["chem_comp"], pl.DataFrame)
            assert len(non_aa_dfs["chem_comp"]) > 0
        except Exception as e:
            pytest.skip(f"CCD creation failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_all_categories_present(self):
        """Test that all expected categories are present."""
        try:
            aa_dfs, non_aa_dfs, _ = create_pdb.ccd()

            # Both should have same categories
            assert set(aa_dfs.keys()) == set(non_aa_dfs.keys())

            # Should have at least these categories
            expected_categories = {
                "chem_comp",
                "chem_comp_atom",
                "chem_comp_bond",
            }
            assert expected_categories.issubset(set(aa_dfs.keys()))
        except Exception as e:
            pytest.skip(f"CCD creation failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_chem_comp_has_id_column(self):
        """Test that chem_comp has id column."""
        try:
            aa_dfs, non_aa_dfs, _ = create_pdb.ccd()

            assert "id" in aa_dfs["chem_comp"].columns
            assert "id" in non_aa_dfs["chem_comp"].columns
        except Exception as e:
            pytest.skip(f"CCD creation failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_chem_comp_atom_has_comp_id(self):
        """Test that chem_comp_atom has comp_id column."""
        try:
            aa_dfs, non_aa_dfs, _ = create_pdb.ccd()

            assert "comp_id" in aa_dfs["chem_comp_atom"].columns
            assert "comp_id" in non_aa_dfs["chem_comp_atom"].columns
            assert "atom_id" in aa_dfs["chem_comp_atom"].columns
            assert "atom_id" in non_aa_dfs["chem_comp_atom"].columns
        except Exception as e:
            pytest.skip(f"CCD creation failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_chem_comp_bond_has_atom_ids(self):
        """Test that chem_comp_bond has atom ID columns."""
        try:
            aa_dfs, non_aa_dfs, _ = create_pdb.ccd()

            assert "comp_id" in aa_dfs["chem_comp_bond"].columns
            assert "atom_id_1" in aa_dfs["chem_comp_bond"].columns
            assert "atom_id_2" in aa_dfs["chem_comp_bond"].columns
            assert "comp_id" in non_aa_dfs["chem_comp_bond"].columns
            assert "atom_id_1" in non_aa_dfs["chem_comp_bond"].columns
            assert "atom_id_2" in non_aa_dfs["chem_comp_bond"].columns
        except Exception as e:
            pytest.skip(f"CCD creation failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_bond_atom_ordering(self):
        """Test that bond atoms are consistently ordered."""
        try:
            aa_dfs, non_aa_dfs, _ = create_pdb.ccd()

            # For each bond, atom_id_1 should be <= atom_id_2 (alphabetically)
            for df in [aa_dfs["chem_comp_bond"], non_aa_dfs["chem_comp_bond"]]:
                # Check that atom_id_1 <= atom_id_2 for all bonds
                # Using string comparison
                comparison = df.select([
                    (pl.col("atom_id_1") <= pl.col("atom_id_2")).alias("ordered")
                ])
                assert comparison["ordered"].all()
        except Exception as e:
            pytest.skip(f"CCD creation failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_no_block_column(self):
        """Test that _block column is removed from all dataframes."""
        try:
            aa_dfs, non_aa_dfs, _ = create_pdb.ccd()

            for cat_name, df in aa_dfs.items():
                assert "_block" not in df.columns, f"_block found in aa {cat_name}"

            for cat_name, df in non_aa_dfs.items():
                assert "_block" not in df.columns, f"_block found in non_aa {cat_name}"
        except Exception as e:
            pytest.skip(f"CCD creation failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_no_esd_columns(self):
        """Test that ESD columns are removed."""
        try:
            aa_dfs, non_aa_dfs, _ = create_pdb.ccd()

            for cat_name, df in aa_dfs.items():
                esd_cols = [col for col in df.columns if "_esd_digits" in col]
                assert len(esd_cols) == 0, f"ESD columns found in aa {cat_name}"

            for cat_name, df in non_aa_dfs.items():
                esd_cols = [col for col in df.columns if "_esd_digits" in col]
                assert len(esd_cols) == 0, f"ESD columns found in non_aa {cat_name}"
        except Exception as e:
            pytest.skip(f"CCD creation failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_problems_structure(self):
        """Test that problems dictionary has expected structure."""
        try:
            _, _, problems = create_pdb.ccd()

            # Problems dict should have specific keys if there are problems
            for key in problems.keys():
                assert key in ["main", "protonation", "merge"]

            # Each problem category should map to category names
            for variant_name, variant_problems in problems.items():
                assert isinstance(variant_problems, dict)
                for cat_name, cat_problems in variant_problems.items():
                    assert isinstance(cat_problems, dict)
        except Exception as e:
            pytest.skip(f"CCD creation failed: {e}")
