"""Tests for the atom module."""

import pytest
import polars as pl
from scicoda import atom


class TestAutodockAtomTypes:
    """Tests for autodock_atom_types function."""

    def test_returns_dataframe(self):
        """Test that the function returns a Polars DataFrame."""
        df = atom.autodock_atom_types()
        assert isinstance(df, pl.DataFrame)

    def test_has_expected_columns(self):
        """Test that the DataFrame has all expected columns."""
        df = atom.autodock_atom_types()
        expected_cols = {
            "type", "element", "description",
            "hbond_acceptor", "hbond_donor", "hbond_count"
        }
        assert set(df.columns) == expected_cols

    def test_column_types(self):
        """Test that columns have correct data types."""
        df = atom.autodock_atom_types()

        # Check string columns
        assert df["type"].dtype == pl.Utf8
        assert df["element"].dtype == pl.Utf8
        assert df["description"].dtype == pl.Utf8

        # Check boolean columns
        assert df["hbond_acceptor"].dtype == pl.Boolean
        assert df["hbond_donor"].dtype == pl.Boolean

        # Check numeric column
        assert df["hbond_count"].dtype == pl.UInt8

    def test_not_empty(self):
        """Test that the DataFrame contains data."""
        df = atom.autodock_atom_types()
        assert len(df) > 0

    def test_known_atom_types(self):
        """Test that common AutoDock atom types are present."""
        df = atom.autodock_atom_types()
        atom_types = df["type"].to_list()
        for expected_type in [
            "H", "HD", "HS",
            "C", "A", "N", "NA", "NS", "OA", "OS", "F",
            "Mg", "P", "S", "SA", "Cl", "Ca", "Mn", "Fe",
            "Zn", "Br", "I"
        ]:
            assert expected_type in atom_types

    def test_hbond_properties(self):
        """Test hydrogen bonding properties."""
        df = atom.autodock_atom_types()

        # HD should be a hydrogen bond donor
        hd = df.filter(pl.col("type") == "HD")
        assert len(hd) == 1
        assert hd["hbond_donor"][0] == True
        assert hd["hbond_acceptor"][0] == False

        # OA should be a hydrogen bond acceptor
        oa = df.filter(pl.col("type") == "OA")
        assert len(oa) == 1
        assert oa["hbond_acceptor"][0] == True
        assert oa["hbond_donor"][0] == False

    def test_mutually_exclusive_hbond(self):
        """Test that acceptor and donor properties are mutually exclusive."""
        df = atom.autodock_atom_types()

        # No atom should be both acceptor and donor
        both = df.filter(
            pl.col("hbond_acceptor") & pl.col("hbond_donor")
        )
        assert len(both) == 0


class TestPeriodicTable:
    """Tests for periodic_table function."""

    def test_returns_dataframe(self):
        """Test that the function returns a Polars DataFrame."""
        df = atom.periodic_table()
        assert isinstance(df, pl.DataFrame)

    def test_has_expected_columns(self):
        """Test that the DataFrame has expected columns."""
        df = atom.periodic_table()
        expected_cols = {
            "z", "symbol", "name", "period", "group", "block",
            "econfig", "mass", "vdwr", "vdwr_bo", "ie", "ea",
            "en_pauling", "oxstates", "state", "mp", "bp",
            "density", "color_cpk", "year"
        }
        assert expected_cols.issubset(set(df.columns))

    def test_element_count(self):
        """Test that all 118 elements are present."""
        df = atom.periodic_table()
        assert len(df) == 118

    def test_known_elements(self):
        """Test that known elements are present with correct properties."""
        df = atom.periodic_table()

        # Test Hydrogen
        h = df.filter(pl.col("symbol") == "H")
        assert len(h) == 1
        assert h["z"][0] == 1
        assert h["name"][0] == "hydrogen"

        # Test Carbon
        c = df.filter(pl.col("symbol") == "C")
        assert len(c) == 1
        assert c["z"][0] == 6
        assert c["name"][0] == "carbon"

        # Test Oxygen
        o = df.filter(pl.col("symbol") == "O")
        assert len(o) == 1
        assert o["z"][0] == 8
        assert o["name"][0] == "oxygen"
