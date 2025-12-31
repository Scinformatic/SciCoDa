"""Tests for the atom module."""

import json
from pathlib import Path

import pytest
import polars as pl
import yaml
import jsonschema

from scicoda import atom


# Paths to schema and data directories
SCHEMA_DIR = Path(__file__).parent / "data_schema" / "atom"
DATA_DIR = Path(__file__).parent.parent / "pkg" / "src" / "scicoda" / "data" / "atom"


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


class TestAtomDataSchemaValidation:
    """Tests for validating atom data files against their JSON schemas."""

    @pytest.fixture
    def autodock_schema(self):
        """Load the AutoDock atom types JSON schema."""
        schema_path = SCHEMA_DIR / "autodock_atom_types.yaml"
        with open(schema_path, "r") as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def autodock_data(self):
        """Load the AutoDock atom types data file."""
        data_path = DATA_DIR / "autodock_atom_types.json"
        with open(data_path, "r") as f:
            return json.load(f)

    @pytest.fixture
    def radii_vdw_schema(self):
        """Load the VdW radii JSON schema."""
        schema_path = SCHEMA_DIR / "radii_vdw_blue_obelisk.yaml"
        with open(schema_path, "r") as f:
            return yaml.safe_load(f)

    @pytest.fixture
    def radii_vdw_data(self):
        """Load the VdW radii data file."""
        data_path = DATA_DIR / "radii_vdw_blue_obelisk.json"
        with open(data_path, "r") as f:
            return json.load(f)

    def test_autodock_atom_types_valid_schema(self, autodock_schema, autodock_data):
        """Test that autodock_atom_types.json validates against its schema."""
        jsonschema.validate(instance=autodock_data, schema=autodock_schema)

    def test_autodock_atom_types_is_array(self, autodock_data):
        """Test that the AutoDock data is a non-empty array."""
        assert isinstance(autodock_data, list)
        assert len(autodock_data) > 0

    def test_autodock_atom_types_required_fields(self, autodock_data):
        """Test that each entry has all required fields."""
        required_fields = {"type", "element", "hbond_acceptor", "hbond_donor", "hbond_count"}
        for entry in autodock_data:
            assert required_fields.issubset(set(entry.keys()))

    def test_radii_vdw_valid_schema(self, radii_vdw_schema, radii_vdw_data):
        """Test that radii_vdw_blue_obelisk.json validates against its schema."""
        jsonschema.validate(instance=radii_vdw_data, schema=radii_vdw_schema)

    def test_radii_vdw_has_118_elements(self, radii_vdw_data):
        """Test that the VdW radii data contains exactly 118 elements."""
        assert len(radii_vdw_data) == 118

    def test_radii_vdw_required_fields(self, radii_vdw_data):
        """Test that each entry has element and radius fields."""
        for entry in radii_vdw_data:
            assert "element" in entry
            assert "radius" in entry
            assert isinstance(entry["element"], str)
            assert isinstance(entry["radius"], int)
            assert entry["radius"] > 0

    def test_radii_vdw_element_symbols(self, radii_vdw_data):
        """Test that element symbols have valid length."""
        for entry in radii_vdw_data:
            assert 1 <= len(entry["element"]) <= 2
