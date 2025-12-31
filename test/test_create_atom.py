"""Tests for the create.atom module."""

import pytest
import polars as pl
from scicoda.create import atom as create_atom


class TestPeriodicTable:
    """Tests for periodic_table function in create module."""

    @pytest.mark.online
    def test_returns_dataframe(self):
        """Test that the function returns a Polars DataFrame."""
        df = create_atom.periodic_table()
        assert isinstance(df, pl.DataFrame)

    @pytest.mark.online
    def test_element_count(self):
        """Test that all 118 elements are present."""
        df = create_atom.periodic_table()
        assert len(df) == 118

    @pytest.mark.online
    def test_has_expected_columns(self):
        """Test that the DataFrame has all expected columns."""
        df = create_atom.periodic_table()
        expected_cols = {
            "z", "symbol", "name", "period", "group", "block",
            "econfig", "mass", "vdwr", "vdwr_bo", "ie", "ea",
            "en_pauling", "oxstates", "state", "mp", "bp",
            "density", "color_cpk", "year"
        }
        assert set(df.columns) == expected_cols

    @pytest.mark.online
    def test_sorted_by_atomic_number(self):
        """Test that elements are sorted by atomic number."""
        df = create_atom.periodic_table()
        z_values = df["z"].to_list()
        assert z_values == list(range(1, 119))

    @pytest.mark.online
    def test_hydrogen_properties(self):
        """Test that Hydrogen has correct properties."""
        df = create_atom.periodic_table()
        h = df.filter(pl.col("symbol") == "H")

        assert len(h) == 1
        assert h["z"][0] == 1
        assert h["name"][0] == "hydrogen"
        assert h["period"][0] == 1
        assert h["group"][0] == 1

    @pytest.mark.online
    def test_carbon_properties(self):
        """Test that Carbon has correct properties."""
        df = create_atom.periodic_table()
        c = df.filter(pl.col("symbol") == "C")

        assert len(c) == 1
        assert c["z"][0] == 6
        assert c["name"][0] == "carbon"
        assert c["period"][0] == 2
        assert c["group"][0] == 14
        assert c["block"][0] == "nonmetal"

    @pytest.mark.online
    def test_noble_gases(self):
        """Test that noble gases have correct group assignment."""
        df = create_atom.periodic_table()
        noble_gases = df.filter(pl.col("block") == "noble gas")

        # All noble gases should be in group 18
        assert (noble_gases["group"] == 18).all()

        # Should have 7 noble gases (He, Ne, Ar, Kr, Xe, Rn, Og)
        # Note: Og might not be classified yet
        assert len(noble_gases) >= 6

    @pytest.mark.online
    def test_lanthanides_actinides_no_group(self):
        """Test that lanthanides and actinides have null group."""
        df = create_atom.periodic_table()
        lanthanides = df.filter(pl.col("block") == "lanthanide")
        actinides = df.filter(pl.col("block") == "actinide")

        # Lanthanides (except La) and actinides (except Ac) should have null group
        # La and Ac are considered part of group 3
        assert lanthanides["group"].null_count() == len(lanthanides) - 1
        assert actinides["group"].null_count() == len(actinides) - 1

    @pytest.mark.online
    def test_period_assignments(self):
        """Test that period assignments are correct."""
        df = create_atom.periodic_table()

        # Check period boundaries
        assert (df.filter(pl.col("z") <= 2)["period"] == 1).all()
        assert (df.filter((pl.col("z") > 2) & (pl.col("z") <= 10))["period"] == 2).all()
        assert (df.filter((pl.col("z") > 10) & (pl.col("z") <= 18))["period"] == 3).all()
        assert (df.filter((pl.col("z") > 86) & (pl.col("z") <= 118))["period"] == 7).all()

    @pytest.mark.online
    def test_oxidation_states_format(self):
        """Test that oxidation states are properly formatted."""
        df = create_atom.periodic_table()

        # Carbon should have common oxidation states
        c = df.filter(pl.col("symbol") == "C")
        ox_states = c["oxstates"][0]
        assert isinstance(ox_states, pl.Series)
        assert ox_states.dtype == pl.Int8
        assert len(ox_states) > 0

    @pytest.mark.online
    def test_standard_states(self):
        """Test that standard states are properly categorized."""
        df = create_atom.periodic_table()

        # Should only have solid, liquid, or gas (or null for unknown)
        states = df["state"].drop_nulls().unique().to_list()
        assert all(s in ["solid", "liquid", "gas"] for s in states)

        # Most elements should be solid at room temperature
        assert (df["state"] == "solid").sum() > 80

    @pytest.mark.online
    def test_vdw_radii_present(self):
        """Test that van der Waals radii are present."""
        df = create_atom.periodic_table()

        # Most elements should have vdw radius data
        assert df["vdwr"].null_count() < 20
        assert df["vdwr_bo"].null_count() < 20

    @pytest.mark.online
    def test_custom_url(self):
        """Test that custom URL parameter works."""
        # Use the default URL
        df = create_atom.periodic_table(
            url="https://pubchem.ncbi.nlm.nih.gov/rest/pug/periodictable/CSV"
        )
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 118
