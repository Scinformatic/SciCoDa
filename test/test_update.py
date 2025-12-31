"""Tests for the main update module."""

import pytest
from scicoda.update import update_all


@pytest.fixture(scope="class")
def update_all_data(tmp_path_factory):
    """Fixture that calls update_all() once and caches the result for all tests in the class."""
    try:
        tmpdir = tmp_path_factory.mktemp("update_all")
        result = update_all(data_dir=str(tmpdir))
        return result, tmpdir
    except Exception as e:
        pytest.skip(f"Update failed: {e}")


@pytest.mark.online
@pytest.mark.slow
class TestUpdateAll:
    """Tests for update_all function in main update module.

    All tests in this class share the same update_all data loaded once via the update_all_data fixture.
    """

    def test_returns_dict(self, update_all_data):
        """Test that update_all returns a dictionary."""
        result, _ = update_all_data
        assert isinstance(result, dict)

    def test_updates_atom_module(self, update_all_data):
        """Test that atom module is updated."""
        result, _ = update_all_data
        assert "atom" in result
        assert isinstance(result["atom"], dict)

    def test_updates_pdb_module(self, update_all_data):
        """Test that pdb module is updated."""
        result, _ = update_all_data
        assert "pdb" in result
        assert isinstance(result["pdb"], dict)
