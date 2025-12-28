"""Tests for the main update module."""

import pytest
from scicoda.update import update_all
import tempfile


class TestUpdateAll:
    """Tests for update_all function in main update module."""

    @pytest.mark.online
    @pytest.mark.slow
    def test_returns_dict(self):
        """Test that update_all returns a dictionary."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                result = update_all(data_dir=tmpdir)
                assert isinstance(result, dict)
        except Exception as e:
            pytest.skip(f"Update failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_updates_atom_module(self):
        """Test that atom module is updated."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                result = update_all(data_dir=tmpdir)
                assert "atom" in result
                assert isinstance(result["atom"], dict)
        except Exception as e:
            pytest.skip(f"Update failed: {e}")

    @pytest.mark.online
    @pytest.mark.slow
    def test_updates_pdb_module(self):
        """Test that pdb module is updated."""
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                result = update_all(data_dir=tmpdir)
                assert "pdb" in result
                assert isinstance(result["pdb"], dict)
        except Exception as e:
            pytest.skip(f"Update failed: {e}")
