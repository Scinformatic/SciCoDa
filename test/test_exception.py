"""Tests for the exception module."""

import pytest
from scicoda import exception


class TestScicodaError:
    """Tests for ScicodaError base class."""

    def test_init(self):
        """Test basic exception initialization."""
        msg = "Test error message"
        error = exception.ScicodaError(msg)
        assert str(error) == msg
        assert error.message == msg

    def test_inheritance(self):
        """Test that ScicodaError inherits from Exception."""
        error = exception.ScicodaError("test")
        assert isinstance(error, Exception)


class TestScicodaMissingDependencyError:
    """Tests for ScicodaMissingDependencyError."""

    def test_init(self):
        """Test initialization with message details."""
        details = "Please install optional dependencies"
        error = exception.ScicodaMissingDependencyError(details)

        assert isinstance(error, exception.ScicodaError)
        assert details in str(error)
        assert "Missing required dependency" in str(error)
        assert hasattr(error, "module")

    def test_module_detection(self):
        """Test that the calling module is detected."""
        error = exception.ScicodaMissingDependencyError("test")
        # The module should be set (though it may be "__main__" in tests)
        assert error.module is not None


class TestScicodaInputError:
    """Tests for ScicodaInputError."""

    def test_init(self):
        """Test initialization with parameter, argument, and message."""
        param = "test_param"
        arg = "invalid_value"
        detail = "Value must be positive"

        error = exception.ScicodaInputError(param, arg, detail)

        assert isinstance(error, exception.ScicodaError)
        assert param in str(error)
        assert str(arg) in str(error)
        assert detail in str(error)
        assert error.parameter == param
        assert error.argument == arg

    def test_function_detection(self):
        """Test that the calling function is detected."""
        error = exception.ScicodaInputError("param", "arg", "detail")
        # The function name should be set
        assert hasattr(error, "function")
        assert error.function is not None


class TestScicodaFileNotFoundError:
    """Tests for ScicodaFileNotFoundError."""

    def test_init(self):
        """Test initialization with category, name, and filepath."""
        from pathlib import Path

        category = "atom"
        name = "test_data"
        filepath = Path("/path/to/data.yaml")

        error = exception.ScicodaFileNotFoundError(category, name, filepath)

        assert isinstance(error, exception.ScicodaError)
        assert category in str(error)
        assert name in str(error)
        assert str(filepath) in str(error)
        assert error.category == category
        assert error.name == name
        assert error.filepath == filepath

    def test_attributes(self):
        """Test that all attributes are properly set."""
        from pathlib import Path

        category = "pdb"
        name = "test_file"
        filepath = Path("/tmp/test.parquet")

        error = exception.ScicodaFileNotFoundError(category, name, filepath)

        assert error.category == category
        assert error.name == name
        assert error.filepath == filepath
