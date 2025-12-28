# SciCoDa Test Suite

This directory contains comprehensive tests for the SciCoDa package using pytest.

## Test Structure

```
test/
├── conftest.py              # Pytest configuration and fixtures
├── test_exception.py        # Tests for exception module
├── test_data.py             # Tests for data loading module
├── test_atom.py             # Tests for atom module (API)
├── test_pdb.py              # Tests for pdb module (API)
├── test_create_atom.py      # Tests for create.atom module
├── test_create_pdb.py       # Tests for create.pdb module
├── test_update_atom.py      # Tests for update.atom module
├── test_update_pdb.py       # Tests for update.pdb module
└── test_update.py           # Tests for main update module
```

## Running Tests

### Run All Tests

```bash
pytest test/
```

### Run Specific Test File

```bash
pytest test/test_exception.py
```

### Run Specific Test Class or Function

```bash
pytest test/test_atom.py::TestAutodockAtomTypes
pytest test/test_atom.py::TestAutodockAtomTypes::test_returns_dataframe
```

### Run with Verbose Output

```bash
pytest test/ -v
```

### Run with Coverage

```bash
pytest test/ --cov=scicoda --cov-report=html
```

## Test Markers

Tests are organized with the following markers:

### `slow`

Marks tests that take a long time to run (e.g., downloading data from online sources).

**Skip slow tests:**
```bash
pytest test/ -m "not slow"
```

**Run only slow tests:**
```bash
pytest test/ -m "slow"
```

### `online`

Marks tests that require an internet connection to fetch data from online sources.

**Skip online tests:**
```bash
pytest test/ -m "not online"
```

**Run only online tests:**
```bash
pytest test/ -m "online"
```

### Combined Markers

```bash
# Run fast, offline tests only
pytest test/ -m "not slow and not online"

# Run all online tests including slow ones
pytest test/ -m "online"
```

## Test Coverage

The test suite covers:

- ✅ **Exception Handling**: All custom exceptions and error messages
- ✅ **Data Loading**: JSON and Parquet file loading, caching, filepath resolution
- ✅ **Atom Module**: Periodic table and AutoDock atom types API
- ✅ **PDB Module**: Chemical Component Dictionary API with various categories
- ✅ **Create Module**: Data fetching and processing from online sources
- ✅ **Update Module**: Updating and saving package data files

## Test Categories

### Unit Tests

Test individual functions and classes in isolation:
- `test_exception.py`
- `test_data.py`

### Integration Tests

Test module interfaces and data flow:
- `test_atom.py`
- `test_pdb.py`

### System Tests

Test end-to-end workflows including online data fetching:
- `test_create_*.py`
- `test_update_*.py`

## Writing New Tests

### Basic Test Structure

```python
import pytest
from scicoda import module


class TestFeature:
    """Tests for a specific feature."""

    def test_basic_functionality(self):
        """Test description."""
        result = module.function()
        assert result is not None

    @pytest.mark.slow
    def test_slow_operation(self):
        """Test that takes time."""
        # ...

    @pytest.mark.online
    def test_online_fetch(self):
        """Test that requires internet."""
        # ...
```

### Using Fixtures

Add fixtures in `conftest.py` for shared test data:

```python
@pytest.fixture
def sample_dataframe():
    """Provide a sample DataFrame for testing."""
    return pl.DataFrame({"col": [1, 2, 3]})
```

### Handling Missing Dependencies

For tests that require optional dependencies:

```python
try:
    result = function_with_optional_deps()
except Exception as e:
    pytest.skip(f"Optional dependency not available: {e}")
```

## Continuous Integration

The test suite is designed to work in CI/CD environments:

- Tests requiring data files gracefully skip if files don't exist
- Online tests are marked and can be selectively excluded
- Slow tests can be excluded for faster CI runs

### Example CI Command

```bash
# Fast, offline tests for quick feedback
pytest test/ -m "not slow and not online" -v

# Full test suite (run nightly)
pytest test/ -v --cov=scicoda
```

## Test Data

Tests use:
- **Bundled data**: JSON files in `pkg/src/scicoda/data/`
- **Data schemas**: YAML schemas in `test/data_schema/` for validation
- **Generated data**: Temporary files created during tests
- **Online data**: Downloaded during marked `@online` tests

Tests clean up temporary files automatically using `tempfile.TemporaryDirectory()`.

## Troubleshooting

### Tests Fail Due to Missing Files

Some tests require data files that may not be present. These tests should automatically skip:

```python
@pytest.mark.skipif(
    not file.exists(),
    reason="Data file not found"
)
```

### Tests Fail Due to Missing Dependencies

Install optional dependencies:

```bash
pip install "SciCoDa[ccd]"
```

### Timeout on Online Tests

Online tests may timeout with slow connections. Increase timeout or skip:

```bash
pytest test/ -m "not online"
```

## Contributing

When contributing new features:

1. Add corresponding tests in the appropriate test file
2. Mark tests with appropriate markers (`@pytest.mark.slow`, `@pytest.mark.online`)
3. Ensure tests pass locally: `pytest test/ -v`
4. Aim for >80% code coverage for new code

## Additional Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest markers](https://docs.pytest.org/en/stable/example/markers.html)
- [pytest fixtures](https://docs.pytest.org/en/stable/fixture.html)
