# SciCoDa

**Scientific Constants and Data** for computational sciencens,
including computational chemistry and structural biology.

SciCoDa is a Python package that provides curated, validated scientific data from authoritative sources.
It offers efficient access to atomic properties, periodic table data, and Protein Data Bank resources
through a clean API using Polars DataFrames.

## Features

- 🔬 **Periodic Table Data**: Complete periodic table with properties from PubChem and Blue Obelisk
- ⚛️ **AutoDock Atom Types**: Atom type definitions and properties for AutoDock4/AutoGrid4
- 🧬 **PDB Chemical Component Dictionary**: Complete CCD with amino acid protonation variants
- 📊 **Efficient Data Structures**: All data returned as Polars DataFrames
- 🔄 **Data Updates**: Built-in tools to update datasets from online sources
- ✅ **Validated Data**: Automatic schema validation and type checking

## Installation

### Basic Installation

```bash
pip install scicoda
```

### With Optional Dependencies

For PDB Chemical Component Dictionary (CCD) access:

```bash
pip install "scicoda[ccd]"
```

**Note:** CCD datasets are **not bundled** with the package due to their size (~70 MB).
The optional `ccd` dependencies are required to download and process the data.
The first time you call `scicoda.pdb.ccd()`, the datasets will be automatically
downloaded from the PDB and saved locally for future use.

## Quick Start

### Periodic Table Data

```python
import scicoda
import polars as pl

# Get complete periodic table
df = scicoda.atom.periodic_table()
print(df.head())

# Access specific properties
carbon = df.filter(pl.col("symbol") == "C")
print(f"Carbon atomic mass: {carbon['mass'][0]} u")
```

### AutoDock Atom Types

```python
# Get AutoDock4 atom types
df = scicoda.atom.autodock_atom_types()

# Filter hydrogen bond acceptors
hb_acceptors = df.filter(pl.col("hbond_acceptor"))
print(hb_acceptors)
```

### PDB Chemical Component Dictionary

```python
# Note: Requires pip install "scicoda[ccd]"
# First call will auto-download CCD data (~70 MB, one-time)

# Get chemical component information
atp = scicoda.pdb.ccd(comp_id="ATP", category="chem_comp")
print(atp)

# Get atom details for a component
atp_atoms = scicoda.pdb.ccd(comp_id="ATP", category="chem_comp_atom")
print(atp_atoms)

# Get bond information
atp_bonds = scicoda.pdb.ccd(comp_id="ATP", category="chem_comp_bond")
print(atp_bonds)
```

## API Reference

### `scicoda.atom`

#### `periodic_table() -> pl.DataFrame`

Returns the periodic table with comprehensive element properties.

**Columns:**
- `z`: Atomic number
- `symbol`: Element symbol
- `name`: Element name
- `period`, `group`: Position in periodic table
- `block`: Group block (e.g., "alkali metal", "noble gas")
- `mass`: Atomic mass (u)
- `vdwr`, `vdwr_bo`: van der Waals radii (pm)
- `ie`, `ea`: Ionization energy and electron affinity (eV)
- `en_pauling`: Pauling electronegativity
- `mp`, `bp`: Melting and boiling points (K)
- `density`: Density (g/cm³)
- And more...

#### `autodock_atom_types() -> pl.DataFrame`

Returns AutoDock4 atom type definitions.

**Columns:**
- `type`: AutoDock atom type (e.g., "C", "HD", "OA")
- `element`: Chemical element symbol
- `description`: Atom type description
- `hbond_acceptor`, `hbond_donor`: H-bonding properties
- `hbond_count`: Number of possible H-bonds

### `scicoda.pdb`

#### `ccd(comp_id, category, variant) -> pl.DataFrame`

Retrieves data from the PDB Chemical Component Dictionary.

**Important:** Requires `pip install "scicoda[ccd]"`. CCD datasets (~70 MB) are not
bundled and will be auto-downloaded on first use.

**Parameters:**
- `comp_id` (str | list[str] | None): Component ID(s) to filter by
- `category` (str): CCD category name (e.g., "chem_comp", "chem_comp_atom", "chem_comp_bond")
- `variant` ("aa" | "non_aa" | "any"): Amino acid or non-amino acid components

**Available Categories:**
- `chem_comp`: Component summary information
- `chem_comp_atom`: Atom details
- `chem_comp_bond`: Bond information
- `pdbx_chem_comp_identifier`: Alternative identifiers
- `pdbx_chem_comp_descriptor`: Chemical descriptors (SMILES, InChI, etc.)
- And more...

## Data Sources

- **Periodic Table**: [PubChem](https://pubchem.ncbi.nlm.nih.gov/periodic-table/)
- **van der Waals Radii**: [Blue Obelisk Data Repository](https://github.com/openbabel/opendata)
- **AutoDock Atom Types**: AutoDockTools (MGLTools)
- **PDB CCD**: [RCSB Protein Data Bank](https://www.rcsb.org/)

## Advanced Usage

### Updating Datasets

To update all datasets with the latest data from online sources:

```python
from scicoda.update import update_all

# Update all datasets (requires write access to package directory)
results = update_all()
```

### Creating Custom Datasets

You can fetch and process data without saving to the package:

```python
from scicoda.create.atom import periodic_table

# Fetch latest periodic table data
df = periodic_table()
df.write_parquet("my_periodic_table.parquet")
```

### Lazy Loading for Large Queries

When filtering specific components, SciCoDa uses lazy evaluation for efficiency:

```python
# Only loads matching rows from disk
df = scicoda.pdb.ccd(comp_id=["ATP", "ADP", "AMP"], category="chem_comp_atom")
```

## Requirements

- Python ≥ 3.12
- polars ≥ 1.0
- pkgdata ≥ 0.1
- dfhelp == 0.0.1

### Optional Dependencies

For CCD data access (required to download and process CCD datasets):
- pdbapi
- ciffile

Install with: `pip install "scicoda[ccd]"`

## Development

### Running Tests

```bash
pytest test/
```

### Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

See the LICENSE file for details.


## Acknowledgments

SciCoDa aggregates data from multiple authoritative scientific databases. We gratefully acknowledge:

- The PubChem team at NCBI
- The RCSB Protein Data Bank
- The Blue Obelisk community
- The AutoDock development team
