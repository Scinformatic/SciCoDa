"""SciCoDa: Scientific constants and data.

SciCoDa provides curated scientific datasets and constants for computational sciences,
including computational chemistry, structural biology, and related fields.

The package offers easy access to:
- Protein Data Bank (PDB) Chemical Component Dictionary (CCD) data
- Atomic and periodic table data from authoritative sources (PubChem, Blue Obelisk)
- AutoDock atom types and properties

All data is provided as Polars DataFrames for efficient manipulation and analysis.

Modules
-------
atom
    Atomic datasets including periodic table and AutoDock atom types.
pdb
    Protein Data Bank datasets including the Chemical Component Dictionary.

Examples
--------
>>> import scicoda
>>> # Get periodic table data
>>> df = scicoda.atom.periodic_table()
>>> # Get AutoDock atom types
>>> df = scicoda.atom.autodock_atom_types()
>>> # Get PDB chemical component data
>>> df = scicoda.pdb.ccd()
"""

from scicoda import atom, pdb

__all__ = [
    "atom",
    "pdb",
]
