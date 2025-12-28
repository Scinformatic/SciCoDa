"""Fetch data from various online sources and process it into structured formats.

This module provides functions to download and process scientific data from
various online databases and repositories, including:
- Atomic and chemical element data from PubChem
- Protein Data Bank (PDB) Chemical Component Dictionary (CCD)

The processed data is returned as Polars DataFrames with cleaned, validated,
and standardized schemas ready for use or storage.
"""
