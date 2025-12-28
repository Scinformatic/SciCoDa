"""Protein Data Bank (PDB) """

from typing import Literal

import polars as pl

from scicoda import data


def ccd(
    category: Literal[
        "chem_comp",
        "chem_comp_atom",
        "chem_comp_bond",
        "pdbx_chem_comp_atom_related",
        "pdbx_chem_comp_audit",
        "pdbx_chem_comp_descriptor",
        "pdbx_chem_comp_feature",
        "pdbx_chem_comp_identifier",
        "pdbx_chem_comp_pcm",
        "pdbx_chem_comp_related",
        "pdbx_chem_comp_synonyms",
    ],
    cache: bool = True
) -> pl.DataFrame:
    """Get a table from the Chemical Component Dictionary (CCD) of the PDB.

    This includes data from both the CCD
    and the companion dictionary to the CCD,
    which contains extra information about different protonation
    states of standard amino acids.

    Parameters
    ----------
    category
        Name of the CCD table to retrieve.
        Must be one of the supported categories.
    cache
        Retain the data in memory after reading it
        for faster access in subsequent calls.

    Returns
    -------
    A `polars.DataFrame` containing the requested CCD table.
    """
    return data.get(
        "pdb",
        name=f"ccd-{category}-non_aa",
        extension="parquet",
        cache=cache
    ).clone()


def ccd_aa(
    category: Literal[
        "chem_comp",
        "chem_comp_atom",
        "chem_comp_bond",
        "pdbx_chem_comp_atom_related",
        "pdbx_chem_comp_audit",
        "pdbx_chem_comp_descriptor",
        "pdbx_chem_comp_feature",
        "pdbx_chem_comp_identifier",
        "pdbx_chem_comp_pcm",
        "pdbx_chem_comp_related",
        "pdbx_chem_comp_synonyms",
    ] = "chem_comp",
    cache: bool = True
) -> pl.DataFrame:
    """Get a table from the Chemical Component Dictionary (CCD) of the PDB.

    This includes data from both the CCD
    and the companion dictionary to the CCD,
    which contains extra information about different protonation
    states of standard amino acids.

    Parameters
    ----------
    category
        Name of the CCD table to retrieve.
        Must be one of the supported categories.
    cache
        Retain the data in memory after reading it
        for faster access in subsequent calls.

    Returns
    -------
    A `polars.DataFrame` containing the requested CCD table.
    """
    return data.get(
        "pdb",
        name=f"ccd-{category}-aa",
        extension="parquet",
        cache=cache
    ).clone()
