"""Protein Data Bank (PDB) datasets."""

from typing import Literal, Sequence

import polars as pl

from scicoda import data


def ccd(
    comp_id: str | Sequence[str] | None = None,
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
    variant: Literal["aa", "non_aa", "any"] = "non_aa",
    *,
    cache: bool = True,
) -> pl.DataFrame:
    """Get a table from the Chemical Component Dictionary (CCD) of the PDB.

    This includes data from both the main CCD
    and the amino acids protonation variants companion dictionary to the CCD,
    which contains extra information about different protonation
    states of standard amino acids.

    Parameters
    ----------
    comp_id
        Chemical component ID(s) to filter the table by.
        If `None`, the entire table is loaded and returned,
        otherwise the table is scanned on disk,
        and only rows matching the specified component ID(s) are collected and returned.
    category
        Name of the CCD table (mmCIF data category) to retrieve.
        Must be one of the available CCD categories.
    variant
        Variant of the CCD to retrieve; one of:
        - "aa": only standard amino acid components and their protonation variants
        - "non_aa": only non-amino acid components
        - "any": search both variants for the specified `comp_id`(s)
          If "any" is selected, `comp_id` must be specified.

        Note that while the raw CCD data contains amino acid components
        in both the main CCD and the protonation variants companion dictionary,
        here, they are separated such that
        the "aa" variant only contains amino acid components,
        and the "non_aa" variant only contains non-amino acid components.
    cache
        Retain the data in memory after reading it
        for faster access in subsequent calls.

    Returns
    -------
    Polars DataFrame containing the requested CCD table data,
    optionally filtered by the specified component ID(s).
    """
    if comp_id is None and variant == "any":
        raise ValueError("If 'variant' is 'any', 'comp_id' must be specified.")
    variants = ["aa", "non_aa"] if variant == "any" else [variant]
    comp_id = [comp_id] if isinstance(comp_id, str) else comp_id
    id_col = "id" if category == "chem_comp" else "comp_id"
    for var in variants:
        df = data.get(
            "pdb",
            name=f"ccd-{category}-{var}",
            extension="parquet",
            cache=cache,
            lazy=comp_id is not None,
        )
        if comp_id is None:
            return df.clone()
        df_collected = df.filter(pl.col(id_col).is_in(comp_id)).collect()
        if not df_collected.is_empty():
            return df_collected
    return df_collected
