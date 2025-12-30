"""Protein Data Bank (PDB) datasets."""

from typing import Literal, Sequence, TypeAlias, get_args as get_type_args

import polars as pl

from scicoda import data, exception


_FILE_CATEGORY_NAME = "pdb"
_CCD_CATEGORY_NAMES: TypeAlias = Literal[
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
]


def ccd(
    comp_id: str | Sequence[str] | None = None,
    category: _CCD_CATEGORY_NAMES = "chem_comp",
    variant: Literal["aa", "non_aa", "any"] = "any",
) -> pl.DataFrame:
    """Get a table from the Chemical Component Dictionary (CCD) of the PDB.

    This includes data from both the main CCD
    and the amino acids protonation variants companion dictionary to the CCD,
    which contains extra information about different protonation
    states of standard amino acids.

    Note
    ----
    The CCD datasets are **not bundled** with the package due to their size (~70 MB).
    To use this function, you must first install the optional dependencies:

        pip install scicoda[ccd]

    The first time you call this function, the CCD datasets will be automatically
    downloaded from the PDB, processed, and saved locally for future use.
    This one-time download may take a few minutes depending on your internet connection.

    Parameters
    ----------
    comp_id
        Chemical component ID(s) to filter the table by (case-insensitive).
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
        - "any": both amino acid and non-amino acid components

        Note that while the raw CCD data contains amino acid components
        in both the main CCD and the protonation variants companion dictionary,
        here, they are separated such that
        the "aa" variant only contains amino acid components,
        and the "non_aa" variant only contains non-amino acid components.

    Returns
    -------
    Polars DataFrame containing the requested CCD table data,
    optionally filtered by the specified component ID(s).
    """
    # Validate category name against allowed CCD category names
    if category not in get_type_args(_CCD_CATEGORY_NAMES):
        raise exception.ScicodaInputError(
            parameter="category",
            argument=category,
            message_detail=(
                f"CCD data category must be one of: {', '.join(get_type_args(_CCD_CATEGORY_NAMES))}."
            )
        )

    # Check if CCD data files exist; if not, download and process them
    try:
        data.get_filepath(
            category=_FILE_CATEGORY_NAME,
            name=f"ccd-chem_comp-aa",
            extension="parquet",
        )
    except exception.ScicodaFileNotFoundError:
        try:
            from scicoda.update.pdb import ccd
        except ImportError as ie:
            raise exception.ScicodaMissingDependencyError(
                "The 'scicoda.update.pdb' module is required to download "
                "and process the PDB Chemical Component Dictionary (CCD), "
                "but its required dependencies are not installed. "
                "Please install 'scicoda[ccd]' to use this functionality."
            ) from ie
        # Download and process the CCD data
        _ = ccd()

    variants = ["aa", "non_aa"] if variant == "any" else [variant]

    if comp_id is None:
        dfs = [
            data.get_file(
                category=_FILE_CATEGORY_NAME,
                name=f"ccd-{category}-{var}",
                extension="parquet",
            ) for var in variants
        ]
        return pl.concat(dfs, how="vertical")

    comp_id = [comp_id] if isinstance(comp_id, str) else comp_id
    id_col = "id" if category == "chem_comp" else "comp_id"
    filterby = pl.col(id_col).is_in([cid.lower() for cid in comp_id])
    df: pl.DataFrame = pl.DataFrame()

    for var in variants:
        # Use lazy loading and filter when comp_id is specified
        df = data.get_file(
            category=_FILE_CATEGORY_NAME,
            name=f"ccd-{category}-{var}",
            extension="parquet",
            filterby=filterby,
        )
        if not df.is_empty():
            return df

    # Return empty DataFrame with columns if no matches found
    return df
