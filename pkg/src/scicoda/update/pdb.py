"""Update Protein Data Bank (PDB) datasets."""

from pathlib import Path

import dfhelp
import polars as pl

from scicoda.data import _data_dir
from scicoda.create import pdb as create_pdb


def update_all(data_dir: Path | str | None = None) -> dict[str, tuple[dict[Path, pl.DataFrame], dict[str, dict]]]:
    """Update all datasets in the PDB data category.

    Parameters
    ----------
    data_dir
        Data directory of the package.
        This must be left as `None`,
        unless you want to generate data files
        in a custom location, without affecting the package data.

    Returns
    -------
    A dictionary mapping dataset names to tuples,
    each containing a dictionary that maps file paths to their corresponding DataFrames,
    and a dictionary of problems encountered during the update.
    """
    if data_dir is None:
        data_dir = _data_dir
    out = {
        "ccd": ccd(data_dir=data_dir),
    }
    return out


def ccd(
    data_dir: Path | str | None = None,
    basepath: str = "pdb/ccd"
) -> tuple[dict[Path, pl.DataFrame], dict[str, dict]]:
    """Update and store the Chemical Component Dictionary (CCD) from PDB.

    Retrieve both the main CCD and the amino acid protonation variants companion dictionary,
    process them into individual tables,
    separate the amino acid components from the non-amino acid components,
    and save each table as a Parquet file.

    Parameters
    ----------
    data_dir
        Data directory of the package.
        This must be left as `None`,
        unless you want to generate data files
        in a custom location, without affecting the package data.
    basepath
        Base path for storing the CCD tables. Each table will be saved
        with this base path plus the table category name and a suffix indicating
        whether it is from the amino acid variant or non-amino acid variant:
        `{basepath}-{category_name}-{variant_suffix}.parquet` where
        `variant_suffix` is either "aa" for amino acid components
        or "noaa" for non-amino acid components.

    Returns
    -------
    path_to_df_dict
        A dictionary mapping file paths to their corresponding DataFrames.
    problems
        A dictionary containing any problems encountered during processing,
        such as validation errors, duplicate rows, or conflicting rows.
    """

    category_df_aa, category_df_non_aa, problems = create_pdb.ccd()

    if data_dir is None:
        data_dir = _data_dir

    dirpath = Path(data_dir)
    out = {}
    for variant_suffix, cat_dfs in [("aa", category_df_aa), ("noaa", category_df_non_aa)]:
        for cat_name, cat_df in cat_dfs.items():
            filepath = (dirpath / f"{basepath}-{cat_name}-{variant_suffix}").with_suffix(".parquet")
            filepath.parent.mkdir(parents=True, exist_ok=True)
            dfhelp.write_parquet(cat_df, filepath=filepath)
            out[filepath] = cat_df
    return out, problems
