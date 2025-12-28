"""Update atomic datasets."""

from pathlib import Path

import polars as pl
import dfhelp

from scicoda.data import _data_dir
from scicoda.create import atom as create_atom


def update_all(data_dir: Path | str | None = None) -> dict[str, dict[Path, pl.DataFrame]]:
    """Update all datasets in the atom data category.

    Parameters
    ----------
    data_dir
        Data directory of the package.
        This must be left as `None`,
        unless you want to generate data files
        in a custom location, without affecting the package data.

    Returns
    -------
    A dictionary mapping dataset names to dictionaries,
    which map file paths to their corresponding DataFrames.
    """
    if data_dir is None:
        data_dir = _data_dir
    out = {
        "periodic_table": periodic_table(data_dir=data_dir)
    }
    return out


def periodic_table(
    data_dir: Path | str | None = None,
    filepath: str = "atom/periodic_table.parquet",
    *,
    url: str = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/periodictable/CSV"
) -> dict[Path, pl.DataFrame]:
    """Update and store the periodic table from PubChem.

    Retrieve the periodic table data from PubChem,
    process it into a polars DataFrame,
    add additional data,
    and save it as a Parquet file.

    Parameters
    ----------
    data_dir
        Data directory of the package.
    filepath
        File path for storing the periodic table Parquet file.

    Returns
    -------
    A dictionary mapping the file path to its corresponding DataFrame.

    References
    ----------
    - [PubChem Periodic Table](https://pubchem.ncbi.nlm.nih.gov/periodic-table/)
    - [IUPAC Cookbook](https://iupac.github.io/WFChemCookbook/datasources/pubchem_ptable.html)
    """
    if data_dir is None:
        data_dir = _data_dir

    df = create_atom.periodic_table(url=url)

    filepath = (Path(data_dir) / filepath).with_suffix(".parquet")
    dfhelp.write_parquet(df, filepath)
    return {filepath: df}
