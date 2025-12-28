"""Update atomic data files."""

from pathlib import Path

import polars as pl
import dfhelp

from scicoda.data import _data_dir, get_data
from scicoda.create.atom import periodic_table as fetch_periodic_table


def update_all(data_dir: Path | str | None = None) -> dict[str, dict[Path, pl.DataFrame]]:
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
    """Download and store the periodic table from PubChem.

    This function retrieves the periodic table data from PubChem,
    processes it into a polars DataFrame,
    and saves it as a Parquet file.

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
    - [IUPAC Cookbook](https://iupac.github.io/WFChemCookbook/datasources/pubchem_ptable.html)
    """
    if data_dir is None:
        data_dir = _data_dir

    df = fetch_periodic_table(url=url)



    filepath = (Path(data_dir) / filepath).with_suffix(".parquet")
    dfhelp.write_parquet(df, filepath)
    return {filepath: df}
