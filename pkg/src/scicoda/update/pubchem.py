from pathlib import Path
import re

import pandas as pd

from scicoda.data import _data_dir
from scicoda.update.io import write_df


def update_all(data_dir: Path | str | None = None):
    if data_dir is None:
        data_dir = _data_dir
    periodic_table(data_dir=data_dir)
    return


def periodic_table(
    data_dir: Path | str | None = None,
    filepath: str = "atom/periodic_table.parquet",
    *,
    url: str = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/periodictable/CSV"
) -> dict[Path, pd.DataFrame]:
    """Download and store the periodic table from PubChem.

    This function retrieves the periodic table data from PubChem,
    processes it into a DataFrame,
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

    df = pd.read_csv(url)

    # Convert column names from 'CamelCase' to 'snake_case'
    df = df.rename(columns={col: _camel_to_snake(col) for col in df.columns}).rename(
        columns={
            "atomic_number": "z",
            "atomic_mass": "mass",
            "c_p_k_hex_color": "cpk_color",
            "electron_configuration": "e_config",
            "electronegativity": "en_pauling",
            "atomic_radius": "r",
            "ionization_energy": "ie",
            "electron_affinity": "ea",
            "oxidation_states": "ox_states",
            "standard_state": "state",
            "melting_point": "mp",
            "boiling_point": "bp",
            "density": "density",
            "group_block": "block",
            "year_discovered": "year",
        }
    )

    # Oxidation states are stored as comma-separated strings;
    # convert them to tuples of integers (or None if missing).
    df["ox_states"] = df["ox_states"].apply(
        lambda xs: tuple(int(x) for x in xs.split(",")) if isinstance(xs, str) else None
    )

    # Standard states are ['Gas', 'Solid', 'Liquid', 'Expected to be a Solid', 'Expected to be a Gas'],
    # convert them to lowercase and simplify the expected states.
    df["state"] = df["state"].apply(
        lambda s: s.lower().replace("expected to be a ", "") if isinstance(s, str) else s
    )

    # Add period column based on atomic number
    def atomic_number_to_period(z: int) -> int:
        if z <= 2:
            return 1
        if z <= 10:
            return 2
        if z <= 18:
            return 3
        if z <= 36:
            return 4
        if z <= 54:
            return 5
        if z <= 86:
            return 6
        if z <= 118:
            return 7
        raise ValueError(f"Invalid atomic number: {z}")

    df["period"] = df["z"].apply(atomic_number_to_period)

    # Convert DataFrame to use appropriate nullable types
    df = df.convert_dtypes()

    # Reorder columns
    column_order = [
        "z",
        "symbol",
        "name",
        "period",
        "block",
        "e_config",
        "mass",
        "r",
        "ie",
        "ea",
        "en_pauling",
        "ox_states",
        "state",
        "mp",
        "bp",
        "density",
        "cpk_color",
        "year",
    ]
    df = df[column_order]

    filepath = (Path(data_dir) / filepath).with_suffix(".parquet")
    write_df(df, filepath)
    return {filepath: df}


def _camel_to_snake(camel_str):
    """Convert a 'camelCase' case string to 'snake_case'.

    This function inserts underscores before uppercase letters and lowercases them.
    """
    snake_str = re.sub(r'(?<!^)(?=[A-Z])', '_', camel_str).lower()
    return snake_str