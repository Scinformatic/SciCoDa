"""Fetch and process data from PubChem web services."""

import polars as pl


def periodic_table(
    *,
    url: str = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/periodictable/CSV"
) -> pl.DataFrame:
    """Download and process the periodic table data from PubChem.

    Parameters
    ----------
    url
        URL to fetch the periodic table CSV data from PubChem.

    Returns
    -------
    A polars DataFrame containing the processed periodic table data.

    References
    ----------
    - [IUPAC Cookbook](https://iupac.github.io/WFChemCookbook/datasources/pubchem_ptable.html)
    """

    schema = {
        "AtomicNumber": pl.UInt8,  # 1-118 fits in UInt8 (0-255)
        "Symbol": pl.String,
        "Name": pl.String,
        "AtomicMass": pl.Float32,  # Float32 has sufficient precision
        "CPKHexColor": pl.String,
        "ElectronConfiguration": pl.String,
        "Electronegativity": pl.Float32,
        "AtomicRadius": pl.UInt16,  # max 348 fits in UInt16 (0-65535)
        "IonizationEnergy": pl.Float32,
        "ElectronAffinity": pl.Float32,
        "OxidationStates": pl.String,
        "StandardState": pl.String,
        "MeltingPoint": pl.Float32,
        "BoilingPoint": pl.Float32,
        "Density": pl.Float32,
        "GroupBlock": pl.String,
        "YearDiscovered": pl.String,
    }
    df = pl.read_csv(url, schema=schema)


    # Clean and process columns
    # -------------------------

    # Lowercase 'Name'
    expr_name = pl.col("Name").str.to_lowercase()

    # 'ElectronConfiguration' notations for unstable/theoretical elements
    # have suffixes like ' (calculated)' and ' (predicted)';
    # remove these suffixes.
    expr_electron_config = (
        pl.col("ElectronConfiguration")
        .str.replace(r" \((calculated|predicted)\)$", "")
        .str.strip_chars()
    )

    # 'OxidationStates' are stored as comma-separated strings;
    # convert them to lists of integers (or None if missing).
    expr_ox_states = (
        pl.col("OxidationStates").str.split(",").cast(pl.List(pl.Int8))
    )

    # 'StandardState' for unstable/theoretical elements
    # have prefixes like 'Expected to be a '.
    # Future proofing for other wordings,
    # we look whether "solid", "liquid", or "gas" appears in the lowercased string,
    # and cast the column to an enum with those three values.
    col_state = pl.col("StandardState").str.to_lowercase()
    expr_standard_state = (
        pl.when(col_state.str.contains("solid")).then(pl.lit("solid"))
        .when(col_state.str.contains("liquid")).then(pl.lit("liquid"))
        .when(col_state.str.contains("gas")).then(pl.lit("gas"))
        .otherwise(pl.lit(None))
        .cast(pl.Enum)
        .alias("StandardState")
    )

    # 'GroupBlock' to lowercase and cast to enum
    expr_group_block = pl.col("GroupBlock").str.to_lowercase().cast(pl.Enum)

    # 'YearDiscovered' to Date, with missing values for 'Ancient' and unknown years
    expr_year = (
        pl.col("YearDiscovered")
        .str.strptime(pl.Date, format="%Y", strict=False)
    )


    # Calculate new columns
    # ---------------------

    # Add 'period' column based on atomic number
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

    expr_period = pl.col("z").map_elements(atomic_number_to_period, return_dtype=pl.UInt8).alias("period")






    # Convert column names from 'CamelCase' to 'snake_case'
    df = df.rename({col: _camel_to_snake(col) for col in df.columns}).rename(
        {
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
    df = df.select(column_order)

    return df
