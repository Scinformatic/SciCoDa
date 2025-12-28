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
    Polars DataFrame containing the processed periodic table data,
    with one row per chemical element,
    and the following columns:

    z : int
        Atomic number of the element.
    symbol : str
        Chemical symbol of the element (capitalized),
        e.g., 'H' for Hydrogen, 'He' for Helium.
    name : str
        Name of the element in lowercase,
        e.g., 'hydrogen', 'helium'.
    period : int
        Period number in the periodic table (1-7).
    group : int | None
        Group number in the periodic table (1-18),
        or `null` for lanthanides and actinides.

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
    expr_name = (
        pl.col("Name")
        .str.to_lowercase()
        .alias("Name")
    )

    # 'ElectronConfiguration' notations for unstable/theoretical elements
    # have suffixes like ' (calculated)' and ' (predicted)';
    # remove these suffixes.
    expr_electron_config = (
        pl.col("ElectronConfiguration")
        .str.replace(r" \((calculated|predicted)\)$", "")
        .str.strip_chars()
        .alias("ElectronConfiguration")
    )

    # 'OxidationStates' are stored as comma-separated strings;
    # convert them to lists of integers (or None if missing).
    # Strip whitespace and remove '+' prefix from each element.
    expr_ox_states = (
        pl.col("OxidationStates")
        .replace("", None)
        .str.split(",")
        .list.eval(pl.element().str.strip_chars().str.replace(r"^\+", ""))
        .cast(pl.List(pl.Int8))
        .alias("OxidationStates")
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
        .cast(pl.Categorical)
        .alias("StandardState")
    )

    # 'GroupBlock' to lowercase and cast to enum
    expr_group_block = (
        pl.col("GroupBlock")
        .str.to_lowercase()
        .cast(pl.Categorical)
        .alias("GroupBlock")
    )

    # 'YearDiscovered' convert to integer, with missing values for 'Ancient' and unknown years
    expr_year = (
        pl.col("YearDiscovered")
        .cast(pl.UInt16, strict=False)
        .alias("YearDiscovered")
    )


    # Calculate new columns
    # ---------------------

    # Add 'period' column based on atomic number (vectorized)
    z = pl.col("AtomicNumber")
    expr_period = (
        pl.when(z <= 2).then(1)
        .when(z <= 10).then(2)
        .when(z <= 18).then(3)
        .when(z <= 36).then(4)
        .when(z <= 54).then(5)
        .when(z <= 86).then(6)
        .when(z <= 118).then(7)
        .otherwise(None)
        .cast(pl.UInt8)
        .alias("period")
    )

    # Add 'group' column based on atomic number (IUPAC numbering 1-18)
    # Calculate position within period, then map to group
    period_start = (  # starting atomic number (minus 1) of each period
        pl.when(z <= 2).then(0)
        .when(z <= 10).then(2)
        .when(z <= 18).then(10)
        .when(z <= 36).then(18)
        .when(z <= 54).then(36)
        .when(z <= 86).then(54)
        .otherwise(86)
    )
    position_in_period = z - period_start

    expr_group = (
        pl
        .when(position_in_period == 1).then(1)  # Group 1 (alkali metals + H)
        .when(z == 2).then(18)  # He is group 18
        .when(position_in_period == 2).then(2)  # Group 2 (alkaline earth metals)
        .when((z <= 10) & (position_in_period >= 3)).then(position_in_period + 10)  # Period 2: groups 13-18
        .when((z <= 18) & (position_in_period >= 3)).then(position_in_period + 10)  # Period 3: groups 13-18
        .when((z >= 58) & (z <= 71)).then(None)  # Lanthanides
        .when((z >= 90) & (z <= 103)).then(None)  # Actinides
        # Periods 6-7: After lanthanides/actinides, subtract 14 to get correct group
        .when(z > 71).then(position_in_period - 14)  # Period 6: Hf onwards (subtract lanthanides)
        .when(z > 103).then(position_in_period - 14)  # Period 7: Rf onwards (subtract actinides)
        .otherwise(position_in_period)  # Periods 4-5: position matches group
        .cast(pl.UInt8)
        .alias("group")
    )

    # Apply all transformations
    # -------------------------
    df = df.with_columns([
        expr_name,
        expr_electron_config,
        expr_ox_states,
        expr_standard_state,
        expr_group_block,
        expr_year,
        expr_period,
        expr_group,
    ])

    # Convert column names from 'CamelCase' to 'snake_case'
    df = df.rename(
        {
            "AtomicNumber": "z",
            "Symbol": "symbol",
            "Name": "name",
            "AtomicMass": "mass",
            "CPKHexColor": "cpk_color",
            "ElectronConfiguration": "e_config",
            "Electronegativity": "en_pauling",
            "AtomicRadius": "r",
            "IonizationEnergy": "ie",
            "ElectronAffinity": "ea",
            "OxidationStates": "ox_states",
            "StandardState": "state",
            "MeltingPoint": "mp",
            "BoilingPoint": "bp",
            "Density": "density",
            "GroupBlock": "block",
            "YearDiscovered": "year",
        }
    )

    # Reorder columns
    column_order = [
        "z",
        "symbol",
        "name",
        "period",
        "group",
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
