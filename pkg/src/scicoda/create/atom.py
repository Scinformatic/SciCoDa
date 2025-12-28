"""Fetch and process atomic data."""

import polars as pl

from scicoda.data import get_data


def periodic_table(
    *,
    url: str = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/periodictable/CSV"
) -> pl.DataFrame:
    """Download and process the periodic table data from PubChem.

    Fetch the periodic table data from PubChem,
    clean the data and cast columns to appropriate types,
    add additional columns such as period and group numbers,
    add data from other sources (such as van der Waals radii from Blue Obelisk),
    and return the processed data as a Polars DataFrame.

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
        or `null` for lanthanides and actinides
        (La and Ac are considered part of group 3).
    block : str
        Group block of the element; one of:
        - 'actinide'
        - 'alkali metal'
        - 'alkaline earth metal'
        - 'halogen'
        - 'lanthanide'
        - 'metalloid'
        - 'noble gas'
        - 'nonmetal'
        - 'post-transition metal'
        - 'transition metal'
    econfig : str
        Electron configuration of the element in standard notation,
        e.g., '1s2', '[He]2s2 2p4'.
    mass : float
        Atomic mass of the element in unified atomic mass units (u).
    vdwr : int
        Atomic (van der Waals) radius of the element in picometers (pm).
    vdwr_bo : int
        Atomic (van der Waals) radius of the element in picometers (pm),
        from the [Blue Obelisk Data Repository](https://github.com/AAriam/bodr/blob/29ce17071c71b2d4d5ee81a2a28f0407331f1624/bodr/elements/elements.xml).
        For elements beyond atomic number 109 (Ds to Og),
        a default value of 2.00 Å is used to fill in missing data.
    ie : float
        Ionization energy of the element in electronvolts (eV).§§§§
    ea : float
        Electron affinity of the element in electronvolts (eV).
    en_pauling : float
        Electronegativity of the element on the Pauling scale.
    oxstates : list[int]
        Possible oxidation states of the element.
    state : str
        Standard state of the element at room temperature;
        one of "solid", "liquid", or "gas".
    mp : float
        Melting point of the element in kelvin (K).
    bp : float
        Boiling point of the element in kelvin (K).
    density : float
        Density of the element in grams per cubic centimeter (g/cm³).
    color_cpk : str
        CPK hex color code for the element, e.g., '#FF0000' for red.
    year : int
        Year the element was discovered,
        or `null` if the element was known since ancient times.

    Note that some elements, especially the synthetic ones,
    may have missing values for some properties.
    All missing values are represented as `null`.

    References
    ----------
    - [PubChem Periodic Table](https://pubchem.ncbi.nlm.nih.gov/periodic-table/)
    - [IUPAC Cookbook](https://iupac.github.io/WFChemCookbook/datasources/pubchem_ptable.html)
    """
    df = pl.read_csv(
        url,
        schema={
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
    )

    # Clean and process columns
    # -------------------------

    # Strip whitespace from all string columns and replace empty strings with None
    df = df.with_columns(
        pl.col(pl.String)
        .str.strip_chars()
        .replace("", None)
    )

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
        .cast(pl.Enum(["solid", "liquid", "gas"]))
        .alias("StandardState")
    )

    # 'GroupBlock' to lowercase and cast to enum
    expr_group_block = (
        pl.col("GroupBlock")
        .str.to_lowercase()
        .cast(
            pl.Enum([
                'actinide',
                'alkali metal',
                'alkaline earth metal',
                'halogen',
                'lanthanide',
                'metalloid',
                'noble gas',
                'nonmetal',
                'post-transition metal',
                'transition metal'
            ])
        )
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
        .when(z == 2).then(18)  # He is group 18
        .when((z >= 58) & (z <= 71)).then(None)  # Lanthanides
        .when((z >= 90) & (z <= 103)).then(None)  # Actinides
        .when(position_in_period == 1).then(1)  # Group 1 (alkali metals + H)
        .when(position_in_period == 2).then(2)  # Group 2 (alkaline earth metals)
        .when((z <= 10) & (position_in_period >= 3)).then(position_in_period + 10)  # Period 2: groups 13-18
        .when((z <= 18) & (position_in_period >= 3)).then(position_in_period + 10)  # Period 3: groups 13-18
        # For periods 6-7, elements after lanthanides/actinides need adjustment
        # Position includes the f-block (14 elements), so subtract 14
        .when((z >= 72) & (z <= 86)).then(position_in_period - 14)  # Period 6: Hf-Rn
        .when(z >= 104).then(position_in_period - 14)  # Period 7: Rf onwards
        .otherwise(position_in_period)  # Periods 4-5: position matches group
        .cast(pl.UInt8)
        .alias("group")
    )

    # Add van der Waals radius column from Blue Obelisk data
    vdwr = get_data("atom", "radii_vdw_blue_obelisk", cache=False)
    expr_vdwr_bo = pl.Series(vdwr).alias("vdwr_bo")


    # Apply all transformations
    # -------------------------
    df = df.sort("z").with_columns([
        expr_name,
        expr_electron_config,
        expr_ox_states,
        expr_standard_state,
        expr_group_block,
        expr_year,
        expr_period,
        expr_group,
        expr_vdwr_bo,
    ])

    # Rename columns to desired names
    df = df.rename(
        {
            "AtomicNumber": "z",
            "Symbol": "symbol",
            "Name": "name",
            "AtomicMass": "mass",
            "CPKHexColor": "color_cpk",
            "ElectronConfiguration": "econfig",
            "Electronegativity": "en_pauling",
            "AtomicRadius": "vdwr",
            "IonizationEnergy": "ie",
            "ElectronAffinity": "ea",
            "OxidationStates": "oxstates",
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
        "econfig",
        "mass",
        "vdwr",
        "vdwr_bo",
        "ie",
        "ea",
        "en_pauling",
        "oxstates",
        "state",
        "mp",
        "bp",
        "density",
        "color_cpk",
        "year",
    ]
    df = df.select(column_order)

    return df
