"""Atomic datasets."""

import polars as pl

from scicoda import data


_FILE_CATEGORY_NAME = "atom"


def autodock_atom_types() -> pl.DataFrame:
    """Get AutoDock4 atom types and their properties.

    These are used in the AutoDock4 software (e.g. AutoGrid4)
    and file formats (e.g. PDBQT, GPF).

    Returns
    -------
    Polars DataFrame containing one row per AutoDock4 atom type,
    with the following columns:

    type : str
        AutoDock4 atom type name (e.g. "A", "C", "HD", "OA", etc.)
    element : str
        Chemical element symbol (e.g. "C", "H", "Cl", etc.) of the atom type.
    description : str
        Short description of the atom type, if available.
    hbond_acceptor : bool
        Whether the atom type is a hydrogen bond acceptor.
    hbond_donor : bool
        Whether the atom type is a hydrogen bond donor.
    hbond_count : int
        Number of possible hydrogen bonds for directionally H-bonding atoms,
        0 for non H-bonding atoms,
        and `null` for spherically H-bonding atoms.

    Notes
    -----
    Only one of the columns `hbond_acceptor` or `hbond_donor`
    can be True for each atom type.
    If both are False, `hbond_count` is 0.
    """
    df = pl.DataFrame(
        data.get_file(
            category=_FILE_CATEGORY_NAME,
            name="autodock_atom_types",
            extension="json",
        ),
        schema={
            "type": pl.Utf8,
            "element": pl.Utf8,
            "description": pl.Utf8,
            "hbond_acceptor": pl.Boolean,
            "hbond_donor": pl.Boolean,
            "hbond_count": pl.UInt8,
        }
    )
    return df


def periodic_table() -> pl.DataFrame:
    """Get periodic table of chemical elements.

    All data is sourced from PubChem,
    unless otherwise noted.

    Returns
    -------
    Polars DataFrame containing one row per chemical element,
    with the following columns:

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
    group : int
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
    - [Blue Obelisk Data Repository](https://github.com/AAriam/bodr/blob/29ce17071c71b2d4d5ee81a2a28f0407331f1624/bodr/elements/elements.xml)

    See Also
    --------
    - [Mendeleev Python Package](https://mendeleev.readthedocs.io)
    """
    return data.get_file(
        category=_FILE_CATEGORY_NAME,
        name="periodic_table",
        extension="parquet",
    )
