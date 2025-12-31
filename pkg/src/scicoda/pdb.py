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
        Name of the CCD table (mmCIF data category) to retrieve; one of:
        - '[chem_comp](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/chem_comp.html)':
            Data items in the CHEM_COMP category give details about each of the chemical components
            from which the relevant chemical structures can be constructed, such as name, mass or charge.
            The related categories CHEM_COMP_ATOM, CHEM_COMP_BOND, CHEM_COMP_ANGLE etc.
            describe the detailed geometry of these chemical components.
        - '[chem_comp_atom](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/chem_comp_atom.html)':
            Data items in the CHEM_COMP_ATOM category record details about the atoms in a chemical component.
            Specifying the atomic coordinates for the components in this category is an alternative
            to specifying the structure of the component via bonds, angles, planes etc. in the appropriate CHEM_COMP subcategories.
        - '[chem_comp_bond](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/chem_comp_bond.html)':
            Data items in the CHEM_COMP_BOND category record details about the bonds between atoms in a chemical component.
            Target values may be specified as bond orders, as a distance between the two atoms, or both.
        - '[pdbx_chem_comp_atom_related](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/pdbx_chem_comp_atom_related.html)':
            PDBX_CHEM_COMP_ATOM_RELATED provides atom level nomenclature mapping between two related chemical components.
        - '[pdbx_chem_comp_audit](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/pdbx_chem_comp_audit.html)':
            Data items in the PDBX_CHEM_COMP_AUDIT category records the status and tracking information for this component.
        - '[pdbx_chem_comp_descriptor](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/pdbx_chem_comp_descriptor.html)':
            Data items in the CHEM_COMP_DESCRIPTOR category provide string descriptors of component chemical structure.
        - '[pdbx_chem_comp_feature](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/pdbx_chem_comp_feature.html)':
            Additional features associated with the chemical component.
        - '[pdbx_chem_comp_identifier](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/pdbx_chem_comp_identifier.html)':
            Data items in the CHEM_COMP_IDENTIFIER category provide identifiers for chemical components.
        - '[pdbx_chem_comp_pcm](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/pdbx_chem_comp_pcm.html)':
            Data items in the PDBX_CHEM_COMP_PCM category provide information about the protein modifications that are described by the chemical component.
        - '[pdbx_chem_comp_related](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/pdbx_chem_comp_related.html)':
            PDBX_CHEM_COMP_RELATED describes the relationship between two chemical components.
        - '[pdbx_chem_comp_synonyms](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/pdbx_chem_comp_synonyms.html)':
            PDBX_CHEM_COMP_SYNONYMS holds chemical name and synonym correspondences.
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
    ccd_category_df
        Polars DataFrame containing the requested CCD table data,
        optionally filtered by the specified component ID(s).
        For each category, the DataFrame columns are given below.
        The column names correspond to the data item keywords in the mmCIF dictionary,
        e.g., the data item '_chem_comp.id' corresponds to the column 'id' of the 'chem_comp' category DataFrame.
        For each column, the original PDBx/mmCIF data type is indicated in parentheses,
        and the Polars data type is indicated after the colon.
        You can click on the data item names to see their definitions
        in the official mmCIF dictionary documentation website.

        [**chem_comp**](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/_pdbx_chem_comp_synonyms.html):
        - '[id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.id.html)' (ucode): String
        - '[formula](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.formula.html)' (text): String
        - '[formula_weight](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.formula_weight.html)' (float): Float64
        - '[mon_nstd_parent_comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.mon_nstd_parent_comp_id.html)' (uline): String
        - '[name](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.name.html)' (text): String
        - '[one_letter_code](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.one_letter_code.html)' (ucode): String
        - '[pdbx_ambiguous_flag](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_ambiguous_flag.html)' (code): String
        - '[pdbx_formal_charge](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_formal_charge.html)' (int): Int64
        - '[pdbx_ideal_coordinates_details](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_ideal_coordinates_details.html)' (text): String
        - '[pdbx_ideal_coordinates_missing_flag](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_ideal_coordinates_missing_flag.html)' (ucode): Boolean
        - '[pdbx_initial_date](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_initial_date.html)' (yyyy-mm-dd): Date
        - '[pdbx_model_coordinates_db_code](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_model_coordinates_db_code.html)' (line): String
        - '[pdbx_model_coordinates_details](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_model_coordinates_details.html)' (text): String
        - '[pdbx_model_coordinates_missing_flag](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_model_coordinates_missing_flag.html)' (ucode): Boolean
        - '[pdbx_modified_date](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_modified_date.html)' (yyyy-mm-dd): Date
        - '[pdbx_pcm](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_pcm.html)' (code): Boolean
        - '[pdbx_processing_site](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_processing_site.html)' (code): Enum(categories=['PDBE', 'EBI', 'PDBJ', 'PDBC', 'RCSB', ''])
        - '[pdbx_release_status](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_release_status.html)' (line): Enum(categories=['REL', 'HOLD', 'HPUB', 'OBS', 'DEL', 'REF_ONLY', ''])
        - '[pdbx_replaced_by](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_replaced_by.html)' (ucode): String
        - '[pdbx_replaces](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_replaces.html)' (uline): String
        - '[pdbx_subcomponent_list](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_subcomponent_list.html)' (text): String
        - '[pdbx_synonyms](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_synonyms.html)' (text): String
        - '[pdbx_type](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.pdbx_type.html)' (uline): String
        - '[three_letter_code](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.three_letter_code.html)' (uchar5): String
        - '[type](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp.type.html)' (uline): Enum(categories=['d-peptide linking', 'l-peptide linking', 'd-peptide nh3 amino terminus', 'l-peptide nh3 amino terminus', 'd-peptide cooh carboxy terminus', 'l-peptide cooh carboxy terminus', 'dna linking', 'rna linking', 'l-rna linking', 'l-dna linking', 'dna oh 5 prime terminus', 'rna oh 5 prime terminus', 'dna oh 3 prime terminus', 'rna oh 3 prime terminus', 'd-saccharide, beta linking', 'd-saccharide, alpha linking', 'l-saccharide, beta linking', 'l-saccharide, alpha linking', 'l-saccharide', 'd-saccharide', 'saccharide', 'non-polymer', 'peptide linking', 'peptide-like', 'l-gamma-peptide, c-delta linking', 'd-gamma-peptide, c-delta linking', 'l-beta-peptide, c-gamma linking', 'd-beta-peptide, c-gamma linking', 'other', ''])

        [**chem_comp_atom**](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/_chem_comp.html):
        - '[comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.comp_id.html)' (ucode): String
        - '[atom_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.atom_id.html)' (atcode): String
        - '[alt_atom_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.alt_atom_id.html)' (line): String
        - '[charge](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.charge.html)' (int): Int64
        - '[model_cartn_x](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.model_Cartn_x.html)' (float): Float64
        - '[model_cartn_y](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.model_Cartn_y.html)' (float): Float64
        - '[model_cartn_z](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.model_Cartn_z.html)' (float): Float64
        - '[pdbx_align](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_align.html)' (int): Int64
        - '[pdbx_aromatic_flag](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_aromatic_flag.html)' (ucode): Boolean
        - '[pdbx_backbone_atom_flag](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_backbone_atom_flag.html)' (ucode): Boolean
        - '[pdbx_c_terminal_atom_flag](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_c_terminal_atom_flag.html)' (ucode): Boolean
        - '[pdbx_component_atom_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_component_atom_id.html)' (atcode): String
        - '[pdbx_component_comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_component_comp_id.html)' (ucode): String
        - '[pdbx_component_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_component_id.html)' (int): Int64
        - '[pdbx_leaving_atom_flag](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_leaving_atom_flag.html)' (ucode): Boolean
        - '[pdbx_model_cartn_x_ideal](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_model_Cartn_x_ideal.html)' (float): Float64
        - '[pdbx_model_cartn_y_ideal](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_model_Cartn_y_ideal.html)' (float): Float64
        - '[pdbx_model_cartn_z_ideal](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_model_Cartn_z_ideal.html)' (float): Float64
        - '[pdbx_n_terminal_atom_flag](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_n_terminal_atom_flag.html)' (ucode): Boolean
        - '[pdbx_ordinal](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_ordinal.html)' (int): Int64
        - '[pdbx_polymer_type](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_polymer_type.html)' (line): Enum(categories=['polymer', 'non-polymer', ''])
        - '[pdbx_residue_numbering](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_residue_numbering.html)' (int): Int64
        - '[pdbx_stereo_config](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.pdbx_stereo_config.html)' (ucode): Enum(categories=['r', 's', 'n', ''])
        - '[type_symbol](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_atom.type_symbol.html)' (code): String

        [**chem_comp_bond**](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/_chem_comp_atom.html):
        - '[comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_bond.comp_id.html)' (ucode): String
        - '[atom_id_1](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_bond.atom_id_1.html)' (atcode): String
        - '[atom_id_2](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_bond.atom_id_2.html)' (atcode): String
        - '[pdbx_aromatic_flag](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_bond.pdbx_aromatic_flag.html)' (ucode): Boolean
        - '[pdbx_ordinal](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_bond.pdbx_ordinal.html)' (int): Int64
        - '[pdbx_stereo_config](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_bond.pdbx_stereo_config.html)' (ucode): Enum(categories=['e', 'z', 'n', ''])
        - '[value_order](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_chem_comp_bond.value_order.html)' (ucode): Enum(categories=['sing', 'doub', 'trip', 'quad', 'arom', 'poly', 'delo', 'pi', ''])

        [**pdbx_chem_comp_atom_related**](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/_chem_comp_bond.html):
        - '[comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_atom_related.comp_id.html)' (ucode): String
        - '[ordinal](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_atom_related.ordinal.html)' (int): Int64
        - '[related_comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_atom_related.related_comp_id.html)' (ucode): String
        - '[atom_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_atom_related.atom_id.html)' (atcode): String
        - '[related_atom_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_atom_related.related_atom_id.html)' (atcode): String
        - '[related_type](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_atom_related.related_type.html)' (line): Enum(categories=['Carbohydrate core', 'Precursor', ''])

        [**pdbx_chem_comp_audit**](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/_pdbx_chem_comp_atom_related.html):
        - '[comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_audit.comp_id.html)' (ucode): String
        - '[date](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_audit.date.html)' (yyyy-mm-dd): Date
        - '[action_type](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_audit.action_type.html)' (line): Enum(categories=['Create component', 'Modify name', 'Modify formula', 'Modify synonyms', 'Modify linking type', 'Modify internal type', 'Modify metal description', 'Modify parent residue', 'Modify processing site', 'Modify subcomponent list', 'Modify one letter code', 'Modify model coordinates code', 'Modify formal charge', 'Modify atom id', 'Modify charge', 'Modify aromatic_flag', 'Modify leaving atom flag', 'Modify component atom id', 'Modify component comp_id', 'Modify value order', 'Modify descriptor', 'Modify identifier', 'Modify coordinates', 'Modify backbone', 'Modify PCM', 'Other modification', 'Obsolete component', 'Initial release', ''])
        - '[processing_site](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_audit.processing_site.html)' (code): String

        [**pdbx_chem_comp_descriptor**](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/_pdbx_chem_comp_audit.html):
        - '[comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_descriptor.comp_id.html)' (ucode): String
        - '[type](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_descriptor.type.html)' (uline): Enum(categories=['smiles_cannonical', 'smiles_canonical', 'smiles', 'inchi', 'inchi_main', 'inchi_main_formula', 'inchi_main_connect', 'inchi_main_hatom', 'inchi_charge', 'inchi_stereo', 'inchi_isotope', 'inchi_fixedh', 'inchi_reconnect', 'inchikey', ''])
        - '[program](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_descriptor.program.html)' (line): String
        - '[program_version](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_descriptor.program_version.html)' (line): String
        - '[descriptor](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_descriptor.descriptor.html)' (text): String

        [**pdbx_chem_comp_feature**](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/_pdbx_chem_comp_descriptor.html):
        - '[comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_feature.comp_id.html)' (ucode): String
        - '[type](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_feature.type.html)' (line): Enum(categories=['CARBOHYDRATE ANOMER', 'CARBOHYDRATE ISOMER', 'CARBOHYDRATE RING', 'CARBOHYDRATE PRIMARY CARBONYL GROUP', ''])
        - '[value](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_feature.value.html)' (text): String
        - '[source](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_feature.source.html)' (line): String
        - '[support](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_feature.support.html)' (text): String

        [**pdbx_chem_comp_identifier**](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/_pdbx_chem_comp_feature.html):
        - '[comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_identifier.comp_id.html)' (ucode): String
        - '[type](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_identifier.type.html)' (line): Enum(categories=['COMMON NAME', 'SYSTEMATIC NAME', 'CAS REGISTRY NUMBER', 'PUBCHEM Identifier', 'MDL Identifier', 'SYNONYM', 'CONDENSED IUPAC CARB SYMBOL', 'IUPAC CARB SYMBOL', 'SNFG CARB SYMBOL', 'CONDENSED IUPAC CARBOHYDRATE SYMBOL', 'IUPAC CARBOHYDRATE SYMBOL', 'SNFG CARBOHYDRATE SYMBOL', ''])
        - '[program](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_identifier.program.html)' (line): String
        - '[program_version](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_identifier.program_version.html)' (line): String
        - '[identifier](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_identifier.identifier.html)' (text): String

        [**pdbx_chem_comp_pcm**](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/_pdbx_chem_comp_identifier.html):
        - '[pcm_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_pcm.pcm_id.html)' (int): Int64
        - '[category](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_pcm.category.html)' (line): Enum(categories=['ADP-ribose', 'Amino acid', 'Biotin', 'Carbohydrate', 'Chromophore/chromophore-like', 'Covalent chemical modification', 'Crosslinker', 'Flavin', 'Heme/heme-like', 'Lipid/lipid-like', 'Named protein modification', 'Non-standard residue', 'Nucleotide monophosphate', 'Terminal acetylation', 'Terminal amidation', 'Metal coordination', ''])
        - '[comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_pcm.comp_id.html)' (ucode): String
        - '[comp_id_linking_atom](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_pcm.comp_id_linking_atom.html)' (atcode): String
        - '[modified_residue_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_pcm.modified_residue_id.html)' (uline): String
        - '[modified_residue_id_linking_atom](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_pcm.modified_residue_id_linking_atom.html)' (atcode): String
        - '[polypeptide_position](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_pcm.polypeptide_position.html)' (line): Enum(categories=['C-terminal', 'N-terminal', 'Any position', ''])
        - '[position](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_pcm.position.html)' (line): Enum(categories=['Amino-acid side chain', 'Amino-acid backbone', 'Amino-acid side chain and backbone', ''])
        - '[type](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_pcm.type.html)' (line): Enum(categories=['12-Hydroxyfarnesylation', '12-Oxomyristoylation', '12R-Hydroxymyristoylation', '14-Hydroxy-10,13-dioxo-7-heptadecenoic acid', "(3-Aminopropyl)(5'-adenosyl)phosphono amidation", '2-Aminoadipylation', '2-Aminoethylphosphorylation', '2-Cholinephosphorylation', '2-Hydroxyisobutyrylation', '2-Oxo-5,5-dimethylhexanoylation', '2-Oxobutanoic acid', '2,3-Dicarboxypropylation', '3-Oxoalanine', '3-Phenyllactic acid', '(3R)-3-Hydroxybutyrylation', '4-Phosphopantetheine', 'ADP-ribosylation', 'ADP-riboxanation', 'AMPylation', 'Acetamidation', 'Acetamidomethylation', 'Acetylation', 'Alanylation', 'Allysine', 'Amination', 'Arachidoylation', 'Archaeol', 'Arginylation', 'Arsenylation', 'Asparaginylation', 'Aspartylation', 'Bacillithiolation', 'Benzoylation', 'Benzylation', 'Beta-amino acid', 'Beta-hydroxybutyrylation', 'Beta-lysylation', 'Beta-mercaptoethanol', 'Biotinylation', 'Bromination', 'Butyrylation', 'Carbamoylation', 'Carboxyethylation', 'Carboxylation', 'Carboxymethylation', 'cGMPylation', 'Chlorination', 'Cholesterylation', 'Citrullination', 'Crotonylation', 'Cyanation', 'Cysteinylation', 'C-Mannosylation', 'Deamidation', 'Decanoylation', 'Decarboxylation', 'Dehydroamino acid', 'Dehydrocoelenterazination', 'Dehydrogenation', 'Dehydroxylation', 'Deoxidation', 'Deoxyhypusine', 'Diacylglycerol', 'Dihydroxyacetonation', 'Dimethylamination', 'Diphosphorylation', 'Diphthamide', 'Dipyrromethane methylation', 'Dopaminylation', 'D-alanylation', 'D-arginylation', 'D-asparaginylation', 'D-aspartylation', 'D-cysteinylation', 'D-glutaminylation', 'D-glutamylation', 'D-histidinylation', 'D-isoleucylation', 'D-lactate', 'D-leucylation', 'D-lysylation', 'D-methionylation', 'D-phenylalanylation', 'D-prolylation', 'D-serylation', 'D-threoninylation', 'D-tryptophanylation', 'D-tyrosination', 'D-valylation', 'Ethylation', 'Ethylsulfanylation', 'Farnesylation', 'Fluorination', 'Formylation', 'GMPylation', 'Geranylgeranylation', 'Glutaminylation', 'Glutamylation', 'Glutarylation', 'Glutathionylation', 'Glycerophosphorylation', 'Glycerylphosphorylethanolamination', 'Glycylation', 'Heptanoylation', 'Hexanoylation', 'Histaminylation', 'Histidinylation', 'Hydrogenation', 'Hydroperoxylation', 'Hydroxyamination', 'Hydroxyethylation', 'Hydroxylation', 'Hydroxymethylation', 'Hydroxysulfanylation', 'Hypusine', 'Iodination', 'Isoleucylation', 'Lactoylation', 'Laurylation', 'Leucylation', 'Lipoylation', 'L-lactate', 'Lysylation', 'Malonylation', 'Methacrylation', 'Methionylation', 'Methoxylation', 'Methylamination', 'Methylation', 'Methylsulfanylation', 'Methylsulfation', 'Myristoylation', 'N6-benzoyllysine', 'N6-isonicotinyllysine', 'N6-methacryllysine', 'N-pyruvic acid 2-iminylation', 'N-methylcarbamoylation', 'Nitration', 'Nitrosylation', 'None', 'Noradrenylation', 'Norleucine', 'Norvaline', 'N-Glycosylation', 'Octanoylation', 'Oleoylation', 'Ornithine', 'Oxidation', 'O-Glycosylation', 'Palmitoleoylation', 'Palmitoylation', 'Pentadecanoylation', 'Pentanoylation', 'Phenylalanylation', 'Phosphatidylethanolamine amidation', 'Phosphoenolpyruvate', 'Phosphorylation', 'Prolylation', 'Propionylation', 'Pyridoxal phosphate', 'Pyrrolidone carboxylic acid', 'Pyruvic acid', 'Retinoylation', 'Selanylation', 'Selenomethionine', 'Serotonylation', 'Serylation', 'Stearoylation', 'Stereoisomerisation', 'Succinamide ring', 'Succination', 'Succinylation', 'Sulfanylmethylation', 'Sulfation', 'Sulfhydration', 'S-Glycosylation', 'Tert-butylation', 'Tert-butyloxycarbonylation', 'Threoninylation', 'Thyroxine', 'Triiodothyronine', 'Triphosphorylation', 'Tryptophanylation', 'Tyrosination', 'UMPylation', 'Valylation', ''])
        - '[uniprot_generic_ptm_accession](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_pcm.uniprot_generic_ptm_accession.html)' (uniprot_ptm_id): String
        - '[uniprot_specific_ptm_accession](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_pcm.uniprot_specific_ptm_accession.html)' (uniprot_ptm_id): String

        [**pdbx_chem_comp_related**](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/_pdbx_chem_comp_pcm.html):
        - '[comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_related.comp_id.html)' (ucode): String
        - '[related_comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_related.related_comp_id.html)' (ucode): String
        - '[relationship_type](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_related.relationship_type.html)' (line): Enum(categories=['Carbohydrate core', 'Precursor', ''])
        - '[details](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_related.details.html)' (text): String

        [**pdbx_chem_comp_synonyms**](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Categories/_pdbx_chem_comp_related.html):
        - '[comp_id](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_synonyms.comp_id.html)' (ucode): String
        - '[ordinal](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_synonyms.ordinal.html)' (int): Int64
        - '[name](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_synonyms.name.html)' (text): String
        - '[provenance](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_synonyms.provenance.html)' (line): Enum(categories=['AUTHOR', 'DRUGBANK', 'CHEBI', 'CHEMBL', 'PDB', 'PUBCHEM', ''])
        - '[type](https://mmcif.wwpdb.org/dictionaries/mmcif_pdbx_v50.dic/Items/_pdbx_chem_comp_synonyms.type.html)' (line): String
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
