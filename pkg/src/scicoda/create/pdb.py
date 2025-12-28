"""Fetch and process datasets from the Protein Data Bank (PDB)."""

import warnings

import ciffile
import dfhelp
import polars as pl
import pdbapi


def ccd() -> tuple[
    dict[str, pl.DataFrame],
    dict[str, pl.DataFrame],
    dict[str, dict]
]:
    """Download and process the Chemical Component Dictionary (CCD) from PDB.

    Download both the main CCD and the amino acid protonation variants companion dictionary,
    process them into individual tables,
    and separate the amino acid components from the non-amino acid components.

    Returns
    -------
    amino_acid_category_dfs
        A dictionary mapping CCD category names to their corresponding
        `polars.DataFrame` for amino acid components.
    non_amino_acid_category_dfs
        A dictionary mapping CCD category names to their corresponding
        `polars.DataFrame` for non-amino acid components.
    problems
        A dictionary containing any problems encountered during processing,
        such as validation errors, duplicate rows, or conflicting rows.
        It may have the keys
        - "main": problems encountered in the main CCD,
        - "protonation": problems encountered in the protonation variants CCD,
        - "merge": problems encountered during merging of the two CCDs,

        with each key mapping to another dictionary
        that maps category names to problem details
        for that category.
    """

    # Create a PDBx/mmCIF validator to validate and cast category data
    validator = ciffile.validator(
        ciffile.read(pdbapi.file.dictionary()).to_validator_dict()
    )

    category_dfs: dict[str, list[pl.DataFrame]] = {}
    amino_acid_comp_ids: set[str] = set()

    # Suffix for estimated standard deviation columns
    esd_cols_suffix = "_esd_digits"

    problems = {}

    for ccd_variant in ("main", "protonation"):
        ccd_bytes = pdbapi.file.ccd(variant=ccd_variant)
        ccd_file = ciffile.read(ccd_bytes)
        ccd_categories = ccd_file.category()
        for cat_name, cat in ccd_categories.items():
            id_col = "id" if cat_name == "chem_comp" else "comp_id"

            # Ensure that the block code matches the ID (case-insensitive) and remove the _block column
            if not (cat.df["_block"].str.to_lowercase() == cat.df[id_col].str.to_lowercase()).all():
                raise ValueError(
                    f"Mismatching block code and ID in category {cat_name} of CCD variant {ccd_variant}."
                )
            cat.df = cat.df.drop("_block")

            # Validate and cast the category data
            errors = validator.validate(cat, esd_col_suffix=esd_cols_suffix)
            n_errors = len(errors)
            if n_errors > 0:
                problems.setdefault(ccd_variant, {})[cat_name] = {"validation": errors}
                err_types = errors["type"].unique().to_list()
                warnings.warn(
                    f"Found {n_errors} validation errors in category '{cat_name}' of CCD variant '{ccd_variant}': {err_types}"
                )
            cat_df = cat.df

            # Remove esd columns (estimated standard deviations)
            esd_cols = [col for col in cat_df.columns if col.endswith(esd_cols_suffix)]
            cat_df = cat_df.drop(esd_cols)

            # For bonds, normalize atom ordering to ensure consistent representation
            # (e.g., bond between atom1-atom2 is same as atom2-atom1)
            if cat_name == "chem_comp_bond":
                # Ensure consistent ordering of atom IDs in each bond
                cat_df = cat_df.with_columns(
                    pl.min_horizontal("atom_id_1", "atom_id_2").alias("atom_id_1"),
                    pl.max_horizontal("atom_id_1", "atom_id_2").alias("atom_id_2"),
                )

            category_dfs.setdefault(cat_name, []).append(cat.df)
            if ccd_variant == "protonation" and cat_name == "chem_comp":
                amino_acid_comp_ids.update(cat.df[id_col].to_list())

    category_df_aa: dict[str, pl.DataFrame] = {}
    category_df_non_aa: dict[str, pl.DataFrame] = {}
    for cat_name, variant_dfs in category_dfs.items():
        # Determine ID columns for deduplication and merging
        id_cols = _CCD_CATEGORY_CHECK.get(cat_name, {}).get("id_cols")
        if not id_cols:
            # If not specified, use all common columns across variants
            if len(variant_dfs) == 1:
                id_cols = list(variant_dfs[0].columns)
            else:
                id_cols = list(set.intersection(*[set(df.columns) for df in variant_dfs]))

        # Deduplicate each variant DataFrame
        variant_dfs_dedup = []
        for variante_name, df in zip(("main", "protonation"), variant_dfs):
            df_dedup, dupes = dfhelp.deduplicate_by_cols(df, id_cols)
            n_dupes = len(dupes)
            if n_dupes > 0:
                problems.setdefault(variante_name, {}).setdefault(cat_name, {})["duplicates"] = dupes
                warnings.warn(
                    f"Found {n_dupes} duplicate rows in category '{cat_name}' "
                    f"of CCD variant '{variante_name}'; keeping first occurrence.",
                )
            variant_dfs_dedup.append(df_dedup)
        merged_df, conflicts = (
            (variant_dfs_dedup[0], [])
            if len(variant_dfs_dedup) == 1 else
            dfhelp.merge_rows(variant_dfs_dedup[0], variant_dfs_dedup[1], id_cols)
        )
        n_conflicts = len(conflicts)
        if n_conflicts > 0:
            problems.setdefault("merge", {})[cat_name] = conflicts
            warnings.warn(
                f"Found {n_conflicts} conflicting rows in category '{cat_name}' of CCD variants; "
                f"keeping first occurrence.",
            )
        id_col = "id" if cat_name == "chem_comp" else "comp_id"
        is_aa_variant = merged_df[id_col].is_in(amino_acid_comp_ids)
        category_df_aa[cat_name] = merged_df.filter(is_aa_variant)
        category_df_non_aa[cat_name] = merged_df.filter(~is_aa_variant)

    return category_df_aa, category_df_non_aa, problems


_CCD_CATEGORY_CHECK = {
    "chem_comp": {
        "id_cols": ["id"],
    },
    "chem_comp_atom": {
        "id_cols": ["comp_id", "atom_id"],
    },
    "chem_comp_bond": {
        "id_cols": ["comp_id", "atom_id_1", "atom_id_2"],
    },
    "pdbx_chem_comp_atom_related": {
        "id_cols": ["comp_id", "atom_id", "related_comp_id", "related_atom_id"],
    },
    "pdbx_chem_comp_pcm": {
        "id_cols": ["comp_id", "pcm_id"],
    },
    "pdbx_chem_comp_synonyms": {
        "id_cols": ["comp_id", "ordinal"],
    }
}
