from __future__ import annotations

import json
from typing import TYPE_CHECKING, overload

import pkgdata
import polars as pl

from scicoda import exception

if TYPE_CHECKING:
    from typing import Literal
    from pathlib import Path


_data_dir = pkgdata.get_package_path_from_caller(top_level=True) / "data"


@overload
def get_file(
    category: str,
    name: str,
    extension: Literal["json"],
    filterby: None = None,
) -> dict | list: ...
@overload
def get_file(
    category: str,
    name: str,
    extension: Literal["parquet"],
    filterby: pl.Expr | None = None,
) -> pl.DataFrame: ...
def get_file(
    category: str,
    name: str,
    extension: Literal["json", "parquet"],
    filterby: pl.Expr | None = None,
) -> dict | list | pl.DataFrame:
    """Get a data file from the package data.

    Parameters
    ----------
    category
        Category of the data file.
        This corresponds to the module name where the data can be accessed.
    name
        Name of the data file.
        This corresponds to the function name that returns the data.
    extension
        File extension of the data file.
    filterby
        A Polars expression to filter the Parquet data on load.
        Only applicable when `extension` is "parquet".

    Returns
    -------
    The data file content.

    Raises
    -------
    ScicodaFileNotFoundError
        If the specified data file does not exist.
    ScicodaInputError
        If an unsupported file extension is specified.
    """
    filepath = get_filepath(category=category, name=name, extension=extension)
    if extension == "json":
        return json.loads(filepath.read_text())
    elif extension == "parquet":
        if filterby is None:
            return pl.read_parquet(filepath)
        return pl.scan_parquet(filepath).filter(filterby).collect()
    raise exception.ScicodaInputError(
        parameter="extension",
        argument=extension,
        message_detail="Unsupported file extension."
    )


def get_filepath(category: str, name: str, extension: Literal["json", "parquet"]) -> Path:
    """Get the absolute path to a data file.

    Parameters
    ----------
    category
        Category of the data file.
        This corresponds to the module name where the data can be accessed.
    name
        Name of the data file.
        This corresponds to the function name that returns the data.
    extension
        File extension of the data file.
        Must be either "json" or "parquet".

    Returns
    -------
    Absolute path to the data file.

    Raises
    -------
    ScicodaFileNotFoundError
        If the specified data file does not exist.
    """
    filepath = _data_dir / category / f"{name}.{extension}"
    if not filepath.is_file():
        raise exception.ScicodaFileNotFoundError(
            category=category,
            name=name,
            filepath=filepath,
        )
    return filepath
