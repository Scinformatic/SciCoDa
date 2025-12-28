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
_cache: dict[str, dict] = {}


@overload
def get_file(
    category: str,
    name: str,
    extension: Literal["json"] = "json",
    *,
    lazy: bool = False,
    cache: bool = True,
) -> dict | list: ...
@overload
def get_file(
    category: str,
    name: str,
    extension: Literal["parquet"] = "parquet",
    *,
    lazy: bool = False,
    cache: bool = True,
) -> pl.DataFrame: ...
@overload
def get_file(
    category: str,
    name: str,
    extension: Literal["parquet"] = "parquet",
    *,
    lazy: bool = True,
    cache: bool = True,
) -> pl.LazyFrame: ...
def get_file(
    category: str,
    name: str,
    extension: Literal["json", "parquet"],
    *,
    lazy: bool = False,
    cache: bool = True,
) -> dict | list | pl.DataFrame | pl.LazyFrame:
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
    cache
        Retain the data in memory after reading it
        for faster access in subsequent calls.
    lazy
        If `extension` is "parquet",
        return a `polars.LazyFrame` instead of a `polars.DataFrame`.

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
    if (cached := _cache.get(category, {}).get(name)) is not None:
        return cached
    filepath = get_filepath(category=category, name=name, extension=extension)
    if extension == "json":
        file = json.loads(filepath.read_text())
    elif extension == "parquet":
        file = pl.scan_parquet(filepath) if lazy else pl.read_parquet(filepath)
    else:
        raise exception.ScicodaInputError(
            parameter="extension",
            argument=extension,
            message_detail="Unsupported file extension."
        )
    if cache:
        _cache.setdefault(category, {})[name] = file
    return file


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
        Default is "yaml".

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
