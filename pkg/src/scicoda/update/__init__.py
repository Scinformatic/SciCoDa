"""Update datasets in scicoda data package."""

from pathlib import Path
from typing import Any

from scicoda.data import _data_dir


def update_all(data_dir: Path | str | None = None) -> dict[str, Any]:
    """Update all datasets in the scicoda data package.

    Parameters
    ----------
    data_dir
        Data directory of the package.
        This must be left as `None`,
        unless you want to generate data files
        in a custom location, without affecting the package data.
    """
    from . import atom, pdb

    if data_dir is None:
        data_dir = _data_dir
    return {
        "atom": atom.update_all(data_dir=data_dir),
        "pdb": pdb.update_all(data_dir=data_dir),
    }
