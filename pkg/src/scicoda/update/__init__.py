from pathlib import Path

from scicoda.update import pdb, pubchem
from scicoda.data import _data_dir


def update_all(data_dir: Path | str | None = None):
    if data_dir is None:
        data_dir = _data_dir()
    pdb.update_all(data_dir=data_dir)
    pubchem.update_all(data_dir=data_dir)
    return
