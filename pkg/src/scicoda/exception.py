"""Exceptions raised by the package."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pkgdata

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any


class ScicodaError(Exception):
    """Base class for all exceptions raised by the package."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message
        return


class ScicodaMissingDependencyError(ScicodaError):
    """Raised when a required dependency is missing."""

    def __init__(self, message_details: str):
        self.module = pkgdata.get_caller_module_name(stack_up=1)
        message = (
            f"Missing required dependency for module '{self.module}': "
            f"{message_details}"
        )
        super().__init__(message)
        return


class ScicodaInputError(ScicodaError):
    """Raised when an input argument is invalid."""

    def __init__(
        self,
        parameter: str,
        argument: Any,
        message_detail: str,
    ):
        self.parameter = parameter
        self.argument = argument
        self.function: str = pkgdata.get_caller_name(stack_up=1)
        message = (
            f"Invalid input argument '{argument}' for parameter '{parameter}' "
            f"of '{self.function}': {message_detail}"
        )
        super().__init__(message)
        return


class ScicodaFileNotFoundError(ScicodaError):
    """Raised when a requested data file is not found.

    Parameters
    ----------
    path_relative
        Path to the file relative to the package's data directory.
    path_absolute
        Absolute path to the file.
    """

    def __init__(
        self,
        category: str,
        name: str,
        filepath: Path,
    ):
        self.category = category
        self.name = name
        self.filepath = filepath
        message = (
            f"Could not find the requested package data file "
            f"'{name}' in category '{category}' at filepath '{filepath}'."
        )
        super().__init__(message)
        return
