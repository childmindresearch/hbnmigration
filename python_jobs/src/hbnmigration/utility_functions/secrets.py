"""Utilities for secret management."""

import importlib
import inspect
from types import FrameType
from typing import Any, Optional, TypeVar

T = TypeVar("T")


class ImportWithFallback:
    """Import with fallback."""

    @staticmethod
    def _get_out_of_secrets() -> Optional[FrameType]:
        """Get caller module outside of this script."""
        frame = inspect.currentframe()
        if frame:
            while frame and "utility_functions.secrets" in frame.f_globals.get(
                "__name__", ""
            ):
                frame = frame.f_back
        return frame

    @classmethod
    def _get_caller___name__(cls) -> Optional[str]:
        """Get the `__name__` of the caller's module."""
        frame = cls._get_out_of_secrets()
        try:
            return frame.f_globals.get("__name__") if frame else None
        finally:
            # Clean up the frame to avoid reference cycles
            del frame

    @classmethod
    def _import_any(cls, module: str, name: str, caller_name: Optional[str]) -> Any:
        """Import `name` from `module`."""
        return getattr(
            importlib.import_module(
                module,
                package=caller_name if module.startswith(".") else None,
            ),
            name,
        )

    @classmethod
    def module(
        cls,
        module: str,
        name: str,
        fallback_module: str,
        fallback_name: Optional[str] = None,
    ) -> Any:
        """Import with fallback module."""
        caller_name = cls._get_caller___name__()
        try:
            return cls._import_any(module, name, caller_name)
        except (ImportError, ModuleNotFoundError, AttributeError):
            return cls._import_any(
                fallback_module, fallback_name if fallback_name else name, caller_name
            )

    @classmethod
    def literal(cls, module: str, name: str, fallback: T) -> Any | T:
        """Import with literal fallback."""
        caller_name = cls._get_caller___name__()
        try:
            return cls._import_any(module, name, caller_name)
        except (ImportError, ModuleNotFoundError, AttributeError):
            return fallback
