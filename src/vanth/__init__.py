__all__ = ["__version__"]


def _package_version() -> str:
    try:
        from importlib.metadata import PackageNotFoundError, version

        return version("vanth")
    except PackageNotFoundError:  # source checkout, not installed
        return "0.0.0"


__version__ = _package_version()
