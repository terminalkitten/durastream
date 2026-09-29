import re

# Lowercase only, so two names can never map to one file on a case-insensitive
# file system (macOS default).
_NAME_RE = re.compile(r"[a-z0-9._-]+")

DEFAULT_CONTENT_TYPE = "application/octet-stream"


def check_name(name: str) -> str:
    """Return `name` if it's a valid new stream name."""
    if not _NAME_RE.fullmatch(name):
        raise ValueError(
            f"invalid stream name {name!r}: use lowercase letters, digits, '.', '_' or '-'"
        )
    return name
