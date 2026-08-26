import re

_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")  # filesystem-safe stream names

DEFAULT_CONTENT_TYPE = "application/octet-stream"


def check_name(name: str) -> str:
    """Return `name` if it's a valid stream name."""
    if not _NAME_RE.match(name):
        raise ValueError(f"invalid stream name: {name!r}")
    return name
