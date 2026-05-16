import uuid


def generate_uid() -> str:
    """Return a raw 12-char UUID hex fragment for use in PROV entity/activity IDs."""
    return uuid.uuid4().hex[:12]


def generate_pid(prefix: str) -> str:
    """Generate a UUID-based persistent identifier with a semantic prefix.

    Use this for fair:identifier attributes in PROV entities.
    Do NOT embed this directly in PROV entity ID keys — the prefix would be
    doubled (e.g. pf:dataset_dataset_abc123). Use generate_uid() for keys.
    """
    return f"{prefix}_{generate_uid()}"
