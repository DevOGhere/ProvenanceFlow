import uuid


def generate_pid(prefix: str) -> str:
    """Generate a UUID-based persistent identifier with a semantic prefix."""
    return f"{prefix}_{uuid.uuid4().hex[:12]}"
