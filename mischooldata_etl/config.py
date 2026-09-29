"""Access to the project's config.toml."""
from pathlib import Path
import tomli


def get_config():
    base_dir = Path(__file__).parent
    with open(base_dir / "config.toml", "rb") as f:
        return tomli.load(f)
