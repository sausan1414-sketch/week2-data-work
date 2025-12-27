@dataclass(frozen=True)
class Paths:
"""Data class to hold all project paths."""
root: Path
raw: Path
cache: Path
processed: Path
external: Path