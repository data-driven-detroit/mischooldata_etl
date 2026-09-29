"""A registry of the tables this project produces.

Each entry is an asset: one table in the `education` schema, and the single
zero-argument callable that (re)builds it. How it gets built -- transform to a
scratch file, then load -- is the asset's business, not the orchestrator's.

An orchestrator can walk `ASSETS` instead of importing each module by hand.
`deps` name other assets whose tables must exist first. Entries are declared in
dependency order, so iterating them in order is always safe.
"""
from dataclasses import dataclass, field
from typing import Callable

from .attendance.transform import transform_attendance
from .attendance.load import load_attendance
from .college_destination.transform import transform_college_destination
from .college_destination.load import load_college_destination
from .college_enrollment.transform import transform_college_enrollment
from .college_enrollment.load import load_college_enrollment
from .college_readiness.transform import transform_college_readiness
from .college_readiness.load import load_college_readiness
from .early_childhood.transform import transform_early_childhood
from .early_childhood.load import load_early_childhood
from .eem.transform import transform_eem
from .eem.geocode import geocode_schools
from .eem.load import load_eem, load_school_geocode
from .grad_dropout.transform import transform_grad_dropout
from .grad_dropout.load import load_grad_dropout
from .non_resident.transform import transform_non_resident
from .non_resident.load import load_non_resident
from .student_counts.transform import transform_student_counts
from .student_counts.load import load_student_counts
from .student_mobility.transform import transform_student_mobility
from .student_mobility.load import load_student_mobility


SCHEMA = "education"


@dataclass(frozen=True)
class Asset:
    """One table, and how to build it."""

    name: str  # the table name in SCHEMA, and the asset's key
    group: str  # the module that owns it
    materialize: Callable[[], None]
    deps: tuple[str, ...] = field(default_factory=tuple)
    description: str = ""


def _chain(*behaviors: Callable[[], None]) -> Callable[[], None]:
    """Fold several behaviors into one materialization."""

    def materialize() -> None:
        for behavior in behaviors:
            behavior()

    return materialize


def _simple(name: str, transform: Callable[[], None], load: Callable[[], None]) -> Asset:
    return Asset(name=name, group=name, materialize=_chain(transform, load))


_ASSETS = [
    _simple("attendance", transform_attendance, load_attendance),
    _simple("college_destination", transform_college_destination, load_college_destination),
    _simple("college_enrollment", transform_college_enrollment, load_college_enrollment),
    _simple("college_readiness", transform_college_readiness, load_college_readiness),
    _simple("early_childhood", transform_early_childhood, load_early_childhood),
    _simple("grad_dropout", transform_grad_dropout, load_grad_dropout),
    _simple("non_resident", transform_non_resident, load_non_resident),
    _simple("student_counts", transform_student_counts, load_student_counts),
    _simple("student_mobility", transform_student_mobility, load_student_mobility),
    _simple("eem", transform_eem, load_eem),
    Asset(
        name="school_geocodes",
        group="eem",
        materialize=_chain(geocode_schools, load_school_geocode),
        deps=("eem",),
        description="Geocoded building addresses from the eem directory.",
    ),
]

ASSETS: dict[str, Asset] = {a.name: a for a in _ASSETS}


def materialize_group(group: str) -> None:
    """Build every asset a module owns, in dependency order -- what `process.py` runs."""
    selected = [a for a in ASSETS.values() if a.group == group]
    if not selected:
        raise KeyError(f"No assets in group {group!r}")
    for a in selected:
        a.materialize()
