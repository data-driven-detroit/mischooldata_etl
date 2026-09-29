"""A registry of the datasets this project loads.

An orchestrator can walk this instead of importing each module by hand. Every
step is a zero-argument callable; the order within a dataset mirrors what
`process.py` runs, and each step reads what the previous one wrote to disk.
"""
from dataclasses import dataclass
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


@dataclass(frozen=True)
class Step:
    """One unit of work -- the granularity an orchestrator schedules and retries."""

    name: str
    run: Callable[[], None]


@dataclass(frozen=True)
class Dataset:
    """A dataset and the ordered steps that build it."""

    name: str
    steps: tuple[Step, ...]

    def run(self) -> None:
        """Run every step in order, the way `process.py` does."""
        for step in self.steps:
            step.run()


def _simple(name: str, transform: Callable[[], None], load: Callable[[], None]) -> Dataset:
    return Dataset(name, (Step("transform", transform), Step("load", load)))


DATASETS: dict[str, Dataset] = {
    d.name: d
    for d in [
        _simple("attendance", transform_attendance, load_attendance),
        _simple("college_destination", transform_college_destination, load_college_destination),
        _simple("college_enrollment", transform_college_enrollment, load_college_enrollment),
        _simple("college_readiness", transform_college_readiness, load_college_readiness),
        _simple("early_childhood", transform_early_childhood, load_early_childhood),
        _simple("grad_dropout", transform_grad_dropout, load_grad_dropout),
        _simple("non_resident", transform_non_resident, load_non_resident),
        _simple("student_counts", transform_student_counts, load_student_counts),
        _simple("student_mobility", transform_student_mobility, load_student_mobility),
        Dataset(
            "eem",
            (
                Step("transform", transform_eem),
                Step("geocode", geocode_schools),
                Step("load", load_eem),
                Step("load_geocode", load_school_geocode),
            ),
        ),
    ]
}
