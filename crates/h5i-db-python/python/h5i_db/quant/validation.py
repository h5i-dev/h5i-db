"""Validation splitters for overlapping-label data (ROADMAP_QUANT.md Q3.4/VII-D4).

Ordinary k-fold cross-validation leaks when labels overlap in time, which in
finance they almost always do: a 5-bar forward return observed at time *t*
is built from prices up to *t+5*, so training on *t* and testing on *t+3*
means the training label already contains the test period. The model is
graded on data it was fitted through.

Two corrections, both here:

* **Purging** removes training observations whose *label horizon* overlaps
  the test set. It is the horizon that matters, not the observation time,
  which is why every splitter here takes one.
* **An embargo** additionally drops the training observations adjacent to the
  test set, covering serial correlation that outlives the label itself. Which
  side is adjacent depends on the splitter: a k-fold has training data after
  the test block, so the embargo runs forward (``embargo_span``); a
  walk-forward trains only on the past, so the observations at risk are the
  ones just before the test block and the embargo runs backward
  (``embargo_gap``).

The embargo width lives in ``embargo_width`` and nothing re-derives it.
Duplicating it is how a splitter and a walk-forward planner drift apart and
disagree about which spans a fold may see.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from typing import Iterator, Optional, Sequence

__all__ = [
    "Split",
    "purged_kfold",
    "combinatorial_purged",
    "walk_forward",
    "embargo_span",
    "embargo_gap",
    "embargo_width",
]


@dataclass(frozen=True)
class Split:
    """One train/test division, as positional indices."""

    train: tuple
    test: tuple
    #: Observations removed from training because their labels overlapped
    #: the test set, or fell inside the embargo.
    purged: tuple = ()

    def __post_init__(self) -> None:
        overlap = set(self.train) & set(self.test)
        if overlap:
            raise ValueError(
                f"train and test overlap at {sorted(overlap)[:5]}; a split that "
                "grades a model on its own training data is not a split"
            )

    @property
    def train_size(self) -> int:
        return len(self.train)

    @property
    def test_size(self) -> int:
        return len(self.test)


def embargo_width(total: int, embargo: float) -> int:
    """How many observations an embargo covers.

    ``embargo`` is a fraction of the whole sample, following the usual
    convention: 0.01 on 1,000 observations embargoes 10. The single owner of
    this arithmetic, so a splitter and a walk-forward planner cannot disagree
    about how wide a fold's embargo is.
    """
    if not 0.0 <= embargo < 1.0:
        raise ValueError(f"embargo must be in [0, 1), got {embargo}")
    return max(0, int(round(total * embargo)))


def embargo_span(test_end: int, total: int, embargo: float) -> range:
    """Indices embargoed after a test block ends.

    For splitters whose training set extends past the test block, which is
    every cross-validator here. A walk-forward wants
    :func:`embargo_gap` instead.
    """
    width = embargo_width(total, embargo)
    if width <= 0:
        return range(0)
    return range(test_end + 1, min(total, test_end + 1 + width))


def embargo_gap(test_start: int, total: int, embargo: float) -> range:
    """Indices embargoed immediately before a test block begins.

    The mirror image of :func:`embargo_span`, for a walk-forward: its training
    set lies entirely in the past, so the observations whose serial
    correlation reaches into the test block are the ones just before it. An
    embargo applied forward there removes nothing at all, which looks like an
    embargo and is not one.
    """
    width = embargo_width(total, embargo)
    if width <= 0:
        return range(0)
    return range(max(0, test_start - width), test_start)


def _purge(
    train: Sequence[int],
    test: Sequence[int],
    horizons: Optional[Sequence[int]],
    embargoed: set,
) -> tuple:
    """Drop training indices whose label reaches into the test set."""
    test_set = set(test)
    if not test_set:
        return tuple(train), ()
    first_test, last_test = min(test_set), max(test_set)

    kept, purged = [], []
    for index in train:
        if index in embargoed:
            purged.append(index)
            continue
        horizon = horizons[index] if horizons is not None else 0
        # The observation occupies [index, index + horizon]; if that touches
        # the test block at all, its label saw the test period.
        if index <= last_test and index + horizon >= first_test:
            purged.append(index)
            continue
        kept.append(index)
    return tuple(kept), tuple(purged)


def purged_kfold(
    n: int,
    *,
    folds: int = 5,
    horizons: Optional[Sequence[int]] = None,
    embargo: float = 0.0,
) -> Iterator[Split]:
    """K-fold over contiguous blocks, purged and embargoed.

    ``horizons[i]`` is how many observations forward observation *i*'s label
    depends on. A scalar horizon is the common case and can be passed as a
    list of one repeated value; leaving it out means labels are
    instantaneous, which is rarely true and is not assumed silently -- it is
    simply what ``None`` says.
    """
    if n < folds or folds < 2:
        raise ValueError(f"cannot cut {n} observations into {folds} folds")
    if horizons is not None and len(horizons) != n:
        raise ValueError("horizons must have one entry per observation")

    bounds = [round(index * n / folds) for index in range(folds + 1)]
    for fold in range(folds):
        start, stop = bounds[fold], bounds[fold + 1]
        test = tuple(range(start, stop))
        if not test:
            continue
        embargoed = set(embargo_span(stop - 1, n, embargo))
        candidate = [index for index in range(n) if index < start or index >= stop]
        train, purged = _purge(candidate, test, horizons, embargoed)
        yield Split(train=train, test=test, purged=purged)


def combinatorial_purged(
    n: int,
    *,
    groups: int = 6,
    test_groups: int = 2,
    horizons: Optional[Sequence[int]] = None,
    embargo: float = 0.0,
) -> Iterator[Split]:
    """Combinatorial purged cross-validation (López de Prado).

    Cuts the sample into ``groups`` blocks and tests on every combination of
    ``test_groups`` of them, which yields many more paths than k-fold and
    therefore a distribution of results rather than a single number. The
    count is ``C(groups, test_groups)``.
    """
    if groups < 2 or not 1 <= test_groups < groups:
        raise ValueError("need at least two groups and a smaller test set")
    if n < groups:
        raise ValueError(f"cannot cut {n} observations into {groups} groups")
    if horizons is not None and len(horizons) != n:
        raise ValueError("horizons must have one entry per observation")

    bounds = [round(index * n / groups) for index in range(groups + 1)]
    blocks = [tuple(range(bounds[i], bounds[i + 1])) for i in range(groups)]

    for chosen in itertools.combinations(range(groups), test_groups):
        test = tuple(sorted(itertools.chain.from_iterable(blocks[i] for i in chosen)))
        if not test:
            continue
        embargoed: set = set()
        # Every chosen block gets its own embargo; the blocks need not be
        # adjacent, so one span at the end would miss the interior ones.
        for index in chosen:
            if blocks[index]:
                embargoed |= set(embargo_span(blocks[index][-1], n, embargo))
        candidate = [index for index in range(n) if index not in set(test)]
        train, purged = _purge(candidate, test, horizons, embargoed)
        yield Split(train=train, test=test, purged=purged)


def walk_forward(
    n: int,
    *,
    train_size: int,
    test_size: int,
    step: Optional[int] = None,
    horizons: Optional[Sequence[int]] = None,
    embargo: float = 0.0,
    expanding: bool = False,
) -> Iterator[Split]:
    """Rolling or expanding walk-forward, purged and embargoed.

    Shares its embargo width with the cross-validators above rather than
    reimplementing it, which is the whole reason it lives in this module, but
    applies it backwards: training here is entirely before the test block, so
    the embargo drops the last observations of the training window rather than
    the first ones after the test. Forward, as the cross-validators apply it,
    it would remove nothing.
    """
    if train_size < 1 or test_size < 1:
        raise ValueError("train and test sizes must be positive")
    step = test_size if step is None else step
    if step < 1:
        raise ValueError("step must be positive")

    start = 0
    while start + train_size + test_size <= n:
        train_start = 0 if expanding else start
        train_end = start + train_size
        test = tuple(range(train_end, train_end + test_size))
        embargoed = set(embargo_gap(test[0], n, embargo))
        candidate = list(range(train_start, train_end))
        train, purged = _purge(candidate, test, horizons, embargoed)
        yield Split(train=train, test=test, purged=purged)
        start += step
