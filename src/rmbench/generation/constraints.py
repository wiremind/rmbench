from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np


def apply_constraints(
    *,
    table_name: str,
    column_arrays: dict[str, list[Any]],
    n: int,
    constraints: dict[str, list[dict[str, Any]]],
    rng: np.random.Generator,
) -> None:
    rules = constraints.get(table_name)
    if not rules:
        return
    for rule in rules:
        kind = rule["kind"]
        if kind == "pointer":
            _apply_pointer(rule, column_arrays, n, rng)
        elif kind == "carry":
            _apply_carry(rule, column_arrays, n)
        elif kind == "coherence":
            _apply_coherence(rule, column_arrays, n)
        elif kind == "share":
            _apply_share(rule, column_arrays, n)
        else:
            raise ValueError(f"Unknown constraint kind {kind!r} in table {table_name}")


def _group_indices(
    key_cols: list[str],
    column_arrays: dict[str, list[Any]],
    n: int,
) -> dict[tuple, list[int]]:
    groups: dict[tuple, list[int]] = defaultdict(list)
    arrs = [column_arrays[c] for c in key_cols]
    for i in range(n):
        groups[tuple(a[i] for a in arrs)].append(i)
    return groups


def _apply_share(
    rule: dict[str, Any],
    column_arrays: dict[str, list[Any]],
    n: int,
) -> None:
    """Collapse ``columns`` to one value per ``key`` group.

    For quantities defined once per group rather than per row. Unlike ``carry``
    no pointer column selects the donor; the first row of the group supplies it.
    """
    key_cols = list(rule["key"])
    targets = [column_arrays[c] for c in rule["columns"] if c in column_arrays]
    if not targets:
        return
    for indices in _group_indices(key_cols, column_arrays, n).values():
        if len(indices) < 2:
            continue
        donor = indices[0]
        for column in targets:
            value = column[donor]
            for i in indices[1:]:
                column[i] = value


def _apply_pointer(
    rule: dict[str, Any],
    column_arrays: dict[str, list[Any]],
    n: int,
    rng: np.random.Generator,
) -> None:
    key_cols = list(rule["key"])
    target = column_arrays[rule["column"]]
    # choose_by flags the lowest row by those columns; without it the pick is random
    choose_by = list(rule.get("choose_by") or ())
    sort_keys = [column_arrays[c] for c in choose_by] if choose_by else []
    for indices in _group_indices(key_cols, column_arrays, n).values():
        # draw regardless so the RNG stream is identical either way
        fallback = indices[int(rng.integers(len(indices)))]
        if sort_keys:
            chosen = min(indices, key=lambda i: tuple(col[i] for col in sort_keys))
        else:
            chosen = fallback
        for i in indices:
            target[i] = i == chosen


def _apply_carry(rule: dict[str, Any], column_arrays: dict[str, list[Any]], n: int) -> None:
    key_cols = list(rule["key"])
    from_cols = list(rule["from_columns"])
    to_cols = list(rule["to_columns"])
    if len(from_cols) != len(to_cols):
        raise ValueError(f"carry: from_columns/to_columns length mismatch in rule {rule!r}")
    where = column_arrays[rule["where"]]
    from_arrs = [column_arrays[c] for c in from_cols]
    to_arrs = [column_arrays[c] for c in to_cols]
    for indices in _group_indices(key_cols, column_arrays, n).values():
        source_idx = next((i for i in indices if where[i] is True), indices[0])
        source_values = [arr[source_idx] for arr in from_arrs]
        for i in indices:
            for out_arr, val in zip(to_arrs, source_values, strict=True):
                out_arr[i] = val


def _apply_coherence(rule: dict[str, Any], column_arrays: dict[str, list[Any]], n: int) -> None:
    op = rule["op"]
    if op != "clip_upper":
        raise ValueError(f"coherence: unknown op {op!r}")
    tgt = column_arrays[rule["target"]]
    bnd = column_arrays[rule["by"]]
    for i in range(n):
        t, b = tgt[i], bnd[i]
        if t is None or b is None:
            continue
        if t > b:
            tgt[i] = type(t)(b) if type(t) is not type(b) else b


