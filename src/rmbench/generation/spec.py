"""Loader for the YAML spec files under ``data/``.

- ``generator.yaml``: entity scaling rules, durations, ablation flags.
- ``constraints.yaml``: entity hierarchy, fanout, per-column rules.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

SPEC_DIR = Path(__file__).resolve().parent / "data"


@dataclass(frozen=True)
class RowScaling:
    sf1_od_row_count: int
    default_chunk_size: int
    default_seed: int
    member_target_rows: int


@dataclass(frozen=True)
class EntityPoolsConfig:
    departure_linear: tuple[str, ...]


@dataclass(frozen=True)
class DurationRange:
    min_seconds: int
    max_seconds: int


@dataclass(frozen=True)
class IntraDayRange:
    low_seconds: int
    high_seconds: int


@dataclass(frozen=True)
class DurationsConfig:
    service: DurationRange
    leg: DurationRange
    intra_day: IntraDayRange


@dataclass(frozen=True)
class AblationConfig:
    """Per-technique on/off toggles for leave-one-out fidelity ablation.

    Every flag defaults to True (full generator) so an absent ``ablation``
    section, or a section that omits a flag, behaves as before.
    """
    family_of_bucket: bool = True
    cabin_subset: bool = True
    constraints: bool = True
    pax_dedupe: bool = True
    metric_grid: bool = True
    per_rung: bool = True
    market_of_route: bool = True
    shared_entity_pools: bool = True


@dataclass(frozen=True)
class GeneratorConfig:
    row_scaling: RowScaling
    entity_pools: EntityPoolsConfig
    durations: DurationsConfig
    literal_values: dict[str, tuple[str, ...]]
    ablation: AblationConfig


def _load_yaml(name: str) -> dict[str, Any]:
    with (SPEC_DIR / name).open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@lru_cache(maxsize=1)
def load_generator_config() -> GeneratorConfig:
    raw = _load_yaml("generator.yaml")
    rs = raw["row_scaling"]
    ep = raw["entity_pools"]
    dur = raw["durations"]
    return GeneratorConfig(
        row_scaling=RowScaling(
            sf1_od_row_count=int(rs["sf1_od_row_count"]),
            default_chunk_size=int(rs["default_chunk_size"]),
            default_seed=int(rs["default_seed"]),
            member_target_rows=int(rs["member_target_rows"]),
        ),
        entity_pools=EntityPoolsConfig(
            departure_linear=tuple(ep["departure_linear"]),
        ),
        durations=DurationsConfig(
            service=DurationRange(
                min_seconds=int(dur["service"]["min_seconds"]),
                max_seconds=int(dur["service"]["max_seconds"]),
            ),
            leg=DurationRange(
                min_seconds=int(dur["leg"]["min_seconds"]),
                max_seconds=int(dur["leg"]["max_seconds"]),
            ),
            intra_day=IntraDayRange(
                low_seconds=int(dur["intra_day"]["low_seconds"]),
                high_seconds=int(dur["intra_day"]["high_seconds"]),
            ),
        ),
        literal_values={
            column: tuple(values)
            for column, values in raw["literal_values"].items()
        },
        ablation=_load_ablation(raw.get("ablation") or {}),
    )


def _load_ablation(raw: dict[str, Any]) -> AblationConfig:
    if not all(isinstance(value, bool) for value in raw.values()):
        raise ValueError(f"ablation flags must be unquoted true/false: {raw}")
    return AblationConfig(**raw)


@lru_cache(maxsize=1)
def load_constraints() -> dict[str, list[dict[str, Any]]]:
    # rule order within a table matters -> Carry after Pointer on the same key
    raw = _load_yaml("constraints.yaml")
    return {name: list(rules) for name, rules in raw["tables"].items()}


@lru_cache(maxsize=1)
def load_entity_hierarchy() -> dict[str, str]:
    raw = _load_yaml("constraints.yaml")
    return dict(raw["entity_hierarchy"])


@lru_cache(maxsize=1)
def load_fanout() -> dict[str, dict[str, Any]]:
    raw = _load_yaml("constraints.yaml")
    return {
        table_name: {
            "primary": fo["primary"],
            "fan_out": tuple(fo["fan_out"]),
            "per_rung": tuple(fo["per_rung"]),
        }
        for table_name, fo in raw["fanout"].items()
    }
