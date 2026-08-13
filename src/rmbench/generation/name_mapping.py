from __future__ import annotations

from dataclasses import dataclass

from rmbench.generation.bundle import Bundle

NAMESPACE_PREFIX_BY_COLUMN: dict[str, str] = {
    "bucket_name": "BUCKET",
    "cabin_name": "CABIN",
    "event_type": "EVENT_TYPE",
    "family_name": "FAMILY",
    "fare_code": "FARE",
    "leg_status": "LEG_STATUS",
    "market_name": "MARKET",
    "od_status": "OD_STATUS",
    "physical_inventory_name": "PINV",
    "route_name": "ROUTE",
    "sale_channel_name": "SC_NAME",
    "sale_channel_type": "SC_TYPE",
    "service_status": "SVC_STATUS",
    "ticket_category": "TICKET_CAT",
    "transporter_type": "TRANSPORTER",
}

# (short_prefix, base36 width) for columns whose values are short codes. The
# default `PREFIX_0001` form would pad these to a fixed width, and grouping keys
# are hashed per byte. Long-form names keep the default.
TOKEN_FORMAT_BY_COLUMN: dict[str, tuple[str, int]] = {
    "cabin_name": ("C", 1),     # C1..CZ    2 chars
    "bucket_name": ("BK", 2),   # BK01..    4 chars
    "family_name": ("FAM", 2),  # FAM01..   5 chars
    "route_name": ("R", 2),     # R01..RZZ  3 chars
    "market_name": ("M", 2),    # M01..MZZ  3 chars
}

_B36 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _base36(value: int, width: int) -> str:
    out = ""
    while value:
        value, rem = divmod(value, 36)
        out = _B36[rem] + out
    return out.rjust(width, "0")


def _token(column: str, ordinal: int, prefix: str) -> str:
    fmt = TOKEN_FORMAT_BY_COLUMN.get(column)
    if fmt is None:
        return f"{prefix}_{ordinal:04d}"
    short_prefix, width = fmt
    encoded = _base36(ordinal, width)
    if len(encoded) > width:
        return f"{prefix}_{ordinal:04d}"
    return f"{short_prefix}{encoded}"


NULL_TOKEN = "__NULL__"
NULL_OUTPUT = ""


@dataclass(frozen=True)
class NameMapping:
    by_cat_id: dict[str, str]

    def name_for(self, cat_id: str) -> str:
        if cat_id == NULL_TOKEN:
            return NULL_OUTPUT
        return self.by_cat_id[cat_id]


def _cat_ids_by_column(bundle: Bundle) -> dict[str, list[str]]:
    """Group category ids under the column that first declares them."""
    claimed: set[str] = set()
    by_column: dict[str, list[str]] = {}
    for table_data in bundle.tables.values():
        for column, category_distribution in table_data["category_distributions"].items():
            for entry in category_distribution["top_values"]:
                cat_id = entry["category_id"]
                if cat_id == NULL_TOKEN or cat_id in claimed:
                    continue
                claimed.add(cat_id)
                by_column.setdefault(column, []).append(cat_id)
    return by_column


def build_name_mapping(
    bundle: Bundle, literal_values: dict[str, tuple[str, ...]]
) -> NameMapping:
    """Map each calibrated category id to an emitted value.

    A column listed in ``literal_values`` gets those values assigned to its
    highest-share categories in order, so query predicates match; every other
    category gets a generated token.
    """
    by_cat_id: dict[str, str] = {}
    for namespace, cat_ids in _cat_ids_by_column(bundle).items():
        literals = literal_values.get(namespace, ())
        if literals:
            for cat_id, literal in zip(
                _share_ordered_cat_ids(bundle, namespace), literals, strict=False,
            ):
                by_cat_id[cat_id] = literal

        prefix = NAMESPACE_PREFIX_BY_COLUMN[namespace]
        unmapped = [cat_id for cat_id in sorted(cat_ids) if cat_id not in by_cat_id]
        for ordinal, cat_id in enumerate(unmapped, start=1):
            by_cat_id[cat_id] = _token(namespace, ordinal, prefix)

    return NameMapping(by_cat_id=by_cat_id)


def _share_ordered_cat_ids(bundle: Bundle, column: str) -> list[str]:
    cat_id_shares: dict[str, float] = {}
    for table_data in bundle.tables.values():
        category_distribution = table_data["category_distributions"].get(column)
        if category_distribution is None:
            continue
        for entry in category_distribution["top_values"]:
            cat_id = entry["category_id"]
            if cat_id == NULL_TOKEN:
                continue
            # the same category carries slightly different shares per table
            cat_id_shares[cat_id] = max(cat_id_shares.get(cat_id, 0.0), float(entry["share"]))
    return sorted(cat_id_shares, key=lambda c: -cat_id_shares[c])


def synth_tail_name(column: str, ordinal: int) -> str:
    prefix = NAMESPACE_PREFIX_BY_COLUMN[column]
    fmt = TOKEN_FORMAT_BY_COLUMN.get(column)
    if fmt is None:
        return f"{prefix}_TAIL_{ordinal:06d}"
    # tail names can be a large share of a column's rows, so a long form would
    # undo the main token's compaction
    short_prefix, width = fmt
    encoded = _base36(ordinal, width + 1)
    if len(encoded) > width + 1:
        return f"{prefix}_TAIL_{ordinal:06d}"
    return f"{short_prefix[0]}T{encoded}"
