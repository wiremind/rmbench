from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

QNULL = "qnull"
EMPTY = "empty"
NOT_NULL = "not_null"

KIND_DECIMAL = "decimal"
KIND_INTEGER = "integer"
KIND_BOOL = "bool"
KIND_STRING = "string"
KIND_ARRAY = "array"
KIND_DATE = "date"
KIND_DATETIME_MS = "datetime_ms"

SOURCE_CATEGORY = "category"
SOURCE_METRIC = "metric"
SOURCE_DERIVED_METRIC = "derived_metric"
SOURCE_SYNTH_ID = "synth_id"
SOURCE_SYNTH_TOKEN = "synth_token"
SOURCE_SYNTH_ARRAY = "synth_array"
SOURCE_FIXED = "fixed"
SOURCE_BOOL_RATE = "bool_rate"
SOURCE_DAY_X = "day_x"
SOURCE_SERVICE_DEPARTURE_DATE = "service_departure_date"
SOURCE_EVENT_DATETIME = "event_datetime"
SOURCE_DERIVED_SERVICE_DEPARTURE_DATETIME = "service_departure_datetime"
SOURCE_DERIVED_SERVICE_ARRIVAL_DATETIME = "service_arrival_datetime"
SOURCE_DERIVED_LEG_DEPARTURE_DATETIME = "leg_departure_datetime"
SOURCE_DERIVED_LEG_ARRIVAL_DATETIME = "leg_arrival_datetime"
SOURCE_DERIVED_IS_LAST = "derived_is_last"
SOURCE_EMPTY = "always_empty"

ARRAY_ITEM_ORDINALS = 30


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    kind: str
    source: str
    # declarative only, so every NULL is written as an empty field
    null_encoding: str = QNULL
    options: dict[str, Any] = field(default_factory=dict)


def _cat(name: str, *, entity: str | None = None) -> ColumnSpec:
    options: dict[str, Any] = {}
    if entity is not None:
        options["entity"] = entity
    return ColumnSpec(name, KIND_STRING, SOURCE_CATEGORY, NOT_NULL, options)


def _metric(name: str, *, kind: str = KIND_DECIMAL, metric: str | None = None, null_encoding: str = QNULL,
            allow_negative: bool = False, entity: str | None = None) -> ColumnSpec:
    options: dict[str, Any] = {"metric": metric or name, "allow_negative": allow_negative}
    if entity is not None:
        options["entity"] = entity
    return ColumnSpec(name, kind, SOURCE_METRIC, null_encoding, options)


def _bool(name: str) -> ColumnSpec:
    # true_rate comes from the bundle; the columns it does not cover are set by a
    # pointer rule in constraints.yaml, which replaces the drawn value entirely
    return ColumnSpec(name, KIND_BOOL, SOURCE_BOOL_RATE, NOT_NULL)


def _is_last() -> ColumnSpec:
    # Structural, not sampled: is_last marks a departure's final snapshot and is
    # therefore a function of day_x, constant within a slice.
    return ColumnSpec("is_last", KIND_BOOL, SOURCE_DERIVED_IS_LAST, NOT_NULL)


def _synth_id(
    name: str,
    *,
    low: int = 1,
    high: int = 10_000_000,
    entity: str | None = None,
    derive_from: str | None = None,
    subdivide: str | None = None,
    cluster_by_parent: bool = False,
) -> ColumnSpec:
    # derive_from: stable hash of another entity-pooled column instead of a draw
    # subdivide: refine each derive_from value into N ids, N from this pool key
    # cluster_by_parent: children of one parent get numerically adjacent ids
    options: dict[str, Any] = {"low": low, "high": high}
    if entity is not None:
        options["entity"] = entity
    if derive_from is not None:
        options["derive_from"] = derive_from
    if subdivide is not None:
        options["subdivide"] = subdivide
    if cluster_by_parent:
        options["cluster_by_parent"] = True
    return ColumnSpec(name, KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, options)


def _synth_token(
    name: str,
    prefix: str,
    *,
    ordinals: int = 200,
    entity: str | None = None,
    scope_by_parent: bool = False,
    unique_per_member: bool = False,
) -> ColumnSpec:
    # scope_by_parent: draw from the parent's token subset, not the full space
    # unique_per_member: one distinct token per pool member
    options: dict[str, Any] = {"prefix": prefix, "ordinals": ordinals}
    if entity is not None:
        options["entity"] = entity
    if scope_by_parent:
        options["scope_by_parent"] = True
    if unique_per_member:
        options["unique_per_member"] = True
    return ColumnSpec(name, KIND_STRING, SOURCE_SYNTH_TOKEN, NOT_NULL, options)


def _synth_array(
    name: str,
    *,
    prefix: str = "LABEL",
    min_items: int = 0,
    max_items: int = 3,
    null_encoding: str = NOT_NULL,
    entity: str | None = None,
) -> ColumnSpec:
    options: dict[str, Any] = {
        "prefix": prefix,
        "min_items": min_items,
        "max_items": max_items,
        "item_ordinals": ARRAY_ITEM_ORDINALS,
    }
    if entity is not None:
        options["entity"] = entity
    return ColumnSpec(name, KIND_ARRAY, SOURCE_SYNTH_ARRAY, null_encoding, options)


def _derived(
    name: str,
    *,
    anchor: str,
    ratio_profile: str,
    kind: str = KIND_DECIMAL,
    null_encoding: str = NOT_NULL,
    allow_negative: bool = False,
    entity: str | None = None,
) -> ColumnSpec:
    options: dict[str, Any] = {
        "anchor": anchor,
        "ratio_profile": ratio_profile,
        "allow_negative": allow_negative,
    }
    if entity is not None:
        options["entity"] = entity
    return ColumnSpec(name, kind, SOURCE_DERIVED_METRIC, null_encoding, options)


def _fixed(name: str, value: str) -> ColumnSpec:
    return ColumnSpec(name, KIND_STRING, SOURCE_FIXED, NOT_NULL, {"value": value})


def _empty(name: str, *, kind: str = KIND_STRING) -> ColumnSpec:
    # Never populate, these columns are NULL in every source row
    return ColumnSpec(name, kind, SOURCE_EMPTY, EMPTY)


def _date_dep() -> ColumnSpec:
    return ColumnSpec("service_departure_date", KIND_DATE, SOURCE_SERVICE_DEPARTURE_DATE, NOT_NULL)


def _day_x() -> ColumnSpec:
    return ColumnSpec("day_x", KIND_INTEGER, SOURCE_DAY_X, NOT_NULL)


def _event_dt(*, spread: bool = False) -> ColumnSpec:
    # spread: per-row timestamps across the sale day, instead of the single
    # end-of-day snapshot stamp used by the daily aggregate tables
    options = {"spread": True} if spread else {}
    return ColumnSpec("event_datetime", KIND_DATETIME_MS, SOURCE_EVENT_DATETIME, NOT_NULL, options)


def _derived_dt(name: str, kind_source: str) -> ColumnSpec:
    return ColumnSpec(name, KIND_DATETIME_MS, kind_source, NOT_NULL)


OD_COLUMNS: tuple[ColumnSpec, ...] = (
    _cat("transporter_type", entity="service"),
    _synth_token("entity_name", "ENTITY", ordinals=50, entity="service"),
    _cat("route_name", entity="service"),
    _synth_id("service_id", low=100_000, high=10_000_000, entity="service"),
    _synth_id("service_number", low=1000, high=999_999, entity="service"),
    _synth_array("service_labels", prefix="LABEL", min_items=1, max_items=5, entity="service"),
    _date_dep(),
    _synth_token("service_origin_station_name", "STATION", ordinals=300, entity="service"),
    _synth_token("service_destination_station_name", "STATION", ordinals=300, entity="service"),
    _derived_dt("service_departure_datetime", SOURCE_DERIVED_SERVICE_DEPARTURE_DATETIME),
    _derived_dt("service_arrival_datetime", SOURCE_DERIVED_SERVICE_ARRIVAL_DATETIME),
    _fixed("service_timezone", "Europe/Paris"),
    _metric("service_budget_objective", entity="service"),
    _metric("service_yield_objective", entity="service"),
    _synth_id("od_id", low=1_000_000, high=99_999_999, entity="od", cluster_by_parent=True),
    _derived_dt("od_departure_datetime", SOURCE_DERIVED_SERVICE_DEPARTURE_DATETIME),
    _derived_dt("od_arrival_datetime", SOURCE_DERIVED_SERVICE_ARRIVAL_DATETIME),
    _synth_token("od_origin_station_name", "STATION", ordinals=300, entity="od", scope_by_parent=True),
    _synth_token("od_destination_station_name", "STATION", ordinals=300, entity="od", scope_by_parent=True),
    _cat("market_name", entity="od"),
    _synth_array("market_list_alerts", prefix="ALERT", min_items=0, max_items=3, null_encoding=NOT_NULL, entity="od"),
    _empty("reference_service_number"),
    _empty("reference_service_departure_date"),
    _event_dt(),
    _day_x(),
    _is_last(),
    _cat("service_status", entity="service"),
    ColumnSpec("service_capacity", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 100, "high": 800, "entity": "service"}),
    ColumnSpec("service_lid", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 100, "high": 800, "entity": "service"}),
    _cat("od_status", entity="od"),
    ColumnSpec("market_rank", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 1, "high": 50, "entity": "od"}),
    ColumnSpec("od_rank", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 1, "high": 50, "entity": "od"}),
    _metric("service_max_leg_load_factor", null_encoding=NOT_NULL, entity="service"),
    _metric("service_max_leg_cumulative_sum_net_bookings", kind=KIND_INTEGER, null_encoding=NOT_NULL, entity="service"),
    _fixed("service_optimization_status", "ACTIVATED"),
    ColumnSpec("cabin_capacity", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 50, "high": 800, "entity": "service"}),
    ColumnSpec("cabin_lid", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 50, "high": 800, "entity": "service"}),
    _metric("market_cabin_max_leg_load_factor", null_encoding=NOT_NULL, entity="od"),
    _metric("market_cabin_max_leg_cumulative_sum_net_bookings", kind=KIND_INTEGER, null_encoding=NOT_NULL, entity="od"),
    _metric("od_cabin_forecasted_traffic", null_encoding=NOT_NULL, entity="od"),
    _derived("od_cabin_forecasted_revenue_vat_inc", anchor="od_cabin_forecasted_traffic", ratio_profile="cumulative_unit_price_inc", entity="od"),
    _derived("od_cabin_forecasted_revenue_vat_exc", anchor="od_cabin_forecasted_traffic", ratio_profile="cumulative_unit_price_exc", entity="od"),
    _derived("od_cabin_optimized_traffic", anchor="od_cabin_forecasted_traffic", ratio_profile="optim_ratio", entity="od"),
    _derived("od_cabin_optimized_revenue_vat_inc", anchor="od_cabin_optimized_traffic", ratio_profile="cumulative_unit_price_inc", entity="od"),
    _derived("od_cabin_optimized_revenue_vat_exc", anchor="od_cabin_optimized_traffic", ratio_profile="cumulative_unit_price_exc", entity="od"),
    _derived("od_cabin_last_predicted", anchor="od_cabin_forecasted_traffic", ratio_profile="last_predicted_ratio", entity="od", null_encoding=QNULL),
    _derived("od_cabin_last_observed", anchor="od_cabin_forecasted_traffic", ratio_profile="last_observed_ratio", entity="od", null_encoding=QNULL),
    _bool("od_cabin_pointer"),
    _bool("service_pointer"),
    _cat("bucket_name"),
    _metric("bucket_authorization_start_day", kind=KIND_INTEGER),
    _metric("bucket_authorization_end_day", kind=KIND_INTEGER),
    _cat("cabin_name"),
    _cat("family_name"),
    ColumnSpec("bucket_order", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 1, "high": 200, "derive_from": "bucket_name"}),
    _bool("is_first_available_start_day"),
    _bool("is_first_available_end_day"),
    _bool("has_pricing_changed_day_x"),
    _bool("has_pricing_changed_bucket"),
    _metric("availability_start_day", kind=KIND_INTEGER, null_encoding=NOT_NULL),
    _metric("availability_end_day", kind=KIND_INTEGER, null_encoding=NOT_NULL),
    _metric("cumul_availability_start_day", kind=KIND_INTEGER, null_encoding=NOT_NULL),
    _metric("cumul_availability_end_day", kind=KIND_INTEGER, null_encoding=NOT_NULL),
    _metric("price_vat_inc"),
    _metric("cumulative_sum_net_bookings", kind=KIND_INTEGER, null_encoding=NOT_NULL),
    _derived("cumulative_sum_net_revenue_vat_inc", anchor="cumulative_sum_net_bookings", ratio_profile="cumulative_unit_price_inc", allow_negative=True),
    _derived("cumulative_sum_net_revenue_vat_exc", anchor="cumulative_sum_net_bookings", ratio_profile="cumulative_unit_price_exc", allow_negative=True),
    _derived("cumulative_sum_net_ancillary_revenue_vat_inc", anchor="cumulative_sum_net_revenue_vat_inc", ratio_profile="cumulative_ancillary_ratio_inc"),
    _derived("cumulative_sum_net_ancillary_revenue_vat_exc", anchor="cumulative_sum_net_revenue_vat_exc", ratio_profile="cumulative_ancillary_ratio_exc"),
    _derived("sum_confirmed_bookings", kind=KIND_INTEGER, anchor="sum_net_bookings", ratio_profile="confirmed_ratio"),
    _metric("sum_net_bookings", kind=KIND_INTEGER, allow_negative=True, null_encoding=NOT_NULL),
    _derived("sum_net_revenue_vat_inc", anchor="sum_net_bookings", ratio_profile="daily_unit_price_inc", allow_negative=True),
    _derived("sum_net_revenue_vat_exc", anchor="sum_net_bookings", ratio_profile="daily_unit_price_exc", allow_negative=True),
    _derived("sum_net_ancillary_revenue_vat_inc", anchor="sum_net_revenue_vat_inc", ratio_profile="daily_ancillary_ratio_inc", allow_negative=True),
    _derived("sum_net_ancillary_revenue_vat_exc", anchor="sum_net_revenue_vat_exc", ratio_profile="daily_ancillary_ratio_exc", allow_negative=True),
    _empty("forecast_full_day_x", kind=KIND_INTEGER),
    _empty("optimization_full_day_x", kind=KIND_INTEGER),
    # deriving from cumulative_sum_net_bookings would undergenerate non-zero rows
    _metric("unconstrained_demand_bookings", null_encoding=NOT_NULL, entity="od"),
    _metric("unconstrained_demand_revenue", null_encoding=NOT_NULL, entity="od"),
    _metric("unconstrained_forecast_bookings", null_encoding=NOT_NULL, entity="od"),
    _metric("unconstrained_forecast_revenue", null_encoding=NOT_NULL, entity="od"),
    _bool("is_optim_current"),
)


LEG_COLUMNS: tuple[ColumnSpec, ...] = (
    _event_dt(),
    _day_x(),
    _is_last(),
    _cat("service_status", entity="service"),
    ColumnSpec("service_utc_offset_minutes", KIND_INTEGER, SOURCE_FIXED, NOT_NULL, {"value": "60"}),
    _synth_token("service_max_leg_origin_station_name", "STATION", ordinals=300, entity="service"),
    _synth_token("service_max_leg_destination_station_name", "STATION", ordinals=300, entity="service"),
    _cat("transporter_type", entity="service"),
    _synth_token("entity_name", "ENTITY", ordinals=50, entity="service"),
    _cat("route_name", entity="service"),
    _synth_id("service_number", low=1000, high=999_999, entity="service"),
    _synth_array("service_labels", prefix="LABEL", min_items=1, max_items=5, entity="service"),
    _date_dep(),
    _synth_token("service_origin_station_name", "STATION", ordinals=300, entity="service"),
    _synth_token("service_destination_station_name", "STATION", ordinals=300, entity="service"),
    _derived_dt("service_departure_datetime", SOURCE_DERIVED_SERVICE_DEPARTURE_DATETIME),
    _derived_dt("service_arrival_datetime", SOURCE_DERIVED_SERVICE_ARRIVAL_DATETIME),
    _synth_id("leg_id", low=100_000, high=99_999_999, entity="leg", cluster_by_parent=True),
    _derived_dt("leg_departure_datetime", SOURCE_DERIVED_LEG_DEPARTURE_DATETIME),
    _derived_dt("leg_arrival_datetime", SOURCE_DERIVED_LEG_ARRIVAL_DATETIME),
    _synth_token("leg_origin_station_name", "STATION", ordinals=300, entity="leg", scope_by_parent=True),
    _synth_token("leg_destination_station_name", "STATION", ordinals=300, entity="leg", scope_by_parent=True),
    # position of the leg within its service, not a random draw, so it carries no
    # low/high range and needs no calibrated distinct count
    ColumnSpec(
        "leg_order",
        KIND_INTEGER,
        SOURCE_SYNTH_ID,
        NOT_NULL,
        {"entity": "leg", "ordinal_within_parent": True},
    ),
    _synth_array("leg_list_alerts", prefix="ALERT", min_items=0, max_items=3, null_encoding=NOT_NULL, entity="leg"),
    _cat("leg_status", entity="leg"),
    _bool("is_service_max_leg"),
    _bool("is_service_peak_leg"),
    _cat("cabin_name"),
    _cat("physical_inventory_name"),
    ColumnSpec("physical_inventory_capacity", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 50, "high": 800, "entity": "leg"}),
    ColumnSpec("physical_inventory_lid", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 50, "high": 800, "entity": "leg"}),
    ColumnSpec("physical_availability_start_day", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 0, "high": 200}),
    ColumnSpec("physical_availability_end_day", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 0, "high": 200}),
    _derived("cumulative_sum_net_bookings", kind=KIND_INTEGER, anchor="physical_inventory_lid", ratio_profile="cumulative_load_factor"),
    _derived("cumulative_sum_net_revenue_vat_inc", anchor="cumulative_sum_net_bookings", ratio_profile="cumulative_unit_price_inc", allow_negative=True),
    _derived("cumulative_sum_net_revenue_vat_exc", anchor="cumulative_sum_net_bookings", ratio_profile="cumulative_unit_price_exc", allow_negative=True),
    _derived("cumulative_sum_net_ancillary_revenue_vat_inc", anchor="cumulative_sum_net_revenue_vat_inc", ratio_profile="cumulative_ancillary_ratio_inc"),
    _derived("cumulative_sum_net_ancillary_revenue_vat_exc", anchor="cumulative_sum_net_revenue_vat_exc", ratio_profile="cumulative_ancillary_ratio_exc"),
    _derived("sum_net_bookings", kind=KIND_INTEGER, anchor="cumulative_sum_net_bookings", ratio_profile="daily_to_cumulative_ratio", allow_negative=True),
    _derived("sum_net_revenue_vat_inc", anchor="sum_net_bookings", ratio_profile="daily_unit_price_inc", allow_negative=True),
    _derived("sum_net_revenue_vat_exc", anchor="sum_net_bookings", ratio_profile="daily_unit_price_exc", allow_negative=True),
    _derived("sum_net_ancillary_revenue_vat_inc", anchor="sum_net_revenue_vat_inc", ratio_profile="daily_ancillary_ratio_inc", allow_negative=True),
    _derived("sum_net_ancillary_revenue_vat_exc", anchor="sum_net_revenue_vat_exc", ratio_profile="daily_ancillary_ratio_exc", allow_negative=True),
    _empty("forecast_full_day_x", kind=KIND_INTEGER),
    _empty("optimization_full_day_x", kind=KIND_INTEGER),
    _derived("unconstrained_demand_bookings", anchor="cumulative_sum_net_bookings", ratio_profile="unconstrained_demand_ratio"),
    _derived("unconstrained_forecast_bookings", anchor="cumulative_sum_net_bookings", ratio_profile="unconstrained_forecast_ratio"),
    _derived("final_forecast_bookings", anchor="cumulative_sum_net_bookings", ratio_profile="final_forecast_ratio", null_encoding=QNULL),
)


PAX_COLUMNS: tuple[ColumnSpec, ...] = (
    _cat("transporter_type", entity="service"),
    _synth_token("entity_name", "ENTITY", ordinals=50, entity="service"),
    _cat("route_name", entity="service"),
    _synth_id("service_id", low=100_000, high=10_000_000, entity="service"),
    _synth_id("service_number", low=1000, high=999_999, entity="service"),
    _synth_array("service_labels", prefix="LABEL", min_items=1, max_items=5, entity="service"),
    _date_dep(),
    _synth_token("service_origin_station_name", "STATION", ordinals=300, entity="service"),
    _synth_token("service_destination_station_name", "STATION", ordinals=300, entity="service"),
    _derived_dt("service_departure_datetime", SOURCE_DERIVED_SERVICE_DEPARTURE_DATETIME),
    _derived_dt("service_arrival_datetime", SOURCE_DERIVED_SERVICE_ARRIVAL_DATETIME),
    _fixed("service_timezone", "Europe/Paris"),
    _synth_id("od_id", low=1_000_000, high=99_999_999, entity="od", cluster_by_parent=True),
    _derived_dt("od_departure_datetime", SOURCE_DERIVED_SERVICE_DEPARTURE_DATETIME),
    _derived_dt("od_arrival_datetime", SOURCE_DERIVED_SERVICE_ARRIVAL_DATETIME),
    _synth_token("od_origin_station_name", "STATION", ordinals=300, entity="od", scope_by_parent=True),
    _synth_id("od_destination_station_id", low=1, high=10_000, entity="od"),
    _synth_token("od_destination_station_name", "STATION", ordinals=300, entity="od", scope_by_parent=True),
    _cat("event_type"),
    _synth_token("booking_key", "BK", entity="booking", unique_per_member=True),
    _synth_token("customer_key", "CK", entity="booking", unique_per_member=True),
    _synth_token("ticket_key", "TK", entity="booking", unique_per_member=True),
    _synth_id("market_id", low=1, high=10_000_000, entity="od", derive_from="market_name", subdivide="market"),
    _cat("market_name", entity="od"),
    ColumnSpec("market_rank", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 1, "high": 50, "entity": "od"}),
    ColumnSpec("cabin_capacity", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 50, "high": 800, "entity": "service"}),
    ColumnSpec("cabin_lid", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 50, "high": 800, "entity": "service"}),
    ColumnSpec("od_rank", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 1, "high": 50, "entity": "od"}),
    _cat("service_status", entity="service"),
    _cat("od_status", entity="od"),
    _cat("physical_inventory_name"),
    _cat("cabin_name"),
    _cat("family_name"),
    _cat("bucket_name"),
    ColumnSpec("bucket_order", KIND_INTEGER, SOURCE_SYNTH_ID, NOT_NULL, {"low": 1, "high": 200, "derive_from": "bucket_name"}),
    _derived("bucket_price_vat_inc", anchor="price_vat_inc", ratio_profile="bucket_price_ratio"),
    _bool("is_ticket_exchanged"),
    _bool("is_confirmed"),
    _day_x(),
    _metric("price_vat_inc", metric="price_vat_inc_abs", null_encoding=NOT_NULL),
    _derived("price_vat_exc", anchor="price_vat_inc", ratio_profile="tax_ratio"),
    _derived("base_price_vat_inc", anchor="price_vat_inc", ratio_profile="base_price_ratio"),
    _derived("ancillary_revenue_vat_inc", anchor="price_vat_inc", ratio_profile="ancillary_ratio_inc"),
    _derived("ancillary_revenue_vat_exc", anchor="price_vat_exc", ratio_profile="ancillary_ratio_exc"),
    _event_dt(spread=True),
    _cat("ticket_category"),
    _cat("fare_code"),
    _cat("sale_channel_name"),
    _cat("sale_channel_type"),
    _bool("no_show"),
)


# Each table's storage key, minus the parts constant within a snapshot slice
# (service_departure_date, and the DAY of event_datetime, so a within-day
# spread is not a sort key). Rows are emitted in this order because the source
# export arrives sorted the same way.
TABLE_SORT_KEYS: dict[str, tuple[str, ...]] = {
    "fact_daily_od_bucket": (
        "service_number",
        "cabin_name",
        "family_name",
        "bucket_name",
        "od_id",
    ),
    "fact_daily_leg_physical_inventory": (
        "service_number",
        "cabin_name",
        "physical_inventory_name",
        "leg_id",
    ),
    # pax leads on event_type, ahead of the date columns
    "fact_passenger_event": (
        "event_type",
        "service_number",
        "od_id",
        "cabin_name",
        "family_name",
        "ticket_key",
    ),
}

TABLE_COLUMNS: dict[str, tuple[ColumnSpec, ...]] = {
    "fact_daily_od_bucket": OD_COLUMNS,
    "fact_daily_leg_physical_inventory": LEG_COLUMNS,
    "fact_passenger_event": PAX_COLUMNS,
}


def output_columns(table_name: str) -> list[str]:
    return [spec.name for spec in TABLE_COLUMNS[table_name]]
