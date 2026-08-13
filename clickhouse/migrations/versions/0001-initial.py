"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-07-09

Baseline schema of the benchmark tables.

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""\
CREATE TABLE fact_daily_od_bucket
(
    `time` DateTime64(3, 'UTC'),
    `day_x` Int32,
    `event_datetime` DateTime64(3, 'UTC'),
    `transporter_type` LowCardinality(String),
    `entity_name` LowCardinality(String),
    `route_name` LowCardinality(String),
    `service_id` Int64,
    `service_departure_date` Date,
    `service_number` String,
    `service_labels` Array(String),
    `service_status` String,
    `service_origin_station_name` String,
    `service_destination_station_name` String,
    `service_departure_datetime` DateTime64(3, 'UTC'),
    `service_arrival_datetime` DateTime64(3, 'UTC'),
    `service_timezone` String,
    `service_arrival_timezone` LowCardinality(String) DEFAULT 'UTC',
    `service_budget_objective` Nullable(Decimal(10, 2)),
    `service_yield_objective` Nullable(Decimal(10, 2)),
    `service_capacity` Nullable(Int32),
    `service_lid` Nullable(Int32),
    `service_max_leg_load_factor` Nullable(Decimal(10, 2)),
    `service_max_leg_cumulative_sum_net_bookings` Nullable(Int32),
    `service_optimization_status` LowCardinality(String),
    `service_pointer` Bool,
    `market_name` LowCardinality(String),
    `market_rank` Int32,
    `market_list_alerts` Array(String),
    `market_cabin_max_leg_load_factor` Nullable(Decimal(10, 2)),
    `market_cabin_max_leg_cumulative_sum_net_bookings` Nullable(Int32),
    `od_id` Int64,
    `od_status` String,
    `od_origin_station_name` String,
    `od_origin_timezone` LowCardinality(String) DEFAULT 'UTC',
    `od_destination_station_name` String,
    `od_destination_timezone` LowCardinality(String) DEFAULT 'UTC',
    `od_departure_datetime` DateTime64(3, 'UTC'),
    `od_arrival_datetime` DateTime64(3, 'UTC'),
    `od_rank` Int32,
    `reference_service_number` Nullable(String),
    `reference_service_departure_date` Nullable(Date),
    `cabin_name` String,
    `od_cabin_forecasted_traffic` Nullable(Decimal(10, 2)),
    `od_cabin_forecasted_revenue_vat_inc` Nullable(Decimal(10, 2)),
    `od_cabin_forecasted_revenue_vat_exc` Nullable(Decimal(10, 2)),
    `od_cabin_optimized_traffic` Nullable(Decimal(10, 2)),
    `od_cabin_optimized_revenue_vat_inc` Nullable(Decimal(10, 2)),
    `od_cabin_optimized_revenue_vat_exc` Nullable(Decimal(10, 2)),
    `od_cabin_last_predicted` Nullable(Decimal(10, 2)),
    `od_cabin_last_observed` Nullable(Decimal(10, 2)),
    `od_cabin_pointer` Bool,
    `family_name` LowCardinality(String),
    `bucket_name` LowCardinality(String),
    `bucket_order` Int32,
    `has_pricing_changed_day_x` Bool,
    `has_pricing_changed_bucket` Bool,
    `cumulative_sum_net_revenue_vat_inc` Decimal(10, 2),
    `cumulative_sum_net_revenue_vat_exc` Decimal(10, 2),
    `cumulative_sum_net_ancillary_revenue_vat_inc` Decimal(10, 2),
    `cumulative_sum_net_ancillary_revenue_vat_exc` Decimal(10, 2),
    `sum_net_revenue_vat_inc` Decimal(10, 2),
    `sum_net_revenue_vat_exc` Decimal(10, 2),
    `sum_net_ancillary_revenue_vat_inc` Decimal(10, 2),
    `sum_net_ancillary_revenue_vat_exc` Decimal(10, 2),
    `availability_start_day` Nullable(Int32),
    `availability_end_day` Nullable(Int32),
    `cumul_availability_start_day` Int32,
    `cumul_availability_end_day` Int32,
    `cumulative_sum_net_bookings` Nullable(Int32),
    `sum_confirmed_bookings` Nullable(Int32),
    `sum_net_bookings` Nullable(Int32),
    `is_first_available_start_day` Bool,
    `is_first_available_end_day` Bool,
    `price_vat_inc` Nullable(Decimal(10, 2)),
    `cabin_capacity` Nullable(Int32),
    `cabin_lid` Nullable(Int32),
    `bucket_authorization_start_day` Nullable(Int32),
    `bucket_authorization_end_day` Nullable(Int32),
    `forecast_full_day_x` Nullable(Int32),
    `optimization_full_day_x` Nullable(Int32),
    `unconstrained_demand_bookings` Nullable(Decimal(10, 2)),
    `unconstrained_demand_revenue` Nullable(Decimal(10, 2)),
    `unconstrained_forecast_bookings` Nullable(Decimal(10, 2)),
    `unconstrained_forecast_revenue` Nullable(Decimal(10, 2)),
    `is_last` Bool,
    `is_optim_current` Bool,
    `insertion_datetime` Nullable(DateTime) DEFAULT now(),
    `is_service_model_available` Nullable(Bool) DEFAULT NULL,
    INDEX is_last_index is_last TYPE set(3) GRANULARITY 1,
    INDEX idx_event_datetime event_datetime TYPE minmax GRANULARITY 1,
    INDEX idx_od_id od_id TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fact_daily_od_bucket/', '{replica}')
PARTITION BY toYYYYMM(event_datetime)
PRIMARY KEY (service_departure_date, service_number, toStartOfDay(event_datetime))
ORDER BY (service_departure_date, service_number, toStartOfDay(event_datetime), cabin_name, family_name, bucket_name, od_id)
SETTINGS index_granularity = 8192, enable_max_bytes_limit_for_min_age_to_force_merge = 1, min_age_to_force_merge_on_partition_only = 1, min_age_to_force_merge_seconds = 7200
""")

    op.execute("""\
CREATE TABLE fact_daily_leg_physical_inventory
(
    `time` DateTime64(3, 'UTC'),
    `day_x` Int16,
    `event_datetime` DateTime64(3, 'UTC'),
    `transporter_type` LowCardinality(String),
    `entity_name` LowCardinality(String),
    `route_name` LowCardinality(String),
    `service_number` String,
    `service_timezone` LowCardinality(String) DEFAULT 'UTC',
    `service_arrival_timezone` LowCardinality(String) DEFAULT 'UTC',
    `service_labels` Array(String),
    `service_status` LowCardinality(String),
    `service_departure_date` Date,
    `service_origin_station_name` String,
    `service_destination_station_name` String,
    `service_departure_datetime` DateTime64(3, 'UTC'),
    `service_arrival_datetime` DateTime64(3, 'UTC'),
    `is_service_peak_leg` Bool,
    `is_service_max_leg` Bool,
    `service_max_leg_origin_station_name` String,
    `service_max_leg_destination_station_name` String,
    `leg_id` Int64,
    `leg_status` String,
    `leg_origin_station_name` String,
    `leg_origin_timezone` LowCardinality(String) DEFAULT 'UTC',
    `leg_destination_station_name` String,
    `leg_destination_timezone` LowCardinality(String) DEFAULT 'UTC',
    `leg_departure_datetime` DateTime64(3, 'UTC'),
    `leg_arrival_datetime` DateTime64(3, 'UTC'),
    `leg_order` Int32,
    `leg_list_alerts` Array(String),
    `cabin_name` LowCardinality(String),
    `physical_inventory_name` LowCardinality(String),
    `physical_inventory_capacity` Nullable(Int32),
    `physical_inventory_lid` Nullable(Int32),
    `physical_availability_start_day` Nullable(Int32),
    `physical_availability_end_day` Nullable(Int32),
    `cumulative_sum_net_revenue_vat_inc` Decimal(10, 2),
    `cumulative_sum_net_revenue_vat_exc` Decimal(10, 2),
    `cumulative_sum_net_ancillary_revenue_vat_inc` Decimal(10, 2),
    `cumulative_sum_net_ancillary_revenue_vat_exc` Decimal(10, 2),
    `sum_net_revenue_vat_inc` Decimal(10, 2),
    `sum_net_revenue_vat_exc` Decimal(10, 2),
    `sum_net_ancillary_revenue_vat_inc` Decimal(10, 2),
    `sum_net_ancillary_revenue_vat_exc` Decimal(10, 2),
    `cumulative_sum_net_bookings` Int32,
    `sum_net_bookings` Int32,
    `forecast_full_day_x` Nullable(Int32),
    `optimization_full_day_x` Nullable(Int32),
    `unconstrained_demand_bookings` Nullable(Decimal(10, 2)),
    `unconstrained_forecast_bookings` Nullable(Decimal(10, 2)),
    `is_last` Bool,
    `final_forecast_bookings` Nullable(Decimal(10, 2)),
    `insertion_datetime` Nullable(DateTime) DEFAULT now(),
    `service_utc_offset_minutes` Int16 DEFAULT 0,
    INDEX is_last_index is_last TYPE set(3) GRANULARITY 1,
    INDEX idx_event_datetime event_datetime TYPE minmax GRANULARITY 1,
    INDEX idx_leg_id leg_id TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fact_daily_leg_physical_inventory/', '{replica}')
PARTITION BY toYYYYMM(event_datetime)
PRIMARY KEY (service_departure_date, service_number, toStartOfDay(event_datetime))
ORDER BY (service_departure_date, service_number, toStartOfDay(event_datetime), cabin_name, physical_inventory_name, leg_id)
SETTINGS index_granularity = 8192, enable_max_bytes_limit_for_min_age_to_force_merge = 1, min_age_to_force_merge_on_partition_only = 1, min_age_to_force_merge_seconds = 7200
""")

    op.execute("""\
CREATE TABLE fact_passenger_event
(
    `time` DateTime64(3, 'UTC'),
    `day_x` Int32,
    `event_datetime` DateTime64(3, 'UTC'),
    `transporter_type` LowCardinality(String),
    `entity_name` LowCardinality(String),
    `route_name` LowCardinality(String),
    `service_id` Int64,
    `service_departure_date` Date,
    `service_number` String,
    `service_labels` Array(String),
    `service_status` LowCardinality(String),
    `service_origin_station_name` String,
    `service_destination_station_name` String,
    `service_departure_datetime` DateTime64(3, 'UTC'),
    `service_arrival_datetime` DateTime64(3, 'UTC'),
    `service_timezone` LowCardinality(String),
    `service_arrival_timezone` LowCardinality(String) DEFAULT 'UTC',
    `market_id` Int64,
    `market_name` LowCardinality(String),
    `market_rank` Nullable(Int32),
    `od_id` Int64,
    `od_status` String,
    `od_origin_station_name` String,
    `od_origin_timezone` LowCardinality(String) DEFAULT 'UTC',
    `od_destination_station_name` String,
    `od_destination_timezone` LowCardinality(String) DEFAULT 'UTC',
    `od_destination_station_id` Int64,
    `od_departure_datetime` DateTime64(3, 'UTC'),
    `od_arrival_datetime` DateTime64(3, 'UTC'),
    `od_rank` Nullable(Int32),
    `cabin_name` LowCardinality(String),
    `family_name` LowCardinality(String),
    `bucket_name` LowCardinality(String),
    `bucket_order` Int32,
    `cabin_capacity` Nullable(Int32),
    `cabin_lid` Nullable(Int32),
    `physical_inventory_name` String,
    `price_vat_inc` Decimal(10, 2),
    `price_vat_exc` Decimal(10, 2),
    `base_price_vat_inc` Decimal(10, 2),
    `bucket_price_vat_inc` Nullable(Decimal(10, 2)),
    `ancillary_revenue_vat_inc` Decimal(10, 2),
    `ancillary_revenue_vat_exc` Decimal(10, 2),
    `event_type` LowCardinality(String),
    `booking_key` String,
    `customer_key` Nullable(String),
    `ticket_category` Nullable(String),
    `ticket_key` String,
    `fare_code` Nullable(String),
    `sale_channel_name` Nullable(String),
    `sale_channel_type` Nullable(String),
    `is_ticket_exchanged` Bool,
    `is_confirmed` Bool,
    `no_show` Bool,
    `insertion_datetime` Nullable(DateTime) DEFAULT now(),
    INDEX idx_event_datetime event_datetime TYPE minmax GRANULARITY 1,
    INDEX idx_od_id od_id TYPE bloom_filter GRANULARITY 1,
    INDEX idx_route_name route_name TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/fact_passenger_event/', '{replica}')
PARTITION BY toYYYYMM(event_datetime)
PRIMARY KEY (event_type, service_departure_date, service_number, toStartOfDay(event_datetime))
ORDER BY (event_type, service_departure_date, service_number, toStartOfDay(event_datetime), od_id, cabin_name, family_name, ticket_key)
SETTINGS index_granularity = 8192, enable_max_bytes_limit_for_min_age_to_force_merge = 1, min_age_to_force_merge_on_partition_only = 1, min_age_to_force_merge_seconds = 7200
""")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS fact_daily_od_bucket SYNC")
    op.execute("DROP TABLE IF EXISTS fact_daily_leg_physical_inventory SYNC")
    op.execute("DROP TABLE IF EXISTS fact_passenger_event SYNC")
