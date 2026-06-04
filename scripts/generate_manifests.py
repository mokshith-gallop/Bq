#!/usr/bin/env python3
"""
Generate all per-table YAML manifests for the bulk migration pipeline.
Derives values from source Hive DDLs and target BigQuery DDLs.
"""

import os
import yaml


def write_manifest(path, data):
    """Write a YAML manifest with a comment header."""
    header = data.pop('_comment', '')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        if header:
            for line in header.strip().split('\n'):
                f.write(f'# {line}\n')
            f.write('\n')
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


# ============================================================================
# RAW DATABASE — 19 tables on acme-lake cluster
# ============================================================================
RAW_TABLES = [
    {
        '_comment': 'Manifest: raw.sales_retail\nSource: Hive CSV external table on acme-lake cluster',
        'source': {
            'database': 'raw', 'table': 'sales_retail', 'cluster': 'acme-lake',
            'format': 'textfile_csv',
            'gcs_path': 'gs://acme-migration-staging-us/raw/sales/',
            'partition_cols': ['date_ts'],
            'format_options': {'delimiter': ',', 'header': True, 'null_value': ''},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'sales_retail'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [{'col': 'quantity', 'from': 'INT', 'to': 'INT64'}],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['invoice_no']},
    },
    {
        '_comment': 'Manifest: raw.omniture_logs\nSource: Hive TSV external table on acme-lake (60 STRING columns, no header)',
        'source': {
            'database': 'raw', 'table': 'omniture_logs', 'cluster': 'acme-lake',
            'format': 'textfile_tsv',
            'gcs_path': 'gs://acme-migration-staging-us/raw/weblogs/',
            'partition_cols': ['date_ts'],
            'format_options': {'delimiter': '\t', 'header': False},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'omniture_logs'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [],
        },
        'wave': 'wave_3_large',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['col_2']},
    },
    {
        '_comment': 'Manifest: raw.returns_cdc\nSource: Hive CSV external table on acme-lake',
        'source': {
            'database': 'raw', 'table': 'returns_cdc', 'cluster': 'acme-lake',
            'format': 'textfile_csv',
            'gcs_path': 'gs://acme-migration-staging-us/raw/returns_cdc/',
            'partition_cols': ['snapshot_date'],
            'format_options': {'delimiter': ',', 'header': True, 'null_value': ''},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'returns_cdc'},
        'transforms': {
            'partition_key_conversion': None,
            'map_to_json': [],
            'type_widening': [
                {'col': 'return_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'snapshot_date', 'null_check_cols': ['return_id', 'invoice_no']},
    },
    {
        '_comment': 'Manifest: raw.pos_transactions\nSource: Hive Parquet external table on acme-lake',
        'source': {
            'database': 'raw', 'table': 'pos_transactions', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/raw/pos_transactions/',
            'partition_cols': ['date_ts'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'pos_transactions'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [
                {'col': 'txn_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'line_count', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_3_large',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['txn_id', 'invoice_no']},
    },
    {
        '_comment': 'Manifest: raw.inventory_movements\nSource: Hive Parquet external table on acme-lake\nMulti-column INT partition (year/month/day) -> generated column in BQ',
        'source': {
            'database': 'raw', 'table': 'inventory_movements', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/raw/inventory_movements/',
            'partition_cols': ['year', 'month', 'day'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'inventory_movements'},
        'transforms': {
            'partition_key_conversion': {
                'source_cols': ['year', 'month', 'day'], 'target_col': 'partition_date',
                'parse_fn': 'DATE', 'generated_column': True,
            },
            'map_to_json': [],
            'type_widening': [
                {'col': 'movement_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'quantity', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': None, 'null_check_cols': ['movement_id', 'sku']},
    },
    {
        '_comment': 'Manifest: raw.customer_signups\nSource: Hive Avro external table on acme-lake (schema-evolved)',
        'source': {
            'database': 'raw', 'table': 'customer_signups', 'cluster': 'acme-lake',
            'format': 'avro',
            'gcs_path': 'gs://acme-migration-staging-us/raw/customer_signups/',
            'partition_cols': ['signup_date'],
            'format_options': {'avro_schema_url': 'hdfs:///user/etl/schemas/customer_signups-v3.avsc'},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'customer_signups'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'signup_date', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'signup_date', 'null_check_cols': ['customer_id']},
    },
    {
        '_comment': 'Manifest: raw.loyalty_events\nSource: Hive RegexSerDe external table on acme-lake\nSpecial handling: spark.read.text() + regexp_extract() per column',
        'source': {
            'database': 'raw', 'table': 'loyalty_events', 'cluster': 'acme-lake',
            'format': 'regex_serde',
            'gcs_path': 'gs://acme-migration-staging-us/raw/loyalty_events/',
            'partition_cols': ['date_ts'],
            'format_options': {
                'regex_pattern': r'^([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|([^|]+)\|TX:([^;]+);META:(.*)$',
                'regex_columns': ['event_ts_str', 'member_id', 'event_type', 'points', 'store_id', 'tx_id', 'meta_raw'],
            },
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'loyalty_events'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['member_id', 'tx_id']},
    },
    {
        '_comment': 'Manifest: raw.product_catalog_feed\nSource: Hive RCFile external table on acme-lake\nSpecial handling: spark.read.format("hive") via HiveContext with SerDe JARs',
        'source': {
            'database': 'raw', 'table': 'product_catalog_feed', 'cluster': 'acme-lake',
            'format': 'rcfile',
            'gcs_path': 'gs://acme-migration-staging-us/raw/product_catalog_feed/',
            'partition_cols': ['feed_date'],
            'format_options': {'hive_serde': True},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'product_catalog_feed'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'feed_date', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [{'source_col': 'metadata', 'target_col': 'metadata'}],
            'type_widening': [],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'feed_date', 'null_check_cols': ['sku', 'supplier_id']},
    },
    {
        '_comment': 'Manifest: raw.supplier_invoices\nSource: Hive SequenceFile external table on acme-lake\nSpecial handling: sc.sequenceFile() custom Hadoop InputFormat reader\nMulti-column INT partition (feed_year/feed_month) -> generated column in BQ',
        'source': {
            'database': 'raw', 'table': 'supplier_invoices', 'cluster': 'acme-lake',
            'format': 'sequencefile',
            'gcs_path': 'gs://acme-migration-staging-us/raw/supplier_invoices/',
            'partition_cols': ['feed_year', 'feed_month'],
            'format_options': {'key_field': 'invoice_no', 'value_format': 'serialized_row'},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'supplier_invoices'},
        'transforms': {
            'partition_key_conversion': {
                'source_cols': ['feed_year', 'feed_month'], 'target_col': 'partition_month',
                'parse_fn': 'DATE', 'generated_column': True,
            },
            'map_to_json': [],
            'type_widening': [],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': None, 'null_check_cols': ['invoice_no', 'supplier_id']},
    },
    {
        '_comment': 'Manifest: raw.email_campaign_clicks\nSource: Hive JSON SerDe external table on acme-lake',
        'source': {
            'database': 'raw', 'table': 'email_campaign_clicks', 'cluster': 'acme-lake',
            'format': 'json_serde',
            'gcs_path': 'gs://acme-migration-staging-us/raw/email_campaign_clicks/',
            'partition_cols': ['date_ts'],
            'format_options': {'ndjson': True},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'email_campaign_clicks'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [{'source_col': 'utm', 'target_col': 'utm'}],
            'type_widening': [],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['campaign_id', 'send_id']},
    },
    {
        '_comment': 'Manifest: raw.shipment_tracking\nSource: Hive CSV external table on acme-lake',
        'source': {
            'database': 'raw', 'table': 'shipment_tracking', 'cluster': 'acme-lake',
            'format': 'textfile_csv',
            'gcs_path': 'gs://acme-migration-staging-us/raw/shipment_tracking/',
            'partition_cols': ['date_ts', 'carrier_partition'],
            'format_options': {'delimiter': ',', 'header': True},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'shipment_tracking'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['tracking_no', 'invoice_no']},
    },
    {
        '_comment': 'Manifest: raw.return_authorizations\nSource: Hive TSV external table on acme-lake',
        'source': {
            'database': 'raw', 'table': 'return_authorizations', 'cluster': 'acme-lake',
            'format': 'textfile_csv',
            'gcs_path': 'gs://acme-migration-staging-us/raw/return_authorizations/',
            'partition_cols': ['date_ts'],
            'format_options': {'delimiter': '\t', 'header': False},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'return_authorizations'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [{'col': 'quantity', 'from': 'INT', 'to': 'INT64'}],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['rma_id', 'invoice_no']},
    },
    {
        '_comment': 'Manifest: raw.fraud_signals\nSource: Hive Avro external table on acme-lake (schema evolves quarterly)',
        'source': {
            'database': 'raw', 'table': 'fraud_signals', 'cluster': 'acme-lake',
            'format': 'avro',
            'gcs_path': 'gs://acme-migration-staging-us/raw/fraud_signals/',
            'partition_cols': ['signal_date'],
            'format_options': {'avro_schema_url': 'hdfs:///user/etl/schemas/fraud_signals-v5.avsc'},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'fraud_signals'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'signal_date', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'signal_date', 'null_check_cols': ['customer_id', 'signal_type']},
    },
    {
        '_comment': 'Manifest: raw.warehouse_picks\nSource: Hive Parquet external table on acme-lake',
        'source': {
            'database': 'raw', 'table': 'warehouse_picks', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/raw/warehouse_picks/',
            'partition_cols': ['date_ts', 'warehouse_id_partition'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'warehouse_picks'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [
                {'col': 'pick_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'quantity', 'from': 'INT', 'to': 'INT64'},
                {'col': 'duration_ms', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['pick_id', 'sku']},
    },
    {
        '_comment': 'Manifest: raw.delivery_routes\nSource: Hive CSV external table on acme-lake',
        'source': {
            'database': 'raw', 'table': 'delivery_routes', 'cluster': 'acme-lake',
            'format': 'textfile_csv',
            'gcs_path': 'gs://acme-migration-staging-us/raw/delivery_routes/',
            'partition_cols': ['date_ts'],
            'format_options': {'delimiter': ',', 'header': True},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'delivery_routes'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [
                {'col': 'planned_stops', 'from': 'INT', 'to': 'INT64'},
                {'col': 'actual_stops', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['route_id', 'driver_id']},
    },
    {
        '_comment': 'Manifest: raw.driver_logs\nSource: Hive JSON SerDe external table on acme-lake\nMAP<STRING,STRING> extras -> JSON; STRUCT gps passthrough',
        'source': {
            'database': 'raw', 'table': 'driver_logs', 'cluster': 'acme-lake',
            'format': 'json_serde',
            'gcs_path': 'gs://acme-migration-staging-us/raw/driver_logs/',
            'partition_cols': ['date_ts'],
            'format_options': {'ndjson': True},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'driver_logs'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [{'source_col': 'extras', 'target_col': 'extras'}],
            'type_widening': [],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['driver_id']},
    },
    {
        '_comment': 'Manifest: raw.customer_complaints\nSource: Hive TSV external table on acme-lake',
        'source': {
            'database': 'raw', 'table': 'customer_complaints', 'cluster': 'acme-lake',
            'format': 'textfile_csv',
            'gcs_path': 'gs://acme-migration-staging-us/raw/customer_complaints/',
            'partition_cols': ['date_ts'],
            'format_options': {'delimiter': '\t', 'header': False},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'customer_complaints'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [{'col': 'csat_score', 'from': 'INT', 'to': 'INT64'}],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['complaint_id', 'customer_id']},
    },
    {
        '_comment': 'Manifest: raw.chat_transcripts\nSource: Hive TSV external table on acme-lake',
        'source': {
            'database': 'raw', 'table': 'chat_transcripts', 'cluster': 'acme-lake',
            'format': 'textfile_csv',
            'gcs_path': 'gs://acme-migration-staging-us/raw/chat_transcripts/',
            'partition_cols': ['date_ts'],
            'format_options': {'delimiter': '\t', 'header': False},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'chat_transcripts'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [
                {'col': 'duration_sec', 'from': 'INT', 'to': 'INT64'},
                {'col': 'message_count', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['chat_id', 'customer_id']},
    },
    {
        '_comment': 'Manifest: raw.mobile_events\nSource: Hive JSON SerDe external table on acme-lake\nMAP->JSON, STRUCT passthrough, ARRAY<STRUCT> passthrough\nGenerated column: partition_date DATE AS (PARSE_DATE("%Y%m%d", event_date))',
        'source': {
            'database': 'raw', 'table': 'mobile_events', 'cluster': 'acme-lake',
            'format': 'json_serde',
            'gcs_path': 'gs://acme-migration-staging-us/raw/mobile_events/',
            'partition_cols': ['event_date', 'hour_bucket'],
            'format_options': {'ndjson': True},
        },
        'target': {'project': 'acme-analytics', 'dataset': 'raw', 'table': 'mobile_events'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'event_date', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': True,
            },
            'map_to_json': [{'source_col': 'properties', 'target_col': 'properties'}],
            'type_widening': [{'col': 'hour_bucket', 'from': 'TINYINT', 'to': 'INT64'}],
        },
        'wave': 'wave_3_large',
        'validation': {'watermark_col': 'event_date', 'null_check_cols': ['event_id', 'user_id']},
    },
]

# ============================================================================
# STAGING DATABASE — 10 tables on acme-lake cluster
# ============================================================================
STAGING_TABLES = [
    {
        '_comment': 'Manifest: staging.cleansed_orders',
        'source': {
            'database': 'staging', 'table': 'cleansed_orders', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/staging/cleansed_orders/',
            'partition_cols': ['order_date'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'staging', 'table': 'cleansed_orders'},
        'transforms': {
            'partition_key_conversion': None,
            'map_to_json': [],
            'type_widening': [{'col': 'line_count', 'from': 'INT', 'to': 'INT64'}],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'order_date', 'null_check_cols': ['order_id', 'invoice_no']},
    },
    {
        '_comment': 'Manifest: staging.cleansed_customers',
        'source': {
            'database': 'staging', 'table': 'cleansed_customers', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/staging/cleansed_customers/',
            'partition_cols': ['load_date'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'staging', 'table': 'cleansed_customers'},
        'transforms': {'partition_key_conversion': None, 'map_to_json': [], 'type_widening': []},
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'load_date', 'null_check_cols': ['customer_id']},
    },
    {
        '_comment': 'Manifest: staging.cleansed_products',
        'source': {
            'database': 'staging', 'table': 'cleansed_products', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/staging/cleansed_products/',
            'partition_cols': ['load_date'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'staging', 'table': 'cleansed_products'},
        'transforms': {'partition_key_conversion': None, 'map_to_json': [], 'type_widening': []},
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'load_date', 'null_check_cols': ['sku']},
    },
    {
        '_comment': 'Manifest: staging.dedup_clickstream\nSTRING partition date_ts + country_partition -> partition_date DATE',
        'source': {
            'database': 'staging', 'table': 'dedup_clickstream', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/staging/dedup_clickstream/',
            'partition_cols': ['date_ts', 'country_partition'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'staging', 'table': 'dedup_clickstream'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['session_id', 'user_id']},
    },
    {
        '_comment': 'Manifest: staging.geocoded_addresses',
        'source': {
            'database': 'staging', 'table': 'geocoded_addresses', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/staging/geocoded_addresses/',
            'partition_cols': ['load_date'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'staging', 'table': 'geocoded_addresses'},
        'transforms': {'partition_key_conversion': None, 'map_to_json': [], 'type_widening': []},
        'wave': 'wave_1_small',
        'validation': {'watermark_col': 'load_date', 'null_check_cols': ['raw_addr_hash']},
    },
    {
        '_comment': 'Manifest: staging.parsed_loyalty_events\nMAP<STRING,STRING> meta -> JSON',
        'source': {
            'database': 'staging', 'table': 'parsed_loyalty_events', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/staging/parsed_loyalty_events/',
            'partition_cols': ['date_ts'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'staging', 'table': 'parsed_loyalty_events'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [{'source_col': 'meta', 'target_col': 'meta'}],
            'type_widening': [{'col': 'points', 'from': 'INT', 'to': 'INT64'}],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['member_id', 'tx_id']},
    },
    {
        '_comment': 'Manifest: staging.merged_returns_cdc',
        'source': {
            'database': 'staging', 'table': 'merged_returns_cdc', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/staging/merged_returns_cdc/',
            'partition_cols': ['snapshot_date'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'staging', 'table': 'merged_returns_cdc'},
        'transforms': {
            'partition_key_conversion': None,
            'map_to_json': [],
            'type_widening': [
                {'col': 'return_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'snapshot_date', 'null_check_cols': ['return_id', 'invoice_no']},
    },
    {
        '_comment': 'Manifest: staging.normalized_carrier_events',
        'source': {
            'database': 'staging', 'table': 'normalized_carrier_events', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/staging/normalized_carrier_events/',
            'partition_cols': ['date_ts'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'staging', 'table': 'normalized_carrier_events'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['tracking_no', 'carrier']},
    },
    {
        '_comment': 'Manifest: staging.fraud_scored',
        'source': {
            'database': 'staging', 'table': 'fraud_scored', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/staging/fraud_scored/',
            'partition_cols': ['score_date'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'staging', 'table': 'fraud_scored'},
        'transforms': {
            'partition_key_conversion': None,
            'map_to_json': [],
            'type_widening': [{'col': 'txn_id', 'from': 'BIGINT', 'to': 'INT64'}],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'score_date', 'null_check_cols': ['txn_id', 'customer_id']},
    },
    {
        '_comment': 'Manifest: staging.warehouse_kpi_snapshot',
        'source': {
            'database': 'staging', 'table': 'warehouse_kpi_snapshot', 'cluster': 'acme-lake',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/staging/warehouse_kpi_snapshot/',
            'partition_cols': ['date_ts'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'staging', 'table': 'warehouse_kpi_snapshot'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'date_ts', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [],
            'type_widening': [
                {'col': 'units_in', 'from': 'INT', 'to': 'INT64'},
                {'col': 'units_picked', 'from': 'INT', 'to': 'INT64'},
                {'col': 'units_shipped', 'from': 'INT', 'to': 'INT64'},
                {'col': 'backlog_units', 'from': 'INT', 'to': 'INT64'},
                {'col': 'avg_pick_ms', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': 'date_ts', 'null_check_cols': ['warehouse_id']},
    },
]


def dim(table, extra_widening=None, **kwargs):
    """Helper for standard Parquet dimension tables on acme-analytics."""
    return {
        '_comment': f'Manifest: retail.{table}',
        'source': {
            'database': 'retail', 'table': table, 'cluster': 'acme-analytics',
            'format': 'parquet',
            'gcs_path': f'gs://acme-migration-staging-us/retail/{table}/',
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': table},
        'transforms': {
            'partition_key_conversion': None,
            'map_to_json': kwargs.get('map_to_json', []),
            'type_widening': extra_widening or [],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': None, 'null_check_cols': kwargs.get('null_check_cols', [])},
    }


def fact(table, partition_col, wave='wave_2_medium', extra_widening=None,
         partition_key_conversion=None, map_to_json=None, null_check_cols=None,
         watermark_col=None, partition_cols=None, **kwargs):
    """Helper for standard Parquet fact tables on acme-analytics."""
    src = {
        'database': 'retail', 'table': table, 'cluster': 'acme-analytics',
        'format': 'parquet',
        'gcs_path': f'gs://acme-migration-staging-us/retail/{table}/',
    }
    if partition_cols:
        src['partition_cols'] = partition_cols
    elif partition_col:
        src['partition_cols'] = [partition_col]
    return {
        '_comment': f'Manifest: retail.{table}',
        'source': src,
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': table},
        'transforms': {
            'partition_key_conversion': partition_key_conversion,
            'map_to_json': map_to_json or [],
            'type_widening': extra_widening or [],
        },
        'wave': wave,
        'validation': {
            'watermark_col': watermark_col or partition_col,
            'null_check_cols': null_check_cols or [],
        },
    }


# ============================================================================
# RETAIL DATABASE — 58 tables on acme-analytics cluster
# ============================================================================
RETAIL_TABLES = [
    # --- Dimensions (14) ---
    dim('dim_date', [
        {'col': 'd_date_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'd_month_seq', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_week_seq', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_quarter_seq', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_year', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_dow', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_moy', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_dom', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_qoy', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_fy_year', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_fy_quarter_seq', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_first_dom', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_last_dom', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_same_day_ly', 'from': 'INT', 'to': 'INT64'},
        {'col': 'd_same_day_lq', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['d_date_sk', 'd_date']),
    dim('dim_customer', [{'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'}],
        null_check_cols=['customer_sk', 'customer_id']),
    dim('dim_product', [{'col': 'product_sk', 'from': 'BIGINT', 'to': 'INT64'}],
        null_check_cols=['product_sk', 'stock_code']),
    dim('dim_store', [
        {'col': 'store_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'sq_ft', 'from': 'INT', 'to': 'INT64'},
        {'col': 'manager_employee_sk', 'from': 'BIGINT', 'to': 'INT64'},
    ], map_to_json=[{'source_col': 'attributes', 'target_col': 'attributes'}],
       null_check_cols=['store_sk', 'store_id']),
    dim('dim_supplier', [
        {'col': 'supplier_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'payment_terms_days', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['supplier_sk', 'supplier_id']),
    dim('dim_employee', [
        {'col': 'employee_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'home_store_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'manager_sk', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['employee_sk', 'employee_id']),
    dim('dim_promotion', [{'col': 'promo_sk', 'from': 'BIGINT', 'to': 'INT64'}],
        map_to_json=[{'source_col': 'eligibility', 'target_col': 'eligibility'}],
        null_check_cols=['promo_sk', 'promo_id']),
    dim('dim_warehouse', [
        {'col': 'warehouse_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'capacity_units', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['warehouse_sk', 'warehouse_id']),
    dim('dim_currency', [{'col': 'minor_unit', 'from': 'INT', 'to': 'INT64'}],
        null_check_cols=['currency_code']),
    dim('dim_geography', [{'col': 'geo_sk', 'from': 'BIGINT', 'to': 'INT64'}],
        null_check_cols=['geo_sk', 'country_iso2']),
    dim('dim_color', [{'col': 'color_sk', 'from': 'BIGINT', 'to': 'INT64'}],
        null_check_cols=['color_sk', 'color_code']),
    dim('dim_size', [
        {'col': 'size_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'sort_order', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['size_sk', 'size_code']),
    dim('dim_brand', [{'col': 'brand_sk', 'from': 'BIGINT', 'to': 'INT64'}],
        null_check_cols=['brand_sk', 'brand_id']),
    dim('dim_category', [
        {'col': 'category_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'depth', 'from': 'INT', 'to': 'INT64'},
        {'col': 'sort_order', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['category_sk', 'category_id']),
    dim('dim_payment_method', [
        {'col': 'payment_method_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'settlement_days', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['payment_method_sk', 'method_code']),
    dim('dim_store_history', [
        {'col': 'history_id', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'store_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'manager_employee_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'sq_ft', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['history_id', 'store_sk']),
    # dim_employee_history — partitioned by eff_from_year INT (no DATE partition in BQ)
    {
        '_comment': 'Manifest: retail.dim_employee_history\nPartitioned by eff_from_year INT — no date partition in BQ',
        'source': {
            'database': 'retail', 'table': 'dim_employee_history', 'cluster': 'acme-analytics',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/retail/dim_employee_history/',
            'partition_cols': ['eff_from_year'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'dim_employee_history'},
        'transforms': {
            'partition_key_conversion': None,
            'map_to_json': [],
            'type_widening': [
                {'col': 'history_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'employee_sk', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'home_store_sk', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'eff_from_year', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': None, 'null_check_cols': ['history_id', 'employee_sk']},
    },
    # --- Standard fact tables ---
    fact('fact_sales', 'sale_date', 'wave_3_large', [
        {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'product_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'quantity', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['invoice_no', 'customer_sk', 'product_sk']),
    fact('fact_web_session', 'event_date', 'wave_2_medium',
         partition_cols=['event_date', 'country'], null_check_cols=['user_id']),
    fact('fact_returns', 'return_date', 'wave_2_medium', [
        {'col': 'return_id', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'product_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'quantity', 'from': 'INT', 'to': 'INT64'},
        {'col': 'store_sk', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['return_id', 'invoice_no']),
    fact('fact_refunds', 'refund_date', 'wave_2_medium', [
        {'col': 'refund_id', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'payment_id', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'return_id', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['refund_id', 'payment_id']),
    fact('fact_chat_interactions', 'start_date', 'wave_2_medium', [
        {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'agent_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'duration_sec', 'from': 'INT', 'to': 'INT64'},
        {'col': 'message_count', 'from': 'INT', 'to': 'INT64'},
        {'col': 'csat_score', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['chat_id', 'customer_sk']),
    fact('fact_customer_complaints', 'created_date', 'wave_1_small', [
        {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'csat_score', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['complaint_id', 'customer_sk']),
    fact('fact_promo_redemptions', 'redemption_date', 'wave_2_medium', [
        {'col': 'redemption_id', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'promo_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['redemption_id', 'promo_sk']),
    fact('fact_fraud_decisions', 'decision_date', 'wave_2_medium', [
        {'col': 'txn_id', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['txn_id', 'customer_sk']),
    fact('fact_email_engagement', 'event_date', 'wave_2_medium', [
        {'col': 'campaign_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'user_sk', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['send_id', 'campaign_sk']),
    # fact_app_clicks — MAP->JSON properties
    fact('fact_app_clicks', 'event_date', 'wave_2_medium', [
        {'col': 'user_sk', 'from': 'BIGINT', 'to': 'INT64'},
    ], map_to_json=[{'source_col': 'properties', 'target_col': 'properties'}],
       null_check_cols=['session_id', 'user_sk'],
       partition_cols=['event_date', 'platform_partition']),
    # fact_loyalty_events — MAP->JSON meta
    fact('fact_loyalty_events', 'event_date', 'wave_2_medium', [
        {'col': 'event_id', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'points', 'from': 'INT', 'to': 'INT64'},
        {'col': 'store_sk', 'from': 'BIGINT', 'to': 'INT64'},
    ], map_to_json=[{'source_col': 'meta', 'target_col': 'meta'}],
       null_check_cols=['event_id', 'member_id']),
    # fact_warehouse_picks
    fact('fact_warehouse_picks', 'pick_date', 'wave_2_medium', [
        {'col': 'pick_id', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'warehouse_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'picker_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'quantity', 'from': 'INT', 'to': 'INT64'},
        {'col': 'duration_ms', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['pick_id', 'sku'],
       partition_cols=['pick_date', 'warehouse_partition']),
    # fact_inventory_snapshot
    fact('fact_inventory_snapshot', 'snapshot_date', 'wave_2_medium', [
        {'col': 'warehouse_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'on_hand_units', 'from': 'INT', 'to': 'INT64'},
        {'col': 'allocated_units', 'from': 'INT', 'to': 'INT64'},
        {'col': 'in_transit_units', 'from': 'INT', 'to': 'INT64'},
        {'col': 'available_units', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['sku', 'warehouse_sk']),
    # --- Multi-col partition facts with generated columns ---
    {
        '_comment': 'Manifest: retail.fact_inventory_movements\nMulti-col partition: year/month/day INT + region STRING -> generated column\nCRITICAL tier',
        'source': {
            'database': 'retail', 'table': 'fact_inventory_movements', 'cluster': 'acme-analytics',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/retail/fact_inventory_movements/',
            'partition_cols': ['year', 'month', 'day', 'region'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'fact_inventory_movements'},
        'transforms': {
            'partition_key_conversion': {
                'source_cols': ['year', 'month', 'day'], 'target_col': 'partition_date',
                'parse_fn': 'DATE', 'generated_column': True,
            },
            'map_to_json': [],
            'type_widening': [
                {'col': 'movement_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'warehouse_sk', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'store_sk', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'quantity', 'from': 'INT', 'to': 'INT64'},
                {'col': 'operator_sk', 'from': 'BIGINT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_3_large',
        'validation': {'watermark_col': None, 'null_check_cols': ['movement_id', 'sku']},
    },
    {
        '_comment': 'Manifest: retail.fact_payments\nMulti-col partition: post_year/post_month INT + payment_method_partition STRING\nGenerated column: partition_month DATE AS (DATE(post_year, post_month, 1))\nCRITICAL tier',
        'source': {
            'database': 'retail', 'table': 'fact_payments', 'cluster': 'acme-analytics',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/retail/fact_payments/',
            'partition_cols': ['post_year', 'post_month', 'payment_method_partition'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'fact_payments'},
        'transforms': {
            'partition_key_conversion': {
                'source_cols': ['post_year', 'post_month'], 'target_col': 'partition_month',
                'parse_fn': 'DATE', 'generated_column': True,
            },
            'map_to_json': [],
            'type_widening': [
                {'col': 'payment_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'payment_method_sk', 'from': 'BIGINT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_3_large',
        'validation': {'watermark_col': None, 'null_check_cols': ['payment_id', 'invoice_no']},
    },
    {
        '_comment': 'Manifest: retail.fact_shipments\nMulti-col partition with generated column + ARRAY<STRUCT> tracking_events passthrough\nCRITICAL tier',
        'source': {
            'database': 'retail', 'table': 'fact_shipments', 'cluster': 'acme-analytics',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/retail/fact_shipments/',
            'partition_cols': ['ship_year', 'ship_month', 'ship_day', 'carrier_partition'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'fact_shipments'},
        'transforms': {
            'partition_key_conversion': {
                'source_cols': ['ship_year', 'ship_month', 'ship_day'], 'target_col': 'partition_date',
                'parse_fn': 'DATE', 'generated_column': True,
            },
            'map_to_json': [],
            'type_widening': [
                {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'warehouse_sk', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'sla_hours', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_3_large',
        'validation': {'watermark_col': None, 'null_check_cols': ['shipment_id', 'invoice_no']},
    },
    {
        '_comment': 'Manifest: retail.fact_supplier_invoice_lines\nMulti-col partition: invoice_year/invoice_month -> generated column',
        'source': {
            'database': 'retail', 'table': 'fact_supplier_invoice_lines', 'cluster': 'acme-analytics',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/retail/fact_supplier_invoice_lines/',
            'partition_cols': ['invoice_year', 'invoice_month'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'fact_supplier_invoice_lines'},
        'transforms': {
            'partition_key_conversion': {
                'source_cols': ['invoice_year', 'invoice_month'], 'target_col': 'partition_month',
                'parse_fn': 'DATE', 'generated_column': True,
            },
            'map_to_json': [],
            'type_widening': [
                {'col': 'invoice_line_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'supplier_sk', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'quantity', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_2_medium',
        'validation': {'watermark_col': None, 'null_check_cols': ['invoice_line_id', 'invoice_no']},
    },
    # --- ACID tables (5) ---
    {
        '_comment': 'Manifest: retail.returns_ledger\nHive ACID/ORC — requires major compaction before DistCp\nCRITICAL tier',
        'source': {
            'database': 'retail', 'table': 'returns_ledger', 'cluster': 'acme-analytics',
            'format': 'orc', 'gcs_path': 'gs://acme-migration-staging-us/retail/returns_ledger/',
            'acid_compaction': True,
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'returns_ledger'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [
                {'col': 'return_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_3_large',
        'validation': {'watermark_col': None, 'null_check_cols': ['return_id', 'invoice_no']},
    },
    {
        '_comment': 'Manifest: retail.acid_customer_address_history\nHive ACID/ORC — SCD-2 customer address tracking',
        'source': {
            'database': 'retail', 'table': 'acid_customer_address_history', 'cluster': 'acme-analytics',
            'format': 'orc', 'gcs_path': 'gs://acme-migration-staging-us/retail/acid_customer_address_history/',
            'acid_compaction': True,
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'acid_customer_address_history'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [
                {'col': 'history_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_3_large',
        'validation': {'watermark_col': None, 'null_check_cols': ['history_id', 'customer_sk']},
    },
    {
        '_comment': 'Manifest: retail.acid_supplier_terms_history\nHive ACID/ORC — SCD-2 supplier payment-terms',
        'source': {
            'database': 'retail', 'table': 'acid_supplier_terms_history', 'cluster': 'acme-analytics',
            'format': 'orc', 'gcs_path': 'gs://acme-migration-staging-us/retail/acid_supplier_terms_history/',
            'acid_compaction': True,
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'acid_supplier_terms_history'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [
                {'col': 'history_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'supplier_sk', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'payment_terms_days', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_3_large',
        'validation': {'watermark_col': None, 'null_check_cols': ['history_id', 'supplier_sk']},
    },
    {
        '_comment': 'Manifest: retail.acid_loyalty_points_ledger\nHive ACID/ORC — live earn/redeem ledger',
        'source': {
            'database': 'retail', 'table': 'acid_loyalty_points_ledger', 'cluster': 'acme-analytics',
            'format': 'orc', 'gcs_path': 'gs://acme-migration-staging-us/retail/acid_loyalty_points_ledger/',
            'acid_compaction': True,
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'acid_loyalty_points_ledger'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [
                {'col': 'entry_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'points_delta', 'from': 'INT', 'to': 'INT64'},
                {'col': 'running_balance', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_3_large',
        'validation': {'watermark_col': None, 'null_check_cols': ['entry_id', 'member_id']},
    },
    {
        '_comment': 'Manifest: retail.acid_inventory_adjustments_log\nHive ACID/ORC — audit-mandatory inventory adjustments',
        'source': {
            'database': 'retail', 'table': 'acid_inventory_adjustments_log', 'cluster': 'acme-analytics',
            'format': 'orc', 'gcs_path': 'gs://acme-migration-staging-us/retail/acid_inventory_adjustments_log/',
            'acid_compaction': True,
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'acid_inventory_adjustments_log'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [
                {'col': 'adjustment_id', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'warehouse_sk', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'quantity_delta', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_3_large',
        'validation': {'watermark_col': None, 'null_check_cols': ['adjustment_id', 'sku']},
    },
    # --- Kudu snapshot tables (4) ---
    {
        '_comment': 'Manifest: retail.inventory_realtime_snapshot\nSource: kudu_inventory_realtime — epoch ms BIGINT -> TIMESTAMP',
        'source': {
            'database': 'retail', 'table': 'kudu_inventory_realtime', 'cluster': 'acme-analytics',
            'format': 'kudu', 'gcs_path': 'gs://acme-migration-staging-us/retail/kudu_inventory_realtime/',
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'inventory_realtime_snapshot'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [
                {'col': 'on_hand', 'from': 'INT', 'to': 'INT64'},
                {'col': 'allocated', 'from': 'INT', 'to': 'INT64'},
                {'col': 'available', 'from': 'INT', 'to': 'INT64'},
            ],
            'kudu_epoch_conversion': [{'col': 'last_updated_ts', 'from_unit': 'millis'}],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': None, 'null_check_cols': ['warehouse_id', 'sku']},
    },
    {
        '_comment': 'Manifest: retail.session_state_snapshot\nSource: kudu_session_state — epoch ms BIGINT -> TIMESTAMP',
        'source': {
            'database': 'retail', 'table': 'kudu_session_state', 'cluster': 'acme-analytics',
            'format': 'kudu', 'gcs_path': 'gs://acme-migration-staging-us/retail/kudu_session_state/',
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'session_state_snapshot'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [{'col': 'cart_items', 'from': 'INT', 'to': 'INT64'}],
            'kudu_epoch_conversion': [
                {'col': 'started_ts', 'from_unit': 'millis'},
                {'col': 'last_event_ts', 'from_unit': 'millis'},
            ],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': None, 'null_check_cols': ['session_id']},
    },
    {
        '_comment': 'Manifest: retail.promo_eligibility_snapshot\nSource: kudu_promo_eligibility — epoch ms BIGINT -> TIMESTAMP',
        'source': {
            'database': 'retail', 'table': 'kudu_promo_eligibility', 'cluster': 'acme-analytics',
            'format': 'kudu', 'gcs_path': 'gs://acme-migration-staging-us/retail/kudu_promo_eligibility/',
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'promo_eligibility_snapshot'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [], 'type_widening': [],
            'kudu_epoch_conversion': [
                {'col': 'valid_from_ts', 'from_unit': 'millis'},
                {'col': 'valid_to_ts', 'from_unit': 'millis'},
            ],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': None, 'null_check_cols': ['customer_id', 'promo_id']},
    },
    {
        '_comment': 'Manifest: retail.realtime_price_snapshot\nSource: kudu_realtime_price — epoch ms BIGINT -> TIMESTAMP',
        'source': {
            'database': 'retail', 'table': 'kudu_realtime_price', 'cluster': 'acme-analytics',
            'format': 'kudu', 'gcs_path': 'gs://acme-migration-staging-us/retail/kudu_realtime_price/',
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'realtime_price_snapshot'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [], 'type_widening': [],
            'kudu_epoch_conversion': [{'col': 'updated_ts', 'from_unit': 'millis'}],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': None, 'null_check_cols': ['sku', 'store_id']},
    },
    # --- Aggregate tables (8) ---
    fact('agg_daily_sales_by_store', 'sale_date', 'wave_1_small', [
        {'col': 'store_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'units_sold', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'txn_count', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['store_sk']),
    fact('agg_daily_sales_by_product', 'sale_date', 'wave_1_small', [
        {'col': 'product_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'units_sold', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'return_units', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'net_units', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['product_sk']),
    fact('agg_weekly_customer_ltv', 'week_start_date', 'wave_1_small', [
        {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'orders_to_date', 'from': 'INT', 'to': 'INT64'},
        {'col': 'days_since_last_order', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['customer_sk']),
    fact('agg_monthly_supplier_performance', 'month_start', 'wave_1_small', [
        {'col': 'supplier_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'orders_placed', 'from': 'INT', 'to': 'INT64'},
        {'col': 'units_received', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['supplier_sk']),
    # agg_hourly_warehouse_kpi — snapshot_hour STRING, no DATE partition in BQ
    {
        '_comment': 'Manifest: retail.agg_hourly_warehouse_kpi\nPartitioned by snapshot_hour STRING — no date partition in BQ',
        'source': {
            'database': 'retail', 'table': 'agg_hourly_warehouse_kpi', 'cluster': 'acme-analytics',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/retail/agg_hourly_warehouse_kpi/',
            'partition_cols': ['snapshot_hour'],
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'agg_hourly_warehouse_kpi'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [
                {'col': 'warehouse_sk', 'from': 'BIGINT', 'to': 'INT64'},
                {'col': 'units_in', 'from': 'INT', 'to': 'INT64'},
                {'col': 'units_picked', 'from': 'INT', 'to': 'INT64'},
                {'col': 'units_shipped', 'from': 'INT', 'to': 'INT64'},
                {'col': 'backlog_units', 'from': 'INT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': None, 'null_check_cols': ['warehouse_sk']},
    },
    fact('agg_daily_carrier_otd', 'ship_date', 'wave_1_small', [
        {'col': 'shipments_total', 'from': 'INT', 'to': 'INT64'},
        {'col': 'delivered_on_time', 'from': 'INT', 'to': 'INT64'},
        {'col': 'delivered_late', 'from': 'INT', 'to': 'INT64'},
        {'col': 'in_transit', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['carrier']),
    fact('agg_marketing_attribution_cube', 'period_date', 'wave_1_small', [
        {'col': 'campaign_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'attributed_units', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'grouping_id', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=[]),
    fact('agg_returns_by_reason_monthly', 'month_start', 'wave_1_small', [
        {'col': 'return_count', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'return_units', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['reason_code']),
    # --- sales_cube and top_countries_daily ---
    fact('sales_cube', 'as_of_date', 'wave_1_small', [
        {'col': 'dim_level', 'from': 'TINYINT', 'to': 'INT64'},
        {'col': 'month_key', 'from': 'SMALLINT', 'to': 'INT64'},
        {'col': 'product_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'orders', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'units', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=[]),
    # top_countries_daily — non-partitioned
    {
        '_comment': 'Manifest: retail.top_countries_daily\nNon-partitioned — TINYINT rank -> INT64',
        'source': {
            'database': 'retail', 'table': 'top_countries_daily', 'cluster': 'acme-analytics',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-us/retail/top_countries_daily/',
        },
        'target': {'project': 'acme-analytics', 'dataset': 'retail', 'table': 'top_countries_daily'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [
                {'col': 'rank', 'from': 'TINYINT', 'to': 'INT64'},
                {'col': 'orders', 'from': 'BIGINT', 'to': 'INT64'},
            ],
        },
        'wave': 'wave_1_small',
        'validation': {'watermark_col': None, 'null_check_cols': ['country']},
    },
    # --- Bridge tables (5) ---
    dim('bridge_product_attribute', [
        {'col': 'product_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'sort_order', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['product_sk', 'attribute_name']),
    dim('bridge_product_supplier', [
        {'col': 'product_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'supplier_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'lead_time_days', 'from': 'INT', 'to': 'INT64'},
        {'col': 'moq', 'from': 'INT', 'to': 'INT64'},
    ], null_check_cols=['product_sk', 'supplier_sk']),
    fact('bridge_customer_segment', 'snapshot_date', 'wave_1_small', [
        {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['customer_sk', 'segment_id']),
    fact('bridge_promo_eligibility', 'load_date', 'wave_1_small', [
        {'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'},
        {'col': 'promo_sk', 'from': 'BIGINT', 'to': 'INT64'},
    ], null_check_cols=['customer_sk', 'promo_sk']),
    dim('bridge_employee_role', [{'col': 'employee_sk', 'from': 'BIGINT', 'to': 'INT64'}],
        null_check_cols=['employee_sk', 'role']),
]

# ============================================================================
# REGIONAL DATABASE — 13 tables on acme-edge cluster
# ============================================================================
REGIONAL_TABLES = [
    {
        '_comment': 'Manifest: regional.events_eu\nevent_date STRING partition -> partition_date DATE (Spark populates explicitly)',
        'source': {
            'database': 'regional', 'table': 'events_eu', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/events_eu/',
            'partition_cols': ['event_date'],
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'events_eu'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'event_date', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': False,
            },
            'map_to_json': [], 'type_widening': [],
        },
        'wave': 'eu',
        'validation': {'watermark_col': 'event_date', 'null_check_cols': ['event_id', 'user_id']},
    },
    {
        '_comment': 'Manifest: regional.dim_customer_snapshot\nNon-partitioned Sqoop snapshot',
        'source': {
            'database': 'regional', 'table': 'dim_customer_snapshot', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/dim_customer_snapshot/',
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'dim_customer_snapshot'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [{'col': 'customer_sk', 'from': 'BIGINT', 'to': 'INT64'}],
        },
        'wave': 'eu',
        'validation': {'watermark_col': None, 'null_check_cols': ['customer_sk', 'customer_id']},
    },
    {
        '_comment': 'Manifest: regional.dim_currency_eu\nNon-partitioned static dimension',
        'source': {
            'database': 'regional', 'table': 'dim_currency_eu', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/dim_currency_eu/',
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'dim_currency_eu'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [{'col': 'minor_unit', 'from': 'INT', 'to': 'INT64'}],
        },
        'wave': 'eu',
        'validation': {'watermark_col': None, 'null_check_cols': ['currency_code']},
    },
    {
        '_comment': 'Manifest: regional.dim_product_eu_catalog\nNon-partitioned, ARRAY<STRING> eu_compliance_flags passthrough',
        'source': {
            'database': 'regional', 'table': 'dim_product_eu_catalog', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/dim_product_eu_catalog/',
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'dim_product_eu_catalog'},
        'transforms': {'partition_key_conversion': None, 'map_to_json': [], 'type_widening': []},
        'wave': 'eu',
        'validation': {'watermark_col': None, 'null_check_cols': ['sku']},
    },
    {
        '_comment': 'Manifest: regional.fact_orders_eu\nMulti-col partition with generated column\nCRITICAL tier',
        'source': {
            'database': 'regional', 'table': 'fact_orders_eu', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/fact_orders_eu/',
            'partition_cols': ['order_year', 'order_month', 'country_partition'],
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'fact_orders_eu'},
        'transforms': {
            'partition_key_conversion': {
                'source_cols': ['order_year', 'order_month'], 'target_col': 'partition_month',
                'parse_fn': 'DATE', 'generated_column': True,
            },
            'map_to_json': [],
            'type_widening': [{'col': 'quantity', 'from': 'INT', 'to': 'INT64'}],
        },
        'wave': 'eu',
        'validation': {'watermark_col': None, 'null_check_cols': ['order_id', 'customer_id']},
    },
    {
        '_comment': 'Manifest: regional.fact_returns_eu',
        'source': {
            'database': 'regional', 'table': 'fact_returns_eu', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/fact_returns_eu/',
            'partition_cols': ['return_date'],
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'fact_returns_eu'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [{'col': 'return_id', 'from': 'BIGINT', 'to': 'INT64'}],
        },
        'wave': 'eu',
        'validation': {'watermark_col': 'return_date', 'null_check_cols': ['return_id', 'order_id']},
    },
    {
        '_comment': 'Manifest: regional.fact_shipments_eu',
        'source': {
            'database': 'regional', 'table': 'fact_shipments_eu', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/fact_shipments_eu/',
            'partition_cols': ['ship_date'],
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'fact_shipments_eu'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [{'col': 'sla_hours', 'from': 'INT', 'to': 'INT64'}],
        },
        'wave': 'eu',
        'validation': {'watermark_col': 'ship_date', 'null_check_cols': ['shipment_id', 'order_id']},
    },
    {
        '_comment': 'Manifest: regional.dim_gdpr_consent\nGDPR compliance-critical: granted BOOLEAN must not be NULL',
        'source': {
            'database': 'regional', 'table': 'dim_gdpr_consent', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/dim_gdpr_consent/',
            'partition_cols': ['consent_date'],
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'dim_gdpr_consent'},
        'transforms': {'partition_key_conversion': None, 'map_to_json': [], 'type_widening': []},
        'wave': 'eu',
        'validation': {'watermark_col': 'consent_date', 'null_check_cols': ['consent_id', 'customer_id', 'granted']},
    },
    {
        '_comment': 'Manifest: regional.fact_mobile_app_events\nMAP->JSON properties; STRUCT device passthrough\nGenerated column: partition_date DATE AS (PARSE_DATE("%Y%m%d", event_date))\nCRITICAL tier',
        'source': {
            'database': 'regional', 'table': 'fact_mobile_app_events', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/fact_mobile_app_events/',
            'partition_cols': ['event_date', 'platform_partition'],
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'fact_mobile_app_events'},
        'transforms': {
            'partition_key_conversion': {
                'source_col': 'event_date', 'target_col': 'partition_date',
                'parse_format': '%Y%m%d', 'parse_fn': 'PARSE_DATE', 'generated_column': True,
            },
            'map_to_json': [{'source_col': 'properties', 'target_col': 'properties'}],
            'type_widening': [],
        },
        'wave': 'eu',
        'validation': {'watermark_col': 'event_date', 'null_check_cols': ['event_id', 'user_id']},
    },
    {
        '_comment': 'Manifest: regional.dim_locale_eu\nNon-partitioned static dimension',
        'source': {
            'database': 'regional', 'table': 'dim_locale_eu', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/dim_locale_eu/',
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'dim_locale_eu'},
        'transforms': {'partition_key_conversion': None, 'map_to_json': [], 'type_widening': []},
        'wave': 'eu',
        'validation': {'watermark_col': None, 'null_check_cols': ['locale_code', 'country_iso2']},
    },
    {
        '_comment': 'Manifest: regional.staging_orders_eu\nSqoop landing for EU order CDC',
        'source': {
            'database': 'regional', 'table': 'staging_orders_eu', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/staging_orders_eu/',
            'partition_cols': ['snapshot_date'],
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'staging_orders_eu'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [{'col': 'quantity', 'from': 'INT', 'to': 'INT64'}],
        },
        'wave': 'eu',
        'validation': {'watermark_col': 'snapshot_date', 'null_check_cols': ['order_id', 'customer_id']},
    },
    {
        '_comment': 'Manifest: regional.staging_customers_eu_cdc\nCDC feed from EU customer DB',
        'source': {
            'database': 'regional', 'table': 'staging_customers_eu_cdc', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/staging_customers_eu_cdc/',
            'partition_cols': ['snapshot_date'],
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'staging_customers_eu_cdc'},
        'transforms': {'partition_key_conversion': None, 'map_to_json': [], 'type_widening': []},
        'wave': 'eu',
        'validation': {'watermark_col': 'snapshot_date', 'null_check_cols': ['customer_id']},
    },
    {
        '_comment': 'Manifest: regional.fact_eu_promotions\nEU-specific promo redemption fact',
        'source': {
            'database': 'regional', 'table': 'fact_eu_promotions', 'cluster': 'acme-edge',
            'format': 'parquet',
            'gcs_path': 'gs://acme-migration-staging-eu/regional/fact_eu_promotions/',
            'partition_cols': ['redemption_date'],
        },
        'target': {'project': 'acme-analytics-eu', 'dataset': 'regional_eu', 'table': 'fact_eu_promotions'},
        'transforms': {
            'partition_key_conversion': None, 'map_to_json': [],
            'type_widening': [{'col': 'redemption_id', 'from': 'BIGINT', 'to': 'INT64'}],
        },
        'wave': 'eu',
        'validation': {'watermark_col': 'redemption_date', 'null_check_cols': ['redemption_id', 'promo_code']},
    },
]


def main():
    base = '/workspace/project/config/tables'
    
    all_tables = {
        'raw': RAW_TABLES,
        'staging': STAGING_TABLES,
        'retail': RETAIL_TABLES,
        'regional': REGIONAL_TABLES,
    }
    
    total = 0
    for db, tables in all_tables.items():
        for tbl in tables:
            table_name = tbl['target']['table']
            path = os.path.join(base, db, f'{table_name}.yaml')
            write_manifest(path, dict(tbl))  # copy to avoid mutating
            total += 1
        print(f'{db}: {len(tables)} manifests written')
    
    print(f'Total: {total} manifests')


if __name__ == '__main__':
    main()
