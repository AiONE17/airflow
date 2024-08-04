CREATE DATABASE report;
DROP TABLE IF EXISTS report.srid_on_buf_to_load;
CREATE TABLE report.srid_on_buf_to_load
(
    `dt_hour`   DateTime,
    `qty` UInt32,
    `dt_load`   datetime materialized now()
)
    ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMMDD(dt_hour)
        ORDER BY (dt_hour)
        TTL toStartOfDay(dt_hour) + interval 3 month
        SETTINGS ttl_only_drop_parts=1;