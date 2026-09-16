------ Streaming Bronze: Wikipedia edits --------------------

-- Stores every event exactly as received from NiFi
-- Nothing is filtered or cast here - that happens in Silver.
-- TTL drops rows after 90 days to control disk usage.

CREATE TABLE IF NOT EXISTS raw.wiki_edits
(
    rev_id      UInt64                          COMMENT 'Unique revision ID',
    wiki        LowCardinality(String)          COMMENT 'Wiki identifier, e.g. enwiki',
    title       String                          COMMENT 'Article title',
    user        String                          COMMENT 'Editor username or IP',
    is_bot      UInt8                           COMMENT '1 = bot edit, 0 = human',
    byte_delta  Int32                           COMMENT 'Bytes added (positive) or removed (negative)',
    event_time  DateTime                        COMMENT 'Timestamp of the edit',
    raw_payload String                          COMMENT 'Full JSON event - for debugging and re-derivation'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (wiki, event_time, rev_id)
TTL event_time + INTERVAL 90 DAY
COMMENT 'Bronze: raw Wikipedia edit events from NiFi SSE ingestion';


------- Batch Bronze: Open-Meteo daily weather -----------------------

-- Stores one row per city per day as fetched from the Open-Meteo API
-- Column names match the API response fields after normalisation

CREATE TABLE IF NOT EXISTS raw.weather_daily
(
    city                    LowCardinality(String)      COMMENT 'City name',
    country                 LowCardinality(String)      COMMENT 'Country name',
    latitude                Float32                     COMMENT 'Latitude coordinate',
    longitude               Float32                     COMMENT 'Longitude coordinate',
    date                    Date                        COMMENT 'Weather date',
    temp_max_c              Float32                     COMMENT 'Maximum temperature Celsius',
    temp_min_c              Float32                     COMMENT 'Minimum temperature Celsius',
    temp_mean_c             Float32                     COMMENT 'Mean temperature Celsius',
    precipitation_mm        Float32                     COMMENT 'Total precipitation mm',
    windspeed_max_kph       Float32                     COMMENT 'Maximum wind speed kph',
    sunrise                 String                      COMMENT 'Sunrise time ISO8601',
    sunset                  String                      COMMENT 'Sunset time ISO8601',
    ingested_at             DateTime DEFAULT now()      COMMENT 'When this row was loaded'


)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (country, city, date)
COMMENT 'Bronze: raw daily weather from Open-Meteo API via Airflow';