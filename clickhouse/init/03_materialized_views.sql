------ Silver Materialized View -  streaming path
"A Clickhouse Materialized View (MV) is a trigger that fires on every 'INSERT' into the source table and writes
transformed rows into a target table. This replaces dbt for the streaming Silver layer - no scheduler needed"

------ clickhouse/init/03_materialized_viewa.sql

------ Step 1: Create the Silver target table first.
------ ReplacingMergeTree deduplicates9 rows with the same ORDER BY key
------ Keeping the row with the hihest event_time. This protects against
------ NiFi replaying events if it reconnects after a drop  

CREATE TABLE IF NOT EXISTS clean.wiki_edits
(
    rev_id          UInt64                  COMMENT 'Unique revision ID - dedupe key',
    wiki            LowCardinality(String)  COMMENT 'Wiki identifier',
    title           String                  COMMENT 'Article title',
    user            String                  COMMENT 'Editor username',
    is_bot          Bool                    COMMENT 'True if bot - cast from UInt8',
    byte_delta      Int32                   COMMENT 'Byte change size',
    event_time      DateTime                COMMENT 'Original event timestamp',
    event_date      Date MATERIALIZED toDate(event_time)    COMMENT 'Derived date for Partitioning'
)
ENGINE = ReplacingMergeTree(event_time)
PARTITION BY toYYYYMM(event_time)
ORDER BY (wiki, rev_id)
COMMENT 'Silver: cleaned Wikipedia edits - bot-filtered, typed, deduped';


-- Step 2: Create the Materialized View.
-- It fires on every INSERT into raw.wiki_edits.
-- Only human edits with non-empty titles and non-zero byte changes pass.
--
CREATE MATERIALIZED VIEW IF NOT EXISTS clean.mv_wiki_edits
TO clean.wiki_edits
AS
SELECT
    toUInt64(rev_id)            AS rev_id,
    wiki,
    title,
    user,
    (is_bot = 1)                AS is_bot,     -- UInt8 → Bool
    toInt32(byte_delta)         AS byte_delta,
    toDateTime(event_time)      AS event_time
FROM raw.wiki_edits
WHERE is_bot = 0               -- drop bot edits at Silver
  AND length(title) > 0        -- drop events with missing titles
  AND byte_delta != 0;         -- drop no-change edits
```