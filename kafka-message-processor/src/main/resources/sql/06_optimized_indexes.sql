-- ============================================================================
-- File: 06_optimized_indexes.sql
-- Description: Optimized indexes for high-volume processing (6000 queries/sec)
-- ============================================================================

-- Drop existing simple indexes
DROP INDEX IDX_INCOMING_MSG_ID;
DROP INDEX IDX_INCOMING_STATUS;
DROP INDEX IDX_INCOMING_LEASE_EXPIRY;

-- OPTIMIZED: Covering index for orphan detection (most critical query)
-- This single index covers the entire orphan detection query
CREATE INDEX IDX_INCOMING_ORPHAN_COVER ON INCOMING_MESSAGE_TABLE(
    INTERNAL_STATUS,
    RETRY_COUNT,
    LEASE_EXPIRY_TIMESTAMP,
    PROCESSING_INSTANCE_ID,
    MSG_ID,
    SOURCE,
    X_CORRELATION_ID
) COMPRESS 2 PARALLEL 4;

-- OPTIMIZED: Bitmap index for status (low cardinality)
CREATE BITMAP INDEX IDX_INCOMING_STATUS_BMP ON INCOMING_MESSAGE_TABLE(INTERNAL_STATUS);
CREATE BITMAP INDEX IDX_INCOMING_FINAL_STATUS_BMP ON INCOMING_MESSAGE_TABLE(FINAL_STATUS);

-- OPTIMIZED: Function-based index for lease expiry checks
CREATE INDEX IDX_INCOMING_EXPIRED_LEASE ON INCOMING_MESSAGE_TABLE(
    CASE WHEN LEASE_EXPIRY_TIMESTAMP < SYSTIMESTAMP THEN 1 ELSE 0 END,
    MSG_ID
);

-- OPTIMIZED: Composite index for retry queries
CREATE INDEX IDX_INCOMING_RETRY ON INCOMING_MESSAGE_TABLE(
    FINAL_STATUS,
    ERROR_MESSAGE,
    RETRY_COUNT,
    UPDATE_TIMESTAMP
) COMPRESS 2;

-- OPTIMIZED: Hash partitioned index on MSG_ID for direct lookups
CREATE INDEX IDX_INCOMING_MSG_ID_HASH ON INCOMING_MESSAGE_TABLE(MSG_ID) LOCAL;

-- OPTIMIZED: Index on insert timestamp for time-based queries
CREATE INDEX IDX_INCOMING_INSERT_TS ON INCOMING_MESSAGE_TABLE(
    INSERT_TIMESTAMP,
    MSG_ID
) REVERSE;

-- OPTIMIZED: Covering index for outgoing message queries
CREATE INDEX IDX_OUTGOING_COVER ON OUTGOING_MESSAGE_TABLE(
    INTERNAL_SOURCE_ID,
    FINAL_STATUS,
    MSG_ID,
    OUTGOING_MSG_ID
) COMPRESS 2;

-- OPTIMIZED: Instance registry heartbeat index
CREATE INDEX IDX_INSTANCE_HEARTBEAT_COVER ON INSTANCE_REGISTRY_TABLE(
    INSTANCE_STATUS,
    LAST_HEARTBEAT,
    INSTANCE_ID
);

-- Gather statistics for optimizer
BEGIN
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'INCOMING_MESSAGE_TABLE',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        degree => 4
    );
    
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'OUTGOING_MESSAGE_TABLE',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        degree => 4
    );
    
    DBMS_STATS.GATHER_TABLE_STATS(
        ownname => USER,
        tabname => 'INSTANCE_REGISTRY_TABLE',
        estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
        method_opt => 'FOR ALL COLUMNS SIZE AUTO',
        degree => 4
    );
END;
/

-- Enable result cache on hot tables
ALTER TABLE INCOMING_MESSAGE_TABLE RESULT_CACHE (MODE FORCE);
ALTER TABLE INSTANCE_REGISTRY_TABLE RESULT_CACHE (MODE FORCE);

COMMIT;
