-- ============================================================================
-- File: 07_performance_tuning.sql  
-- Description: Oracle performance tuning for high-volume workload
-- ============================================================================

-- Enable parallel DML for bulk operations
ALTER SESSION ENABLE PARALLEL DML;
ALTER SESSION FORCE PARALLEL QUERY PARALLEL 4;

-- Set optimizer mode
ALTER SESSION SET OPTIMIZER_MODE = ALL_ROWS;

-- Enable result cache
ALTER SYSTEM SET RESULT_CACHE_MAX_SIZE = 2G SCOPE=BOTH;
ALTER SYSTEM SET RESULT_CACHE_MODE = FORCE SCOPE=BOTH;

-- Optimize redo logs
-- Add more redo log groups to reduce checkpoint frequency
ALTER DATABASE ADD LOGFILE GROUP 4 
('/oracle/redo04a.log', '/oracle/redo04b.log') SIZE 2G;

ALTER DATABASE ADD LOGFILE GROUP 5
('/oracle/redo05a.log', '/oracle/redo05b.log') SIZE 2G;

-- Set optimal redo log size
ALTER SYSTEM SET LOG_BUFFER = 256M SCOPE=SPFILE;

-- Increase SGA for better caching
ALTER SYSTEM SET SGA_TARGET = 16G SCOPE=SPFILE;
ALTER SYSTEM SET SGA_MAX_SIZE = 20G SCOPE=SPFILE;

-- Optimize PGA for sorting/hashing
ALTER SYSTEM SET PGA_AGGREGATE_TARGET = 8G SCOPE=BOTH;

-- Enable automatic memory management
ALTER SYSTEM SET MEMORY_TARGET = 24G SCOPE=SPFILE;
ALTER SYSTEM SET MEMORY_MAX_TARGET = 28G SCOPE=SPFILE;

-- Optimize for high concurrency
ALTER SYSTEM SET PROCESSES = 500 SCOPE=SPFILE;
ALTER SYSTEM SET SESSIONS = 800 SCOPE=SPFILE;

-- Enable adaptive query optimization
ALTER SYSTEM SET OPTIMIZER_ADAPTIVE_FEATURES = TRUE SCOPE=BOTH;

-- Set optimal commit write behavior
ALTER SYSTEM SET COMMIT_WRITE = 'BATCH,NOWAIT' SCOPE=BOTH;

-- Create optimized tablespace for message tables
CREATE TABLESPACE MSG_TABLESPACE
DATAFILE '/oracle/data/msg_data_01.dbf' SIZE 20G
AUTOEXTEND ON NEXT 2G MAXSIZE UNLIMITED
EXTENT MANAGEMENT LOCAL AUTOALLOCATE
SEGMENT SPACE MANAGEMENT AUTO
LOGGING;

-- Move tables to optimized tablespace
ALTER TABLE INCOMING_MESSAGE_TABLE MOVE TABLESPACE MSG_TABLESPACE;
ALTER TABLE OUTGOING_MESSAGE_TABLE MOVE TABLESPACE MSG_TABLESPACE;

-- Rebuild indexes after move
ALTER INDEX IDX_INCOMING_ORPHAN_COVER REBUILD PARALLEL 4 ONLINE;
ALTER INDEX IDX_OUTGOING_COVER REBUILD PARALLEL 4 ONLINE;

-- Schedule statistics gathering
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name => 'GATHER_MESSAGE_STATS',
        job_type => 'PLSQL_BLOCK',
        job_action => '
            BEGIN
                DBMS_STATS.GATHER_SCHEMA_STATS(
                    ownname => USER,
                    estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
                    degree => 4,
                    cascade => TRUE
                );
            END;',
        start_date => SYSTIMESTAMP,
        repeat_interval => 'FREQ=DAILY; BYHOUR=2',
        enabled => TRUE,
        comments => 'Gather statistics for message tables'
    );
END;
/

COMMIT;
