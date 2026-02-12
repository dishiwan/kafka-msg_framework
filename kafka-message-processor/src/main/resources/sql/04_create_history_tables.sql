-- ============================================================================
-- File: 04_create_history_tables.sql
-- Description: Creates history tables for archiving
-- ============================================================================

-- Incoming Message History Table
CREATE TABLE INCOMING_MESSAGE_TABLE_HIST (
    MSG_ID                NUMBER(16,0),
    SOURCE                VARCHAR2(100),
    X_CORRELATION_ID      VARCHAR2(200),
    INTERNAL_SOURCE_ID    NUMBER(16,0),
    FINAL_STATUS          VARCHAR2(50),
    INTERNAL_STATUS       VARCHAR2(200),
    PHASE                 VARCHAR2(50),
    EVENT_TYPE            VARCHAR2(100),
    PROCESS_TYPE          VARCHAR2(100),
    ERROR_MESSAGE         VARCHAR2(2000),
    CYCLE_DATE            DATE,
    ATTRIBUTE_KEY1        VARCHAR2(500),
    ATTRIBUTE_KEY2        VARCHAR2(500),
    ATTRIBUTE_KEY3        VARCHAR2(500),
    ATTRIBUTE_KEY4        VARCHAR2(500),
    ORIGINAL_MSG          CLOB,
    TRANSFORMED_MSG       CLOB,
    PROCESS_LOG           CLOB,
    INSERT_TIMESTAMP      TIMESTAMP,
    UPDATE_TIMESTAMP      TIMESTAMP,
    INSERTED_BY           VARCHAR2(100),
    UPDATED_BY            VARCHAR2(100),
    PRIORITY              NUMBER(2,0),
    RETRY_COUNT           NUMBER(3,0),
    ARCHIVED_TIMESTAMP    TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- Outgoing Message History Table
CREATE TABLE OUTGOING_MESSAGE_TABLE_HIST (
    MSG_ID                NUMBER(16,0),
    SOURCE                VARCHAR2(100),
    X_CORRELATION_ID      VARCHAR2(200),
    INTERNAL_SOURCE_ID    NUMBER(16,0),
    OUTGOING_MSG_ID       VARCHAR2(200),
    FINAL_STATUS          VARCHAR2(50),
    PHASE                 VARCHAR2(50),
    ERROR_MESSAGE         VARCHAR2(2000),
    EVENT_TYPE            VARCHAR2(100),
    PROCESS_TYPE          VARCHAR2(100),
    CYCLE_DATE            DATE,
    ATTRIBUTE_KEY1        VARCHAR2(500),
    ATTRIBUTE_KEY2        VARCHAR2(500),
    ATTRIBUTE_KEY3        VARCHAR2(500),
    ATTRIBUTE_KEY4        VARCHAR2(500),
    ORIGINAL_MSG          CLOB,
    PROCESS_LOG           CLOB,
    INSERT_TIMESTAMP      TIMESTAMP,
    UPDATE_TIMESTAMP      TIMESTAMP,
    INSERTED_BY           VARCHAR2(100),
    UPDATED_BY            VARCHAR2(100),
    RETRY_COUNT           NUMBER(3,0),
    ARCHIVED_TIMESTAMP    TIMESTAMP DEFAULT SYSTIMESTAMP
);

COMMIT;
