-- ============================================================================
-- File: 01_create_sequences.sql
-- Description: Creates all database sequences for the messaging application
-- ============================================================================

-- Main Message ID Sequence
CREATE SEQUENCE MSG_ID_SEQUENCE
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

-- Internal Source ID Sequence
CREATE SEQUENCE INTERNAL_SOURCE_SEQUENCE
    START WITH 1000000
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

-- Outgoing Message ID Sequence
CREATE SEQUENCE OUTGOING_MSG_ID_SEQUENCE
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

-- Duplicate Message Sequence
CREATE SEQUENCE DUPLICATE_MSG_SEQUENCE
    START WITH 1
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;

COMMIT;
