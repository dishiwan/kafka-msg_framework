-- ============================================================================
-- File: 08_composite_sequence_setup.sql
-- Description: Composite Sequence Implementation
-- ============================================================================

-- Add columns for business key
ALTER TABLE INCOMING_MESSAGE_TABLE ADD (
    BUSINESS_KEY_ID        VARCHAR2(30),
    BUSINESS_KEY_PREFIX    VARCHAR2(5),
    BUSINESS_KEY_DATE      DATE,
    BUSINESS_KEY_SEQUENCE  NUMBER(10,0)
);

-- Create unique constraint
ALTER TABLE INCOMING_MESSAGE_TABLE 
ADD CONSTRAINT UQ_BUSINESS_KEY UNIQUE (BUSINESS_KEY_ID);

-- Create composite index for filtering
CREATE INDEX IDX_BIZ_KEY_COMPONENTS ON INCOMING_MESSAGE_TABLE(
    BUSINESS_KEY_PREFIX, 
    BUSINESS_KEY_DATE,
    BUSINESS_KEY_SEQUENCE
) COMPRESS 2;

-- Create continuous sequence (if using continuous strategy)
CREATE SEQUENCE BUSINESS_KEY_CONTINUOUS_SEQ
    START WITH 1
    INCREMENT BY 1
    MAXVALUE 999999999
    NOCYCLE
    CACHE 1000;

-- Function to create daily sequence (called automatically)
CREATE OR REPLACE PROCEDURE CREATE_DAILY_SEQUENCE(p_date IN DATE) AS
    v_seq_name VARCHAR2(50);
    v_exists NUMBER;
BEGIN
    v_seq_name := 'BUSINESS_KEY_SEQ_' || TO_CHAR(p_date, 'YYYYMMDD');
    
    SELECT COUNT(*) INTO v_exists 
    FROM USER_SEQUENCES 
    WHERE SEQUENCE_NAME = v_seq_name;
    
    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 
            'CREATE SEQUENCE ' || v_seq_name || 
            ' START WITH 1 INCREMENT BY 1 MAXVALUE 999999 NOCYCLE CACHE 100';
        DBMS_OUTPUT.PUT_LINE('Created sequence: ' || v_seq_name);
    END IF;
END;
/

COMMIT;
