-- ============================================================================
-- File: 05_archiving_procedure.sql
-- Description: Creates procedure for archiving data to history tables
-- ============================================================================

CREATE OR REPLACE PROCEDURE ARCHIVE_MESSAGES_WEEKLY AS
    v_archive_date DATE;
    v_rows_archived NUMBER;
BEGIN
    v_archive_date := TRUNC(SYSDATE) - 7;
    
    -- Archive Incoming Messages
    INSERT INTO INCOMING_MESSAGE_TABLE_HIST
    SELECT *, SYSTIMESTAMP
    FROM INCOMING_MESSAGE_TABLE
    WHERE CYCLE_DATE < v_archive_date
      AND FINAL_STATUS IN ('PUBLISHED', 'FAILED');
    
    v_rows_archived := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('Archived ' || v_rows_archived || ' incoming messages');
    
    DELETE FROM INCOMING_MESSAGE_TABLE
    WHERE CYCLE_DATE < v_archive_date
      AND FINAL_STATUS IN ('PUBLISHED', 'FAILED');
    
    -- Archive Outgoing Messages
    INSERT INTO OUTGOING_MESSAGE_TABLE_HIST
    SELECT *, SYSTIMESTAMP
    FROM OUTGOING_MESSAGE_TABLE
    WHERE CYCLE_DATE < v_archive_date
      AND FINAL_STATUS IN ('PUBLISHED', 'FAILED');
    
    v_rows_archived := SQL%ROWCOUNT;
    DBMS_OUTPUT.PUT_LINE('Archived ' || v_rows_archived || ' outgoing messages');
    
    DELETE FROM OUTGOING_MESSAGE_TABLE
    WHERE CYCLE_DATE < v_archive_date
      AND FINAL_STATUS IN ('PUBLISHED', 'FAILED');
    
    COMMIT;
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
/
