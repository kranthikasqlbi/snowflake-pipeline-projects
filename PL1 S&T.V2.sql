----------------USE CASE-----------------
-- Customer Master Incremental Load
OR
--Customer Incremental Data Loading using CDC (Streams + Tasks)

--Step 1: Create Database & Schema
CREATE DATABASE DEMO_DB;

CREATE SCHEMA DEMO_DB.PIPELINE;

USE DATABASE DEMO_DB;
USE SCHEMA PIPELINE;


--Step 2: Create Source Table

CREATE OR REPLACE TABLE SOURCE_CUSTOMER
(
    CUSTOMER_ID INT,
    CUSTOMER_NAME STRING,
    CITY STRING
);

--Step 3: Create Target Table

CREATE OR REPLACE TABLE TARGET_CUSTOMER
(
    CUSTOMER_ID INT,
    CUSTOMER_NAME STRING,
    CITY STRING,
    IS_DELETED BOOLEAN DEFAULT FALSE,
    DELETED_DATE TIMESTAMP
);

--Step 4: Insert Initial Data into Source

INSERT INTO SOURCE_CUSTOMER
VALUES
(101,'Kumar','Hyderabad'),
(102,'Rahul','Chennai'),
(103,'Anil','Bangalore');

SELECT * FROM SOURCE_CUSTOMER;


--Step 5: Create Stream

CREATE OR REPLACE STREAM CUSTOMER_STREAM
ON TABLE SOURCE_CUSTOMER;

SHOW STREAMS;

--Step 6: Create Task
-- Run every 1 minute.


CREATE OR REPLACE TASK CUSTOMER_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS
BEGIN

    -- 1. Soft Delete
    UPDATE TARGET_CUSTOMER T
    SET
        IS_DELETED = TRUE,
        DELETED_DATE = CURRENT_TIMESTAMP()
    FROM CUSTOMER_STREAM S
    WHERE T.CUSTOMER_ID = S.CUSTOMER_ID
      AND METADATA$ACTION = 'DELETE';

    -- 2. Update Existing Records
    UPDATE TARGET_CUSTOMER T
    SET
        CUSTOMER_NAME = S.CUSTOMER_NAME,
        CITY = S.CITY,
        IS_DELETED = FALSE,
        DELETED_DATE = NULL
    FROM CUSTOMER_STREAM S
    WHERE T.CUSTOMER_ID = S.CUSTOMER_ID
      AND METADATA$ACTION = 'INSERT'
      AND METADATA$ISUPDATE = TRUE;

    -- 3. Insert New Records
    INSERT INTO TARGET_CUSTOMER
    (
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CITY,
        IS_DELETED,
        DELETED_DATE
    )
    SELECT
        CUSTOMER_ID,
        CUSTOMER_NAME,
        CITY,
        FALSE,
        NULL
    FROM CUSTOMER_STREAM
    WHERE METADATA$ACTION = 'INSERT'
      AND METADATA$ISUPDATE = FALSE;

END;

--Step 7: Resume Task
--Tasks are created in Suspended state.

ALTER TASK CUSTOMER_TASK RESUME;

--Verify
SHOW TASKS;
Status should be
Started

--Step 8: Insert New Records

INSERT INTO SOURCE_CUSTOMER
VALUES
(104,'Suresh','Pune'),
(105,'Priya','Mumbai');

--Step 9: Check Stream
Before task executes

SELECT * FROM CUSTOMER_STREAM;

These are the new rows captured by the stream.

--Step 10: Wait 1 Minute

The task runs automatically.

SELECT * FROM TARGET_CUSTOMER;


The pipeline worked successfully.

--Step 12: Check Stream Again
SELECT * FROM CUSTOMER_STREAM;

No rows

Because the task has already consumed the stream records.


-- Step 13: Insert More Data
INSERT INTO SOURCE_CUSTOMER
VALUES
(106,'Ajay','Delhi');

Wait one minute.

SELECT * FROM TARGET_CUSTOMER;






--source table stores operational (raw) data
--target table stores processed/analytics/reporting data.
-- Generally, the application inserts/update/delete the record into the source table first.
--The target table is automatically synchronized by the CDC pipeline (Streams + Tasks).


--1) Use case name:
Customer incremental data loading pipeline.

--2) Why is it useful?
Processes only new data automatically, saving resources.

--3) What does Stream do?
Captures inserted, updated, and deleted table changes.

--4) What does Task do?
Executes SQL automatically on a schedule.

--5 What do Streams + Tasks do together?
Automate incremental data movement without manual intervention.

--6) Benefit to company:
Lower costs, faster processing, fewer operational errors.

--7) Benefit to employee:
Eliminates repetitive work, improves productivity and reliability.

--8) Cost incurred to company:
Warehouse compute charges during task execution only.

--9) How much load do Streams + Tasks accept?
Scales from thousands to billions of records.

--10)How is runtime calculated in the above Streams + Tasks pipeline?
-- tell me say example: 1mb , how much costs/credit incurrs to compnay

Snowflake charges for warehouse compute time, not for data volume(1mb).
Example
Data processed: 1 MB
Warehouse: X-Small
Execution time: 1 minute
Credits consumed: Approximately 1/60 credit = 0.0167 credits

If 1 credit = $3, then:

Cost ≈ $0.05 USD (about ₹4–₹5, depending on exchange rate)
Key point for interviews

Snowflake charges based on warehouse runtime, not on MB or GB processed.

Snowflake charges for compute time and warehouse size, not directly for data size. 
Larger datasets(GB/TB/PBs) may increase runtime, which can increase cost. memcite




--Step 11 : Test INSERT

INSERT INTO SOURCE_CUSTOMER
VALUES
(104,'Suresh','Pune');

--wait for a minute

SELECT * FROM TARGET_CUSTOMER;

--Step 12 : Test UPDATE

UPDATE SOURCE_CUSTOMER
SET CITY='Mumbai'
WHERE CUSTOMER_ID=104;

SELECT * FROM TARGET_CUSTOMER
WHERE CUSTOMER_ID=104;

---STEP 13 TEST DELETE
DELETE
FROM SOURCE_CUSTOMER
WHERE CUSTOMER_ID=104;

---These two queries are diagnostic tools used to monitor, troubleshoot, and track the execution execution history of Snowflake Tasks.

--If someone wrote these queries in your environment, they are trying to answer two fundamental production questions: "Is my automation actually running?" and "If it failed, why did it fail?"

--Here is the breakdown of why each specific query is used:

--Query 1: The Global Health Check

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY SCHEDULED_TIME DESC;

--Query 2: The Targeted Troubleshooting Snippet

SELECT
    NAME,
    STATE,
    ERROR_MESSAGE,
    SCHEDULED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'CUSTOMER_TASK'
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;

--Why it's there: To isolate and investigate a specific pipeline pipeline—in this case, a task named 'CUSTOMER_TASK'.

--What it tells you: Instead of looking at raw system clutter, it filters down to just four critical data points for that specific task:

--STATE: Tells you if the run was SUCCEEDED, FAILED, or SKIPPED (e.g., if a root stream was empty).

--ERROR_MESSAGE: If the state is FAILED, this column contains the exact error log (e.g., syntax errors, missing tables, or privilege issues) so you know exactly what to fix.


--Common Real-World Scenarios for Using These
--Debugging a Broken Pipeline: Your CUSTOMER_TASK didn't populate the target table this morning. You run Query 2 to see the exact error message that caused the crash.

---Checking Overlaps / Performance: You want to see if a task is taking longer to execute than its scheduled interval (e.g., a task scheduled every 5 minutes that takes 7 minutes to run).


--1) ignore
--testing Table creation in vscode 

--Step 2: Create Source Table

CREATE OR REPLACE TABLE demo_Db.PIPELINE.SOURCE_CUSTOMER2
(
    CUSTOMER_ID INT,
    CUSTOMER_NAME STRING,
    CITY STRING
);
hi
--- Github update check 1
---code 100 lines


