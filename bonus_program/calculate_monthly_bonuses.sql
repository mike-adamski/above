-- USE ROLE ABOVE_OPS_ROLE;
USE ROLE ABOVE_STR_OPS_ROLE;

/* CHOOSE TIME PERIOD */
-- This should be either 'FINAL' OR 'MTD'
SET RUN_TYPE = 'MTD';
/* CHOOSE TIME PERIOD */


SET BONUS_MONTH = CASE
                      WHEN RUN_TYPE = 'MTD' THEN date_trunc('month', current_date)
                      WHEN RUN_TYPE = 'FINAL' THEN date_trunc('month', current_date - extract(DAY FROM current_date))
                      ELSE '/' -- should return an error and stop running
                      END;
-- Determine which agents are eligible based on working 80+ hours in the month
DELETE
FROM ABOVE_STR_OPS.BP_ELIGIBLE
WHERE MONTH = $BONUS_MONTH;
INSERT INTO ABOVE_STR_OPS.BP_ELIGIBLE
SELECT $BONUS_MONTH AS MONTH
     , NAME
     , POSITION
     , MANAGER_NAME
     , START_DATE
     , (
       SELECT coalesce(sum(STATUS_TIME) / 3600, 0)
       FROM TALKDESK.USER_STATUS
       WHERE LOWER(USER_NAME) = LOWER(E.NAME)
         AND date_trunc('month', STATUS_START_AT - INTERVAL '6 HOUR')::DATE = $BONUS_MONTH
         AND STATUS_LABEL <> 'Offline'
       ) AS HOURS_WORKED_IN_MONTH
     , IFF(HOURS_WORKED_IN_MONTH >= 80
               OR POSITION = 'Special Handling'
               OR POSITION ILIKE 'Team Lead%', TRUE, FALSE) AS IS_ELIGIBLE
     , current_timestamp AS RUN_TIMESTAMP
FROM ABOVE_STR_OPS.OPS_EMPLOYEES E
WHERE SNAPSHOT_DATE = (
                      SELECT max(SNAPSHOT_DATE)
                      FROM ABOVE_STR_OPS.OPS_EMPLOYEES
                      )
  AND POSITION NOT IN ('Ops Manager', 'Director');

-- Calculate alarm time by agent + day using Talkdesk status data
TRUNCATE TABLE ABOVE_STR_OPS.ALARM_TIME_RAW;
INSERT INTO ABOVE_STR_OPS.ALARM_TIME_RAW
WITH AGENT_TIME AS (
                   SELECT USER_NAME
                        , (STATUS_START_AT - INTERVAL '6 HOUR')::DATE AS WORK_DATE
                        , sum(CASE WHEN STATUS_LABEL = 'After Call Work' THEN STATUS_TIME ELSE 0 END) /
                          3600 AS AFTER_CALL_WORK
                        , sum(CASE WHEN STATUS_LABEL = 'Available' THEN STATUS_TIME ELSE 0 END) /
                          3600 AS AVAILABLE
                        , sum(CASE WHEN STATUS_LABEL = 'Away' THEN STATUS_TIME ELSE 0 END) /
                          3600 AS AWAY
                        , sum(CASE WHEN STATUS_LABEL = 'Break' THEN STATUS_TIME ELSE 0 END) / 3600 AS BREAK
                        , sum(CASE WHEN STATUS_LABEL = 'Email' THEN STATUS_TIME ELSE 0 END) / 3600 AS EMAIL
                        , sum(CASE WHEN STATUS_LABEL = 'Lunch' THEN STATUS_TIME ELSE 0 END) / 3600 AS LUNCH
                        , sum(CASE WHEN STATUS_LABEL = 'Meeting' THEN STATUS_TIME ELSE 0 END) / 3600 AS MEETING
                        , sum(CASE WHEN STATUS_LABEL = 'On a Call' THEN STATUS_TIME ELSE 0 END) / 3600 AS ON_CALL
                        , sum(CASE WHEN STATUS_LABEL = 'Outbound' THEN STATUS_TIME ELSE 0 END) / 3600 AS OUTBOUND
                        , sum(CASE WHEN STATUS_LABEL = 'Personal' THEN STATUS_TIME ELSE 0 END) / 3600 AS PERSONAL
                        , sum(CASE WHEN STATUS_LABEL = 'System Issues' THEN STATUS_TIME ELSE 0 END) /
                          3600 AS SYSTEM_ISSUES
                        , sum(STATUS_TIME) / 3600 - LUNCH AS TOTAL_DURATION
                   FROM TALKDESK.USER_STATUS
                   WHERE TRUE
                     AND STATUS_LABEL <> 'Offline'
                   GROUP BY 1, 2
                   )

   , CALL_DATA AS (
                  SELECT USER_NAME
                       , (START_AT - INTERVAL '6 hour')::DATE AS WORK_DATE
                       , count(*) AS NUM_CALLS
                       , count(DISPOSITION_CODE) AS DISPOSITIONED_CALLS
                  FROM TALKDESK.CALLS
                  WHERE TRUE
                  GROUP BY 1, 2
                  )

   , SUMMARY_BY_DAY AS (
                       SELECT T.USER_NAME
                            , T.WORK_DATE
                            , date_trunc('month', T.WORK_DATE) AS WORK_MONTH
                            , COALESCE(CD.NUM_CALLS, 0) AS NUM_CALLS_TOTAL
                            , COALESCE(CD.DISPOSITIONED_CALLS, 0) AS NUM_CALLS_DISPOSITIONED
                            , AFTER_CALL_WORK
                            , AVAILABLE
                            , AWAY
                            , BREAK
                            , EMAIL
                            , LUNCH
                            , MEETING
                            , ON_CALL
                            , OUTBOUND
                            , PERSONAL
                            , SYSTEM_ISSUES
                            , TOTAL_DURATION
                            , DIV0(T.AFTER_CALL_WORK, NUM_CALLS_TOTAL) * 3600 AS AVG_ACW_SECONDS
                            , DIV0(NUM_CALLS_DISPOSITIONED, NUM_CALLS_TOTAL) AS PERC_DISPOSITIONED
                            , (GREATEST(PERSONAL, 0) + GREATEST(BREAK - .5, 0) + GREATEST(LUNCH - 1, 0) +
                               GREATEST(8 - TOTAL_DURATION, 0)) * 60 AS ALARM_TIME_MINS
                       FROM AGENT_TIME T
                            LEFT JOIN CALL_DATA CD
                                      ON LOWER(T.USER_NAME) = LOWER(CD.USER_NAME) AND T.WORK_DATE = CD.WORK_DATE
                       ORDER BY 1, 2
                       )

SELECT USER_NAME AS AGENT_NAME
     , WORK_DATE
     , WORK_MONTH
     , NUM_CALLS_TOTAL
     , NUM_CALLS_DISPOSITIONED
     , AFTER_CALL_WORK
     , AVAILABLE
     , AWAY
     , BREAK
     , EMAIL
     , LUNCH
     , MEETING
     , ON_CALL
     , OUTBOUND
     , PERSONAL
     , SYSTEM_ISSUES
     , TOTAL_DURATION
     , AVG_ACW_SECONDS
     , PERC_DISPOSITIONED
     , ALARM_TIME_MINS
     , IFF(TOTAL_DURATION < 6 OR TOTAL_DURATION > 12, TRUE, FALSE) AS IS_OUTLIER_DAY
FROM SUMMARY_BY_DAY
;

-- Load alarm time data relevant to bonus plan
DELETE
FROM ABOVE_STR_OPS.BP_ALARM_TIME
WHERE MONTH = $BONUS_MONTH;
INSERT INTO ABOVE_STR_OPS.BP_ALARM_TIME
SELECT E.MONTH
     , E.NAME AS AGENT_NAME
     , E.POSITION
     , E.MANAGER_NAME
     , WORK_DATE
     , AFTER_CALL_WORK
     , AVAILABLE
     , AWAY
     , BREAK
     , EMAIL
     , LUNCH
     , MEETING
     , ON_CALL
     , OUTBOUND
     , PERSONAL
     , SYSTEM_ISSUES
     , TOTAL_DURATION
     , ALARM_TIME_MINS
     , IS_OUTLIER_DAY
     , current_timestamp AS RUN_TIMESTAMP
FROM ABOVE_STR_OPS.ALARM_TIME_RAW S
     JOIN ABOVE_STR_OPS.BP_ELIGIBLE E
          ON LOWER(S.AGENT_NAME) = LOWER(E.NAME) AND S.WORK_MONTH = E.MONTH
WHERE E.MONTH = $BONUS_MONTH
  AND E.POSITION IN ('Customer Care', 'Loan Processing', 'Collection/Hybrid')
;

-- Get historical QA data from Observe AI
/* NOTE: Recent evaluations must be manually loaded before running */
DELETE
FROM ABOVE_STR_OPS.BP_QA_SCORES QA
WHERE MONTH = $BONUS_MONTH;
INSERT INTO ABOVE_STR_OPS.BP_QA_SCORES
SELECT E.MONTH
     , E.NAME AS AGENT_NAME
     , E.POSITION
     , E.MANAGER_NAME
     , EVALUATION_DATE
     , CALL_ID
     , SCORE
     , EVALUATOR_NAME
     , PASS_FAIL
     , CALL_DATE
     , CALL_TIME
     , CALL_TYPE
     , CALL_DISPOSITION
     , max(LOADED_TIMESTAMP) OVER () AS RUN_TIMESTAMP
FROM ABOVE_STR_OPS.QA_DATA_RAW QA
     JOIN ABOVE_STR_OPS.BP_ELIGIBLE E
          ON LOWER(QA.AGENT_NAME) = LOWER(E.NAME) AND date_trunc('month', EVALUATION_DATE)::DATE = E.MONTH
WHERE MONTH = $BONUS_MONTH
  AND E.POSITION IN ('Customer Care', 'Loan Processing', 'Collection/Hybrid', 'Verification')
    QUALIFY rank() OVER (PARTITION BY CALL_ID ORDER BY LOADED_TIMESTAMP DESC) = 1
;

-- Refresh verification funnel data to identify UIDs that have been decisioned
TRUNCATE ABOVE_STR_OPS.VERIF_FUNNEL;
INSERT INTO ABOVE_STR_OPS.VERIF_FUNNEL
SELECT *
FROM ABOVE_STR_OPS.VERIF_FUNNEL_REFRESH_V;

-- Pull decisions made during the month and get sendback rate data
DELETE
FROM ABOVE_STR_OPS.BP_SENDBACK_RATE
WHERE MONTH = $BONUS_MONTH;
INSERT INTO ABOVE_STR_OPS.BP_SENDBACK_RATE
WITH DECISIONS AS (
                  SELECT UNIFIED_ID
                       , APPLICATION_DATE
                       , APPLICATION_DATE + MAX(DAYS_TO_DECISION) AS DECISION_DATE
                       , MAX(DECISIONED_FLG) AS DECISIONED
                  FROM ABOVE_STR_OPS.VERIF_FUNNEL F
                  GROUP BY 1, 2
                  )
SELECT MONTH
     , coalesce(SB.AGENT, E.NAME) AS AGENT_NAME
     , E.POSITION
     , E.MANAGER_NAME
     , D.UNIFIED_ID
     , APPLICATION_DATE
     , coalesce(SB.SENDBACK_DATE, D.DECISION_DATE) AS DEC_DATE
     , COALESCE(SB.NUM_SENDBACKS, 0) AS SENDBACKS
     , SB.SENDBACK_DATE
     , current_timestamp::TIMESTAMP_NTZ AS RUN_TIMESTAMP
FROM DECISIONS D
     JOIN (
          SELECT *
          FROM ABOVE_STR_OPS.VERIF_AGENT_UID_ASSIGNMENTS -- Must be manually updated before running
              QUALIFY rank() OVER (PARTITION BY UNIFIED_ID ORDER BY LOADED_DATE DESC) = 1
          ) A
          ON D.UNIFIED_ID = A.UNIFIED_ID
     LEFT JOIN (
               SELECT UNIFIED_ID
                    , AGENT
                    , count(*) AS NUM_SENDBACKS
                    , min(DECISION_DATE) AS SENDBACK_DATE
               FROM ABOVE_STR_OPS.VERIF_SENDBACKS
               WHERE LOADED_DATE = (
                                   SELECT max(LOADED_DATE)
                                   FROM ABOVE_STR_OPS.VERIF_SENDBACKS
                                   )
               GROUP BY 1, 2
               ) SB ON SB.UNIFIED_ID = D.UNIFIED_ID
     JOIN ABOVE_STR_OPS.BP_ELIGIBLE E
          ON LOWER(coalesce(SB.AGENT, A.AGENT_NAME)) = LOWER(E.NAME) AND
             date_trunc('month', coalesce(SB.SENDBACK_DATE, D.DECISION_DATE))::DATE = E.MONTH
WHERE A.AGENT_NAME IS NOT NULL
  AND date_trunc('month', DEC_DATE)::DATE = $BONUS_MONTH
  AND E.POSITION IN ('Verification')
;


/* TEAM LEADS */
DELETE
FROM ABOVE_STR_OPS.BP_TL_ALARM_TIME
WHERE MONTH = $BONUS_MONTH;
INSERT INTO ABOVE_STR_OPS.BP_TL_ALARM_TIME
SELECT BPAT.MONTH
     , MGR.NAME AS LEAD_NAME
     , MGR.POSITION
     , AGENT.NAME AS AGENT_NAME
     , AGENT.START_DATE AS AGENT_START_DATE
     , IFF(AGENT.START_DATE > $BONUS_MONTH - INTERVAL '3 MONTHS', TRUE, FALSE) AS IS_NEW_HIRE
     , COUNT(*) AS DAYS_WORKED
     , SUM(ALARM_TIME_MINS) AS TOTAL_ALARM_TIME
     , DIV0(TOTAL_ALARM_TIME, DAYS_WORKED) AS AVG_ALARM_TIME
     , current_timestamp AS RUN_TIMESTAMP
FROM ABOVE_STR_OPS.BP_ALARM_TIME BPAT
     JOIN ABOVE_STR_OPS.BP_ELIGIBLE AGENT
          ON lower(BPAT.AGENT_NAME) = lower(AGENT.NAME) AND BPAT.MONTH = AGENT.MONTH
     JOIN ABOVE_STR_OPS.BP_ELIGIBLE MGR
          ON lower(BPAT.MANAGER_NAME) = lower(MGR.NAME) AND BPAT.MONTH = MGR.MONTH
WHERE NOT IS_OUTLIER_DAY
  AND BPAT.MONTH = $BONUS_MONTH
GROUP BY 1, 2, 3, 4, 5, 6
;

DELETE
FROM ABOVE_STR_OPS.BP_TL_QA_SCORES
WHERE MONTH = $BONUS_MONTH;
INSERT INTO ABOVE_STR_OPS.BP_TL_QA_SCORES
SELECT BPQA.MONTH
     , MGR.NAME AS LEAD_NAME
     , MGR.POSITION
     , AGENT.NAME AS AGENT_NAME
     , AGENT.START_DATE AS AGENT_START_DATE
     , IFF(AGENT.START_DATE > $BONUS_MONTH - INTERVAL '3 MONTHS', TRUE, FALSE) AS IS_NEW_HIRE
     , COUNT(*) AS NUM_EVALUATIONS
     , AVG(SCORE) AS AVG_SCORE
     , current_timestamp AS RUN_TIMESTAMP
FROM ABOVE_STR_OPS.BP_QA_SCORES BPQA
     JOIN ABOVE_STR_OPS.BP_ELIGIBLE AGENT
          ON lower(BPQA.AGENT_NAME) = lower(AGENT.NAME) AND BPQA.MONTH = AGENT.MONTH
     JOIN ABOVE_STR_OPS.BP_ELIGIBLE MGR
          ON lower(BPQA.MANAGER_NAME) = lower(MGR.NAME) AND BPQA.MONTH = MGR.MONTH
WHERE TRUE
  AND BPQA.MONTH = $BONUS_MONTH
GROUP BY 1, 2, 3, 4, 5
;

DELETE
FROM ABOVE_STR_OPS.BP_TL_SENDBACK_RATE
WHERE MONTH = $BONUS_MONTH;
INSERT INTO ABOVE_STR_OPS.BP_TL_SENDBACK_RATE
SELECT BPSR.MONTH
     , MGR.NAME AS LEAD_NAME
     , MGR.POSITION
     , AGENT.NAME AS AGENT_NAME
     , AGENT.START_DATE AS AGENT_START_DATE
     , IFF(AGENT.START_DATE > $BONUS_MONTH - INTERVAL '3 MONTHS', TRUE, FALSE) AS IS_NEW_HIRE
     , COUNT(*) AS NUM_DECISIONS
     , sum(BPSR.SENDBACKS) AS NUM_SENDBACKS
     , DIV0(NUM_SENDBACKS, NUM_DECISIONS) AS SENDBACK_RATE
     , current_timestamp AS RUN_TIMESTAMP
FROM ABOVE_STR_OPS.BP_SENDBACK_RATE BPSR
     JOIN ABOVE_STR_OPS.BP_ELIGIBLE AGENT
          ON lower(BPSR.AGENT_NAME) = lower(AGENT.NAME) AND BPSR.MONTH = AGENT.MONTH
     JOIN ABOVE_STR_OPS.BP_ELIGIBLE MGR
          ON lower(BPSR.MANAGER_NAME) = lower(MGR.NAME) AND BPSR.MONTH = MGR.MONTH
WHERE TRUE
  AND BPSR.MONTH = $BONUS_MONTH
GROUP BY 1, 2, 3, 4, 5
;


/* SUMMARY */
DELETE
FROM ABOVE_STR_OPS.BP_SUMMARY
WHERE MONTH = $BONUS_MONTH;

-- Alarm Time
INSERT INTO ABOVE_STR_OPS.BP_SUMMARY
SELECT AT.MONTH
     , E.NAME
     , AT.POSITION
     , E.IS_ELIGIBLE
     , M.METRIC_NAME
     , M.TARGET AS METRIC_TARGET
     , avg(ALARM_TIME_MINS) AS METRIC_VALUE
     , round(METRIC_VALUE) AS METRIC_VALUE_ROUNDED
     , IFF(E.IS_ELIGIBLE AND METRIC_VALUE_ROUNDED <= METRIC_TARGET, MAX(M.BONUS), 0) AS BONUS_AMOUNT
     , NULL AS VALUE_ALL_AGENTS
     , NULL AS VALUE_EXCL_NEW_AGENTS
     , current_timestamp::TIMESTAMP_NTZ AS RUN_TIMESTAMP
FROM ABOVE_STR_OPS.BP_ALARM_TIME AS AT
     LEFT JOIN ABOVE_STR_OPS.BP_ELIGIBLE AS E ON AT.MONTH = E.MONTH AND AT.AGENT_NAME = E.NAME
     LEFT JOIN ABOVE_STR_OPS.BP_METRICS AS M
               ON AT.POSITION = M.POSITION AND M.METRIC_NAME = 'Alarm Time' AND M.EFF_END_DATE IS NULL
WHERE AT.MONTH = $BONUS_MONTH
  AND NOT IS_OUTLIER_DAY
GROUP BY 1, 2, 3, 4, 5, 6
;

-- QA Scores
INSERT INTO ABOVE_STR_OPS.BP_SUMMARY
SELECT QA.MONTH
     , E.NAME
     , QA.POSITION
     , E.IS_ELIGIBLE
     , M.METRIC_NAME
     , M.TARGET AS METRIC_TARGET
     , avg(QA.SCORE) AS METRIC_VALUE
     , round(METRIC_VALUE, 2) AS METRIC_VALUE_ROUNDED
     , IFF(E.IS_ELIGIBLE AND METRIC_VALUE_ROUNDED >= METRIC_TARGET, MAX(M.BONUS), 0) AS BONUS_AMOUNT
     , NULL AS VALUE_ALL_AGENTS
     , NULL AS VALUE_EXCL_NEW_AGENTS
     , current_timestamp::TIMESTAMP_NTZ AS RUN_TIMESTAMP
FROM ABOVE_STR_OPS.BP_QA_SCORES AS QA
     LEFT JOIN ABOVE_STR_OPS.BP_ELIGIBLE AS E ON QA.MONTH = E.MONTH AND QA.AGENT_NAME = E.NAME
     LEFT JOIN ABOVE_STR_OPS.BP_METRICS AS M
               ON QA.POSITION = M.POSITION AND M.METRIC_NAME = 'QA Score' AND M.EFF_END_DATE IS NULL
WHERE QA.MONTH = $BONUS_MONTH
GROUP BY 1, 2, 3, 4, 5, 6
;

-- Sendback Rate
INSERT INTO ABOVE_STR_OPS.BP_SUMMARY
SELECT SB.MONTH
     , E.NAME
     , SB.POSITION
     , E.IS_ELIGIBLE
     , M.METRIC_NAME
     , M.TARGET AS METRIC_TARGET
     , sum(SENDBACKS) / count(*) AS METRIC_VALUE
     , round(METRIC_VALUE, 3) AS METRIC_VALUE_ROUNDED
     , IFF(E.IS_ELIGIBLE AND METRIC_VALUE_ROUNDED <= METRIC_TARGET, MAX(M.BONUS), 0) AS BONUS_AMOUNT
     , NULL AS VALUE_ALL_AGENTS
     , NULL AS VALUE_EXCL_NEW_AGENTS
     , current_timestamp::TIMESTAMP_NTZ AS RUN_TIMESTAMP
FROM ABOVE_STR_OPS.BP_SENDBACK_RATE AS SB
     LEFT JOIN ABOVE_STR_OPS.BP_ELIGIBLE AS E ON SB.MONTH = E.MONTH AND SB.AGENT_NAME = E.NAME
     LEFT JOIN ABOVE_STR_OPS.BP_METRICS AS M
               ON SB.POSITION = M.POSITION AND M.METRIC_NAME = 'Sendback Rate' AND M.EFF_END_DATE IS NULL
WHERE SB.MONTH = $BONUS_MONTH
GROUP BY 1, 2, 3, 4, 5, 6
;

-- Average Escalation Time
/* NOTE: Need to load 'BP_ESCALATION_RESOLUTION' manually using Zendesk data */
INSERT INTO ABOVE_STR_OPS.BP_SUMMARY
SELECT DISTINCT
       E.MONTH
     , E.NAME
     , E.POSITION
     , E.IS_ELIGIBLE
     , M.METRIC_NAME
     , MAX(M.TARGET) OVER () AS METRIC_TARGET -- Return target for lowest bonus level
     , BPER.AVG_RESOLUTION_TIME AS METRIC_VALUE
     , round(METRIC_VALUE) AS METRIC_VALUE_ROUNDED
     , max(IFF(E.IS_ELIGIBLE AND METRIC_VALUE_ROUNDED <= M.TARGET, M.BONUS, 0))
           OVER (PARTITION BY E.NAME) AS BONUS_AMOUNT
     , NULL AS VALUE_ALL_AGENTS
     , NULL AS VALUE_EXCL_NEW_AGENTS
     , current_timestamp::TIMESTAMP_NTZ AS RUN_TIMESTAMP
FROM ABOVE_STR_OPS.BP_ESCALATION_RESOLUTION AS BPER
     LEFT JOIN ABOVE_STR_OPS.BP_ELIGIBLE AS E ON BPER.MONTH = E.MONTH AND BPER.AGENT_NAME = E.NAME
     LEFT JOIN ABOVE_STR_OPS.BP_METRICS AS M
               ON E.POSITION = M.POSITION
                   AND M.METRIC_NAME = 'Escalation Resolution Time'
                   AND M.EFF_END_DATE IS NULL
WHERE BPER.MONTH = $BONUS_MONTH
;

-- TL Alarm Time
INSERT INTO ABOVE_STR_OPS.BP_SUMMARY
WITH ALL_DATA AS (
                 SELECT TLAT.MONTH
                      , E.NAME
                      , TLAT.POSITION
                      , E.IS_ELIGIBLE
                      , sum(TOTAL_ALARM_TIME) / SUM(DAYS_WORKED) AS VALUE_ALL_AGENTS
                      , sum(IFF(NOT IS_NEW_HIRE, TOTAL_ALARM_TIME, 0)) /
                        SUM(IFF(NOT IS_NEW_HIRE, DAYS_WORKED, 0)) AS VALUE_EXCL_NEW_AGENTS
                 FROM ABOVE_STR_OPS.BP_TL_ALARM_TIME AS TLAT
                      LEFT JOIN ABOVE_STR_OPS.BP_ELIGIBLE AS E ON TLAT.MONTH = E.MONTH AND TLAT.LEAD_NAME = E.NAME
                 WHERE TLAT.MONTH = $BONUS_MONTH
                 GROUP BY 1, 2, 3, 4
                 )

SELECT DISTINCT
       D.MONTH
     , D.NAME
     , D.POSITION
     , D.IS_ELIGIBLE
     , M.METRIC_NAME
     , MAX(M.TARGET) OVER () AS METRIC_TARGET -- Return target for lowest bonus level
     , least(D.VALUE_ALL_AGENTS, D.VALUE_EXCL_NEW_AGENTS) AS METRIC_VALUE
     , round(METRIC_VALUE) AS METRIC_VALUE_ROUNDED
     , max(IFF(D.IS_ELIGIBLE AND METRIC_VALUE_ROUNDED <= M.TARGET, M.BONUS, 0))
           OVER (PARTITION BY D.NAME) AS BONUS_AMOUNT
     , D.VALUE_ALL_AGENTS
     , D.VALUE_EXCL_NEW_AGENTS
     , current_timestamp::TIMESTAMP_NTZ AS RUN_TIMESTAMP
FROM ALL_DATA AS D
     LEFT JOIN ABOVE_STR_OPS.BP_METRICS AS M
               ON D.POSITION = M.POSITION
                   AND M.METRIC_NAME = 'Team Alarm Time'
                   AND M.EFF_END_DATE IS NULL
;

-- TL Sendback Rate
INSERT INTO ABOVE_STR_OPS.BP_SUMMARY
WITH ALL_DATA AS (
                 SELECT TLSR.MONTH
                      , E.NAME
                      , TLSR.POSITION
                      , E.IS_ELIGIBLE
                      , sum(NUM_SENDBACKS) / SUM(NUM_DECISIONS) AS VALUE_ALL_AGENTS
                      , sum(IFF(NOT IS_NEW_HIRE, NUM_SENDBACKS, 0)) /
                        SUM(IFF(NOT IS_NEW_HIRE, NUM_DECISIONS, 0)) AS VALUE_EXCL_NEW_AGENTS
                 FROM ABOVE_STR_OPS.BP_TL_SENDBACK_RATE AS TLSR
                      LEFT JOIN ABOVE_STR_OPS.BP_ELIGIBLE AS E ON TLSR.MONTH = E.MONTH AND TLSR.LEAD_NAME = E.NAME
                 WHERE TLSR.MONTH = $BONUS_MONTH
                 GROUP BY 1, 2, 3, 4
                 )

SELECT DISTINCT
       D.MONTH
     , D.NAME
     , D.POSITION
     , D.IS_ELIGIBLE
     , M.METRIC_NAME
     , MAX(M.TARGET) OVER () AS METRIC_TARGET -- Return target for lowest bonus level
     , least(D.VALUE_ALL_AGENTS, D.VALUE_EXCL_NEW_AGENTS) AS METRIC_VALUE
     , round(METRIC_VALUE, 3) AS METRIC_VALUE_ROUNDED
     , max(IFF(D.IS_ELIGIBLE AND METRIC_VALUE_ROUNDED <= M.TARGET, M.BONUS, 0))
           OVER (PARTITION BY D.NAME) AS BONUS_AMOUNT
     , D.VALUE_ALL_AGENTS
     , D.VALUE_EXCL_NEW_AGENTS
     , current_timestamp::TIMESTAMP_NTZ AS RUN_TIMESTAMP
FROM ALL_DATA AS D
     LEFT JOIN ABOVE_STR_OPS.BP_METRICS AS M
               ON D.POSITION = M.POSITION
                   AND M.METRIC_NAME = 'Team Sendback Rate'
                   AND M.EFF_END_DATE IS NULL
;

-- TL QA Score
INSERT INTO ABOVE_STR_OPS.BP_SUMMARY
WITH ALL_DATA AS (
                 SELECT TLQA.MONTH
                      , E.NAME
                      , TLQA.POSITION
                      , E.IS_ELIGIBLE
                      , sum(AVG_SCORE * NUM_EVALUATIONS) / SUM(NUM_EVALUATIONS) AS VALUE_ALL_AGENTS
                      , sum(IFF(NOT IS_NEW_HIRE, AVG_SCORE * NUM_EVALUATIONS, 0)) /
                        SUM(IFF(NOT IS_NEW_HIRE, NUM_EVALUATIONS, 0)) AS VALUE_EXCL_NEW_AGENTS
                 FROM ABOVE_STR_OPS.BP_TL_QA_SCORES AS TLQA
                      LEFT JOIN ABOVE_STR_OPS.BP_ELIGIBLE AS E ON TLQA.MONTH = E.MONTH AND TLQA.LEAD_NAME = E.NAME
                 WHERE TLQA.MONTH = $BONUS_MONTH
                 GROUP BY 1, 2, 3, 4
                 )

SELECT DISTINCT
       D.MONTH
     , D.NAME
     , D.POSITION
     , D.IS_ELIGIBLE
     , M.METRIC_NAME
     , min(M.TARGET) OVER () AS METRIC_TARGET -- Return target for lowest bonus level
     , greatest(D.VALUE_ALL_AGENTS, D.VALUE_EXCL_NEW_AGENTS) AS METRIC_VALUE
     , round(METRIC_VALUE, 2) AS METRIC_VALUE_ROUNDED
     , max(IFF(D.IS_ELIGIBLE AND METRIC_VALUE_ROUNDED >= M.TARGET, M.BONUS, 0))
           OVER (PARTITION BY D.NAME) AS BONUS_AMOUNT
     , D.VALUE_ALL_AGENTS
     , D.VALUE_EXCL_NEW_AGENTS
     , current_timestamp::TIMESTAMP_NTZ AS RUN_TIMESTAMP
FROM ALL_DATA AS D
     LEFT JOIN ABOVE_STR_OPS.BP_METRICS AS M
               ON D.POSITION = M.POSITION
                   AND M.METRIC_NAME = 'Team QA Score'
                   AND M.EFF_END_DATE IS NULL
;


--------------------------------------------------------------

-- OUTPUTS
SELECT *
FROM ABOVE_STR_OPS.BP_SUMMARY
WHERE MONTH = $BONUS_MONTH AND POSITION IN ('Collection/Hybrid', 'Customer Care')
ORDER BY 1, 2, 3;


SELECT *
FROM ABOVE_STR_OPS.BP_ALARM_TIME
WHERE MONTH = $BONUS_MONTH AND POSITION IN ('Collection/Hybrid', 'Customer Care')
ORDER BY 1, 2, 3;


SELECT *
FROM ABOVE_STR_OPS.BP_QA_SCORES
WHERE MONTH = $BONUS_MONTH AND POSITION IN ('Collection/Hybrid', 'Customer Care')
ORDER BY 1, 2, 3;


SELECT *
FROM ABOVE_STR_OPS.BP_SENDBACK_RATE
WHERE MONTH = $BONUS_MONTH AND POSITION IN ('Collection/Hybrid', 'Customer Care')
ORDER BY 1, 2, 3;


SELECT *
FROM ABOVE_STR_OPS.BP_TL_ALARM_TIME
WHERE MONTH = $BONUS_MONTH AND POSITION IN ('Collection/Hybrid', 'Customer Care')
ORDER BY 1, 2, 3;


SELECT *
FROM ABOVE_STR_OPS.BP_TL_QA_SCORES
WHERE MONTH = $BONUS_MONTH AND POSITION IN ('Collection/Hybrid', 'Customer Care')
ORDER BY 1, 2, 3;


SELECT *
FROM ABOVE_STR_OPS.BP_TL_SENDBACK_RATE
WHERE MONTH = $BONUS_MONTH AND POSITION IN ('Collection/Hybrid', 'Customer Care')
ORDER BY 1, 2, 3;


-- QA RESULTS
SELECT MONTH, COUNT(*)
FROM ABOVE_STR_OPS.BP_ELIGIBLE
GROUP BY 1;
SELECT MONTH, COUNT(*)
FROM ABOVE_STR_OPS.BP_ALARM_TIME
GROUP BY 1;
SELECT MONTH, COUNT(*)
FROM ABOVE_STR_OPS.BP_QA_SCORES
GROUP BY 1;
SELECT MONTH, COUNT(*)
FROM ABOVE_STR_OPS.BP_SENDBACK_RATE
GROUP BY 1;
SELECT MONTH, COUNT(*)
FROM ABOVE_STR_OPS.BP_TL_ALARM_TIME
GROUP BY 1;
SELECT MONTH, COUNT(*)
FROM ABOVE_STR_OPS.BP_TL_QA_SCORES
GROUP BY 1;
SELECT MONTH, COUNT(*)
FROM ABOVE_STR_OPS.BP_TL_SENDBACK_RATE
GROUP BY 1;
SELECT MONTH, COUNT(*)
FROM ABOVE_STR_OPS.BP_SUMMARY
GROUP BY 1;


SELECT MONTH
     , POSITION
     , COUNT(DISTINCT NAME) AS TOTAL_PEOPLE
     , COUNT(DISTINCT CASE WHEN IS_ELIGIBLE THEN NAME END) AS TOTAL_ELIGIBLE
     , SUM(BONUS_AMOUNT) AS TOTAL_BONUS
     , TOTAL_BONUS / TOTAL_PEOPLE AS AVG_BONUS_ALL
     , TOTAL_BONUS / TOTAL_ELIGIBLE AS AVG_BONUS_ELIGIBLE
FROM ABOVE_STR_OPS.BP_SUMMARY
GROUP BY 1, 2
;