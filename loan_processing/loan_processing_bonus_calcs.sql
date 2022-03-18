SET START_DATE = '2022-03-07'::DATE;
SET END_DATE = '2022-03-11'::DATE;

WITH TEMPY AS (
              SELECT USER_NAME
                   , sum(CASE WHEN STATUS_LABEL = 'Personal' THEN STATUS_TIME ELSE 0 END) / 3600 AS PERSONAL
                   , sum(CASE WHEN STATUS_LABEL = 'Break' THEN STATUS_TIME ELSE 0 END) / 3600 AS BREAK
                   , sum(CASE WHEN STATUS_LABEL = 'Lunch' THEN STATUS_TIME ELSE 0 END) / 3600 AS LUNCH
                   , sum(STATUS_TIME) / 3600 - LUNCH AS TOTAL_DURATION
                   , sum(CASE WHEN STATUS_LABEL = 'After Call Work' THEN STATUS_TIME ELSE 0 END) /
                     3600 AS AFTER_CALL_WORK
              FROM TALKDESK.USER_STATUS
              WHERE TRUE
                AND (STATUS_START_AT - INTERVAL '6 hour')::DATE BETWEEN $START_DATE AND $END_DATE
                AND STATUS_LABEL <> 'Offline'
              GROUP BY 1
              )

   , CALL_DATA AS (
                  SELECT USER_NAME
                       , (START_AT - INTERVAL '6 hour')::DATE AS WORK_DATE
                       , count(*) AS NUM_CALLS
                       , count(DISPOSITION_CODE) AS DISPOSITIONED_CALLS
                  FROM TALKDESK.CALLS
                  WHERE TRUE
                    AND (START_AT - INTERVAL '6 hour')::DATE BETWEEN $START_DATE AND $END_DATE
                  GROUP BY 1, 2
                  )

   , CALL_DATA_SUMMARIZED AS (
                             SELECT USER_NAME
                                  , SUM(NUM_CALLS) AS NUM_CALLS
                                  , SUM(DISPOSITIONED_CALLS) AS DISPOSITIONED_CALLS
                                  , count(DISTINCT WORK_DATE) AS WORK_DAYS
                             FROM CALL_DATA
                             WHERE USER_NAME IS NOT NULL
                             GROUP BY 1
                             )

SELECT T.USER_NAME
     , CD.NUM_CALLS
     , CD.DISPOSITIONED_CALLS
     , PERSONAL
     , BREAK
     , LUNCH
     , TOTAL_DURATION
     , AFTER_CALL_WORK
     , CD.WORK_DAYS
     , T.AFTER_CALL_WORK / CD.NUM_CALLS * 3600 AS AVG_ACW_SECONDS
     , CD.DISPOSITIONED_CALLS / CD.NUM_CALLS AS PERC_DISPOSITIONED
     , (GREATEST(PERSONAL, 0) + GREATEST(BREAK - WORK_DAYS * .5, 0) + GREATEST(LUNCH - WORK_DAYS * 1, 0) +
       GREATEST(WORK_DAYS * 8 - TOTAL_DURATION, 0)) * 60 AS ALARM_TIME_MINS
FROM TEMPY T
     LEFT JOIN CALL_DATA_SUMMARIZED CD ON T.USER_NAME = CD.USER_NAME
     JOIN (
          SELECT USER_NAME, COUNT(*) AS NUM_CALLS
          FROM TALKDESK.CALLS
          WHERE TRUE
            AND RING_GROUPS IN ('loan processing', 'loan processing+loan processing overflow')
            AND (START_AT - INTERVAL '6 hour')::DATE BETWEEN $START_DATE AND $END_DATE
          GROUP BY 1
          ) C ON T.USER_NAME = C.USER_NAME AND C.NUM_CALLS >= 10
ORDER BY 1
;