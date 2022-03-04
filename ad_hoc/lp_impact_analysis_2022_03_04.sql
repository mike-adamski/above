/*
Doc resubmissions
When applicants submit docs to verifications, how often do we need to ask for more information?
How often does the applicant re-send what we're asking for?
Does reaching back out sooner increase the chances of them re-submitting?
 */

WITH ALL_DOCS AS (
                 SELECT L.*
                      , T.ID AS TODO_ID
                      , T.TYPE AS TODO_TYPE
                      , T.STATUS AS TODO_STATUS
                      , CONVERT_TIMEZONE('America/Chicago', T.CREATED_AT) AS TODO_CREATED_TS
                      , TD.ID AS TODO_DOC_ID
                      , TD.NAME AS TODO_DOC_NAME
                      , TD.STATUS AS TODO_DOC_STATUS
                      , CONVERT_TIMEZONE('America/Chicago', TD.CREATED_AT) AS TODO_DOC_CREATED_TS
                      , IFF(LSH.ID IS NOT NULL, 1, 0) AS DECISIONED_FLAG
                      , CONVERT_TIMEZONE('America/Chicago', LSH.UPDATED_AT) AS DECISIONED_TS
                 FROM ABOVE_PUBLIC.LOANS L
                      JOIN ABOVE_PUBLIC.TODOS T ON L.ID = T.LOAN_ID
                      JOIN ABOVE_PUBLIC.TODO_DOCS TD ON T.ID = TD.TODO_ID
                      LEFT JOIN (
                                SELECT *
                                FROM ABOVE_PUBLIC.LOAN_STATUS_HISTORY
                                WHERE NEW_STATUS IN (
                                                     'IPL_ONBOARDED',
                                                     'IPL_APPROVED',
                                                     'IPL_INITIAL_TIL_SUBMIT',
                                                     'IPL_BACK_END_DECLINED'
                                    )
                                    QUALIFY RANK() OVER (PARTITION BY LOAN_ID ORDER BY UPDATED_AT) = 1
                                ) LSH ON L.ID = LSH.LOAN_ID
                 WHERE PRODUCT_TYPE = 'IPL'
                   AND T.TYPE <> 'payment_adherence'
--                    AND T.TYPE = 'bank'
                   AND L.CREATED_AT <= current_date - 7
                   AND L.UNIFIED_ID IS NOT NULL
                 )
   , ALL_DOCS_CURATED AS (
                         SELECT AD.*
                              , LAS.NAME AS LOAN_STATUS
                              , RANK() OVER (PARTITION BY AD.ID ORDER BY TODO_DOC_CREATED_TS) AS DOC_SUBMIT_NUM
                              , IFF(RANK() OVER (PARTITION BY AD.ID ORDER BY TODO_DOC_CREATED_TS DESC) = 1, 1,
                                    0) AS LAST_DOC_FLAG
                         FROM ALL_DOCS AD
                              LEFT JOIN ABOVE_PUBLIC.LOAN_APP_STATUSES LAS ON AD.LOAN_APP_STATUS_ID = LAS.ID
                         WHERE NOT EXISTS(SELECT 1
                                          FROM ALL_DOCS
                                          WHERE ID = AD.ID
                                            AND TODO_DOC_CREATED_TS > AD.TODO_DOC_CREATED_TS
                                            AND TODO_DOC_CREATED_TS < AD.TODO_DOC_CREATED_TS + INTERVAL '10 minutes'
                             )
                           -- de-dupe when multiple docs are uploaded at the same time
                           AND NOT EXISTS(SELECT 1
                                          FROM ALL_DOCS
                                          WHERE ID = AD.ID
                                            AND TODO_DOC_CREATED_TS = AD.TODO_DOC_CREATED_TS
                                            AND TODO_DOC_ID < AD.TODO_DOC_ID
                             )
                         )

   , ALL_DOC_DATA AS (
                     SELECT ID AS LOAN_ID
                          , UNIFIED_ID
                          , PROGRAM_ID
                          , CONVERT_TIMEZONE('America/Chicago', CREATED_AT)::DATE AS APP_SUBMIT_DATE
--      , LOAN_STATUS
                          , TODO_DOC_ID
                          , C.TODO_DOC_NAME
                          , TODO_DOC_CREATED_TS
                          , TODO_TYPE
                          , TODO_DOC_STATUS
                          , DOC_SUBMIT_NUM
                          , CASE
                                WHEN DECISIONED_FLAG = 1 AND LAST_DOC_FLAG = 1 THEN 0
                                ELSE 1
                                END AS ADDL_DOCS_NEEDED_FLAG
                          , CASE
--                                 WHEN ADDL_DOCS_NEEDED_FLAG = 0 THEN NULL
                                WHEN EXISTS(
                                        SELECT 1
                                        FROM ALL_DOCS_CURATED
                                        WHERE ID = C.ID AND DOC_SUBMIT_NUM = C.DOC_SUBMIT_NUM + 1)
                                    THEN 1
                                ELSE 0
                                END AS NEXT_DOC_SUBMITTED
                     FROM ALL_DOCS_CURATED C
                     )

   , TALKDESK_DATA AS (
                      SELECT C.CALL_ID
                           , CONVERT_TIMEZONE('America/Chicago', C.START_AT) AS CALL_TS
                           , L.PROGRAM_ID
                           , right(CONTACT_PHONE_NUMBER, 10) AS PHONE_CLEAN
                           , CONTACT_PHONE_NUMBER
                           , DISPOSITION_CODE
                           , RING_GROUPS
                           , TALKDESK_PHONE_DISPLAY_NAME
                      FROM TALKDESK.CALLS C
                           JOIN ABOVE_PUBLIC.LEADS L
                                ON right(C.CONTACT_PHONE_NUMBER, 10) = L.PHONE_NUMBER
                                    AND CONVERT_TIMEZONE('America/Chicago', C.START_AT) >=
                                        CONVERT_TIMEZONE('America/Chicago', L.CREATED_AT)
                          QUALIFY rank() OVER (PARTITION BY C.CALL_ID ORDER BY L.CREATED_AT) = 1
                      )

SELECT DD.LOAN_ID
     , DD.UNIFIED_ID
     , DD.PROGRAM_ID
     , DD.APP_SUBMIT_DATE
     , DD.TODO_DOC_ID
     , DD.TODO_DOC_CREATED_TS::TIMESTAMP AS TODO_DOC_CREATED_TS
     , TODO_DOC_CREATED_TS::DATE - dayofweek(TODO_DOC_CREATED_TS::DATE - 6) AS DOC_SUBMIT_WEEK
     , DD.TODO_TYPE
     , DD.TODO_DOC_STATUS
     , DD.DOC_SUBMIT_NUM
     , DD.ADDL_DOCS_NEEDED_FLAG
     , DD.NEXT_DOC_SUBMITTED
     , TD.CALL_ID
     , TD.CALL_TS AS NEXT_CALL_TS
     , DATEDIFF('hour', DD.TODO_DOC_CREATED_TS, NEXT_CALL_TS) AS RESPONSE_TIME_HRS
     , CASE
           WHEN RESPONSE_TIME_HRS BETWEEN 0 AND 1 THEN '0-1'
           WHEN RESPONSE_TIME_HRS BETWEEN 2 AND 4 THEN '2-4'
           WHEN RESPONSE_TIME_HRS BETWEEN 5 AND 12 THEN '5-12'
           WHEN RESPONSE_TIME_HRS BETWEEN 13 AND 24 THEN '13-24'
           WHEN RESPONSE_TIME_HRS BETWEEN 25 AND 48 THEN '25-48'
           WHEN RESPONSE_TIME_HRS BETWEEN 49 AND 72 THEN '49-72'
           WHEN RESPONSE_TIME_HRS > 72 THEN '72+'
           END AS RESPONSE_TIME_HRS_GROUP
FROM ALL_DOC_DATA DD
     LEFT JOIN TALKDESK_DATA TD ON DD.PROGRAM_ID = TD.PROGRAM_ID AND TD.CALL_TS >= DD.TODO_DOC_CREATED_TS
    QUALIFY RANK() OVER (PARTITION BY DD.TODO_DOC_ID ORDER BY TD.CALL_TS) = 1
ORDER BY 1, 5
;


----------------------------------------------------------

WITH PENDING_LOANS AS (
                      SELECT *
                      FROM ABOVE_PUBLIC.LOAN_APP_STATUSES
                      LIMIT 10
                      )


   , TALKDESK_DATA AS (
                      SELECT C.CALL_ID
                           , CONVERT_TIMEZONE('America/Chicago', C.START_AT) AS CALL_TS
                           , L.PROGRAM_ID
                           , right(CONTACT_PHONE_NUMBER, 10) AS PHONE_CLEAN
                           , CONTACT_PHONE_NUMBER
                           , DISPOSITION_CODE
                           , RING_GROUPS
                           , TALKDESK_PHONE_DISPLAY_NAME
                      FROM TALKDESK.CALLS C
                           JOIN ABOVE_PUBLIC.LEADS L
                                ON right(C.CONTACT_PHONE_NUMBER, 10) = L.PHONE_NUMBER
                                    AND CONVERT_TIMEZONE('America/Chicago', C.START_AT) >=
                                        CONVERT_TIMEZONE('America/Chicago', L.CREATED_AT)
                          QUALIFY rank() OVER (PARTITION BY C.CALL_ID ORDER BY L.CREATED_AT) = 1
                      )


SELECT L.ID AS LOAN_ID
     , L.UNIFIED_ID
     , L.CREATED_AT
     , ENTERED_PENDING
     , LEFT_PENDING
FROM ABOVE_PUBLIC.LOANS L
     LEFT JOIN (
               SELECT LOAN_ID, MIN(UPDATED_AT) AS ENTERED_PENDING
               FROM ABOVE_PUBLIC.LOAN_STATUS_HISTORY
               WHERE NEW_STATUS = 'IPL_PENDING'
               GROUP BY 1
               ) LSH1 ON LSH1.LOAN_ID = L.ID
     LEFT JOIN (
               SELECT LOAN_ID, MAX(UPDATED_AT) AS LEFT_PENDING
               FROM ABOVE_PUBLIC.LOAN_STATUS_HISTORY
               WHERE OLD_STATUS = 'IPL_PENDING'
               GROUP BY 1
               ) LSH2 ON LSH2.LOAN_ID = L.ID
WHERE L.PRODUCT_TYPE = 'IPL'
  AND ENTERED_PENDING IS NOT NULL
ORDER BY 3


SELECT UPDATED_AT::DATE AS UPDATED_DATE
     , COUNT(*) AS TOT_LEADS
     , count(DISTINCT PROGRAM_ID) AS DISTINCT_LEADS
FROM ABOVE_PUBLIC.LEADS
GROUP BY 1
ORDER BY 1 DESC

SELECT *
FROM ABOVE_PUBLIC.LEADS
WHERE TYPE = 'IPL'

SELECT PROGRAM_ID
FROM ABOVE_PUBLIC.LOANS
WHERE UNIFIED_ID = '44073017'
SELECT *
FROM ABOVE_PUBLIC.LOAN_APP_STATUSES


SELECT *
FROM ABOVE_PUBLIC.LOANS
WHERE PROGRAM_ID = 'BRP-000972'
SELECT *
FROM ABOVE_PUBLIC.LOAN_STATUS_HISTORY
WHERE UNIFIED_ID = 61273674



WITH FIRST_TARGETED AS (
                       SELECT PROGRAM_NAME, MIN(LOADED_DATE) AS FIRST_TARGET_DATE
                       FROM BEYOND.OUTBOUND_ABOVELENDING.VW_IPL_TARGETED
                       WHERE IS_TARGETED
                       GROUP BY 1
                       )
SELECT date_trunc('week', FIRST_TARGET_DATE)::DATE AS WEEK_OF, count(*)
FROM FIRST_TARGETED
GROUP BY 1
ORDER BY 1 DESC
;

SELECT DIALER_CAMPAIGN, count(*)
FROM BEYOND.OUTBOUND_ABOVELENDING.VW_IPL_TARGETED
WHERE IS_TARGETED AND LOADED_DATE = current_date
GROUP BY 1


SELECT L.ID
     , L.CREATED_AT
     , L.UNIFIED_ID
     , L.CODE
     , L.PROGRAM_ID
     , B.*
FROM ABOVE_PUBLIC.LOANS L
     JOIN ABOVE_PUBLIC.BORROWERS B ON L.BORROWER_ID = B.ID
WHERE L.PRODUCT_TYPE = 'IPL'
  AND L.PROGRAM_ID IS NULL
;



SELECT date_trunc('month', ENROLLED_DATE_CST) AS MONTH
     , count(*)
FROM ABOVE_DW_PROD.BEYOND.IPL_PROGRAM
WHERE SERVICE_ENTITY_NAME = 'Five Lakes Law Group'
GROUP BY 1
ORDER BY 1;
