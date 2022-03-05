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
                      SELECT L.ID AS LOAN_ID
                           , L.UNIFIED_ID
                           , convert_timezone('America/Chicago', L.CREATED_AT) AS APP_SUBMIT_TS
                           , BAI.PHONE_NUMBER
                           , ENTERED_PENDING
                           , coalesce(LEFT_PENDING, current_timestamp) AS LEFT_PENDING
                           , IFF(LEFT_PENDING IS NULL AND LAS.NAME <> 'EXPIRED', 1, 0) AS IN_FLIGHT
                      FROM ABOVE_PUBLIC.LOANS L
                           JOIN ABOVE_PUBLIC.LOAN_APP_STATUSES LAS ON L.LOAN_APP_STATUS_ID = LAS.ID
                           LEFT JOIN ABOVE_PUBLIC.BORROWER_ADITIONAL_INFO BAI ON L.ID = BAI.LOAN_ID
                           LEFT JOIN (
                                     SELECT LOAN_ID
                                          , convert_timezone('America/Chicago', MIN(UPDATED_AT)) AS ENTERED_PENDING
                                     FROM ABOVE_PUBLIC.LOAN_STATUS_HISTORY
                                     WHERE NEW_STATUS = 'IPL_PENDING'
                                     GROUP BY 1
                                     ) LSH1 ON LSH1.LOAN_ID = L.ID
                           LEFT JOIN (
                                     SELECT LOAN_ID
                                          , convert_timezone('America/Chicago', MAX(UPDATED_AT)) AS LEFT_PENDING
                                     FROM ABOVE_PUBLIC.LOAN_STATUS_HISTORY
                                     WHERE OLD_STATUS = 'IPL_PENDING'
                                     GROUP BY 1
                                     ) LSH2 ON LSH2.LOAN_ID = L.ID
                      WHERE L.PRODUCT_TYPE = 'IPL'
                        AND ENTERED_PENDING IS NOT NULL
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

SELECT PL.LOAN_ID
     , PL.UNIFIED_ID
     , PL.APP_SUBMIT_TS::TIMESTAMP_NTZ AS APP_SUBMIT_TS
     , IN_FLIGHT
     , count(*) AS NUM_DIALS_IN_PENDING
FROM PENDING_LOANS PL
     LEFT JOIN TALKDESK_DATA TD
               ON PL.PHONE_NUMBER = TD.PHONE_CLEAN
                   AND TD.CALL_TS BETWEEN PL.ENTERED_PENDING AND PL.LEFT_PENDING
GROUP BY 1, 2, 3, 4
;


----------------------------------------------------------------------------

-- CREATE OR REPLACE VIEW ABOVE_PUBLIC.VERIFICATION_FUNNEL_V AS
WITH DOCUMENT_DATA AS (
                      SELECT DISTINCT
                             L.ID AS LOAN_ID
                           , L.UNIFIED_ID
                           , L.CREATED_AT AS LOAN_CREATED_TS
                           , CONVERT_TIMEZONE('America/Chicago', L.CREATED_AT)::DATE AS APPLICATION_DATE
                           , LEAD.MONTHS_SINCE_ENROLLMENT
                           , L.UPDATED_AT AS LOAN_UPDATED_TS
                           , LAS.NAME AS LOAN_STATUS
                           , NEXT_STATUS.NEW_STATUS AS NEXT_LOAN_STATUS
                           , NEXT_STATUS.UPDATED_AT AS NEXT_LOAN_STATUS_UPDATED_TS
                           , DATEDIFF('DAY', L.CREATED_AT, NEXT_LOAN_STATUS_UPDATED_TS) -
                             iff(LOAN_CREATED_TS::TIME > NEXT_LOAN_STATUS_UPDATED_TS::TIME, 1, 0) AS DAYS_TO_NEXT_STATUS
                           , T.ID AS TODO_ID
                           , T.TYPE AS TODO_TYPE
                           , T.STATUS AS TODO_STATUS

                           , CASE
                                 WHEN LOAN_STATUS IN ('APPROVED', 'ONBOARDED', 'INITIAL_TIL_SUBMIT') THEN 1
                                 WHEN D.NAME IS NOT NULL AND T.STATUS = 'approved' THEN 1
                                 ELSE 0
                                 END AS TODO_DOC_APPROVED_FLAG
                           , CASE
                                 WHEN TODO_DOC_APPROVED_FLAG = 1 THEN 1
                                 WHEN D.NAME IS NOT NULL THEN 1
                                 ELSE 0
                                 END AS TODO_DOC_SUBMITTED_FLAG
                           , CASE
                                 WHEN D.NAME IS NULL AND T.STATUS = 'approved' THEN 0
                                 WHEN D.NAME IS NULL AND T.STATUS <> 'approved' THEN 1
                                 ELSE 1
                                 END AS TODO_DOC_NEEDED_FLAG

                           , CASE
                                 WHEN TODO_DOC_APPROVED_FLAG = 1
                                     THEN COALESCE(T.UPDATED_AT, NEXT_STATUS.UPDATED_AT, L.UPDATED_AT)
                                 END AS APPROVE_TS

                           , MIN(COALESCE(D.CREATED_AT, APPROVE_TS)) OVER (PARTITION BY L.ID, T.ID) AS FIRST_SUBMIT_TS
                           , DATEDIFF('DAY', L.CREATED_AT, APPROVE_TS) -
                             iff(LOAN_CREATED_TS::TIME > APPROVE_TS::TIME, 1, 0) AS DAYS_TO_APPROVE
                           , DATEDIFF('DAY', L.CREATED_AT, FIRST_SUBMIT_TS) -
                             iff(LOAN_CREATED_TS::TIME > FIRST_SUBMIT_TS::TIME, 1, 0) AS DAYS_TO_SUBMIT

                      FROM ABOVE_PUBLIC.LOANS L
                           LEFT JOIN ABOVE_PUBLIC.TODOS T
                                     ON T.LOAN_ID = L.ID
                           LEFT JOIN ABOVE_PUBLIC.TODO_DOCS D ON T.ID = D.TODO_ID
                           LEFT JOIN ABOVE_PUBLIC.LOAN_APP_STATUSES LAS ON L.LOAN_APP_STATUS_ID = LAS.ID
                           LEFT JOIN (
                                     SELECT *
                                     FROM ABOVE_PUBLIC.LOAN_STATUS_HISTORY
                                     WHERE OLD_STATUS = 'IPL_PENDING'
                                         QUALIFY RANK() OVER (PARTITION BY LOAN_ID ORDER BY UPDATED_AT DESC) = 1
                                     ) NEXT_STATUS ON L.ID = NEXT_STATUS.LOAN_ID
                           LEFT JOIN (
                                     SELECT *
                                     FROM ABOVE_PUBLIC.LEADS
                                         QUALIFY RANK() OVER (PARTITION BY PROGRAM_ID ORDER BY CREATED_AT DESC) = 1
                                     ) LEAD ON LEAD.PROGRAM_ID = L.PROGRAM_ID
                      WHERE L.CREATED_AT >= '2021-11-01'
                        AND T.TYPE <> 'payment_adherence'
                        AND L.PRODUCT_TYPE = 'IPL'
                        AND TODO_DOC_NEEDED_FLAG = 1
                      )


   , DOC_GROUPING_CLASSIFICATION AS (
                                    SELECT DISTINCT
                                           D.LOAN_ID
                                         , CASE
                                               WHEN exists(SELECT 1
                                                           FROM DOCUMENT_DATA
                                                           WHERE LOAN_ID = D.LOAN_ID AND TODO_TYPE = 'bank') THEN 1
                                               ELSE 0
                                               END AS DOC_NEEDED_BANK_STATEMENT
                                         , CASE
                                               WHEN exists(SELECT 1
                                                           FROM DOCUMENT_DATA
                                                           WHERE LOAN_ID = D.LOAN_ID AND TODO_TYPE = 'identity') THEN 1
                                               ELSE 0
                                               END AS DOC_NEEDED_IDENTITY
                                         , CASE
                                               WHEN exists(SELECT 1
                                                           FROM DOCUMENT_DATA
                                                           WHERE LOAN_ID = D.LOAN_ID AND TODO_TYPE = 'residence') THEN 1
                                               ELSE 0
                                               END AS DOC_NEEDED_RESIDENCE
                                         , CASE
                                               WHEN exists(SELECT 1
                                                           FROM DOCUMENT_DATA
                                                           WHERE LOAN_ID = D.LOAN_ID AND TODO_TYPE = 'ofac') THEN 1
                                               ELSE 0
                                               END AS DOC_NEEDED_OFAC
                                         , CASE
                                               WHEN exists(SELECT 1
                                                           FROM DOCUMENT_DATA
                                                           WHERE LOAN_ID = D.LOAN_ID AND TODO_TYPE = 'fraud_alert')
                                                   THEN 1
                                               ELSE 0
                                               END AS DOC_NEEDED_FRAUD_ALERT
                                         , CASE
                                               WHEN exists(
                                                       SELECT 1 FROM DOCUMENT_DATA WHERE LOAN_ID = D.LOAN_ID AND TODO_TYPE = 'nsf')
                                                   THEN 1
                                               ELSE 0
                                               END AS DOC_NEEDED_NSF
                                         , CASE
                                               WHEN exists(SELECT 1
                                                           FROM DOCUMENT_DATA
                                                           WHERE LOAN_ID = D.LOAN_ID AND TODO_TYPE = 'fraud') THEN 1
                                               ELSE 0
                                               END AS DOC_NEEDED_FRAUD
                                         , CASE
                                               WHEN exists(SELECT 1
                                                           FROM DOCUMENT_DATA
                                                           WHERE LOAN_ID = D.LOAN_ID AND TODO_TYPE = 'consumer_statement')
                                                   THEN 1
                                               ELSE 0
                                               END AS DOC_NEEDED_CONSUMER_STATEMENT
                                         , CASE
                                               WHEN exists(SELECT 1
                                                           FROM DOCUMENT_DATA
                                                           WHERE LOAN_ID = D.LOAN_ID AND TODO_TYPE = 'active_duty')
                                                   THEN 1
                                               ELSE 0
                                               END AS DOC_NEEDED_ACTIVE_DUTY
                                         , CASE
                                               WHEN exists(SELECT 1
                                                           FROM DOCUMENT_DATA
                                                           WHERE LOAN_ID = D.LOAN_ID AND TODO_TYPE = 'income') THEN 1
                                               ELSE 0
                                               END AS DOC_NEEDED_INCOME
                                         , CASE
                                               WHEN exists(SELECT 1
                                                           FROM DOCUMENT_DATA
                                                           WHERE LOAN_ID = D.LOAN_ID
                                                             AND TODO_TYPE NOT IN ('income', 'bank', 'identity', 'residence'))
                                                   THEN 1
                                               ELSE 0
                                               END AS DOC_NEEDED_OTHER
                                         , CASE
                                               WHEN DOC_NEEDED_OTHER = 1 THEN 'Other'
                                               WHEN least(DOC_NEEDED_INCOME) = 1
                                                   AND greatest(DOC_NEEDED_IDENTITY, DOC_NEEDED_RESIDENCE) = 0
                                                   THEN 'Income'
                                               WHEN least(DOC_NEEDED_IDENTITY) = 1
                                                   AND greatest(DOC_NEEDED_RESIDENCE, DOC_NEEDED_INCOME) = 0
                                                   THEN 'Identity'
                                               WHEN least(DOC_NEEDED_RESIDENCE) = 1
                                                   AND greatest(DOC_NEEDED_IDENTITY, DOC_NEEDED_INCOME) = 0
                                                   THEN 'Residence'
                                               WHEN least(DOC_NEEDED_INCOME, DOC_NEEDED_IDENTITY) = 1
                                                   AND greatest(DOC_NEEDED_RESIDENCE) = 0
                                                   THEN 'Income + Identity'
                                               WHEN least(DOC_NEEDED_INCOME, DOC_NEEDED_RESIDENCE) = 1
                                                   AND greatest(DOC_NEEDED_IDENTITY) = 0
                                                   THEN 'Income + Residence'
                                               WHEN least(DOC_NEEDED_IDENTITY, DOC_NEEDED_RESIDENCE) = 1
                                                   AND greatest(DOC_NEEDED_INCOME) = 0
                                                   THEN 'Identity + Residence'
                                               WHEN least(DOC_NEEDED_INCOME, DOC_NEEDED_IDENTITY, DOC_NEEDED_RESIDENCE) = 1
                                                   THEN 'Income + Identity + Residence'
                                               WHEN greatest(DOC_NEEDED_IDENTITY, DOC_NEEDED_RESIDENCE, DOC_NEEDED_INCOME) =
                                                    0
                                                   THEN 'Fastest Path (Bank only)'
                                               END AS DOC_VERIFICATION_GROUP_NEW
                                    FROM DOCUMENT_DATA AS D
                                    )

   , APPLICATION_DATA AS (
                         SELECT DISTINCT
                                D.LOAN_ID
                              , UNIFIED_ID
                              , APPLICATION_DATE
                              , MONTHS_SINCE_ENROLLMENT
                              , DOC_VERIFICATION_GROUP_NEW
                              , LOAN_STATUS
                              , DOC_NEEDED_BANK_STATEMENT
                              , DOC_NEEDED_IDENTITY
                              , DOC_NEEDED_RESIDENCE
                              , DOC_NEEDED_INCOME
                              , DOC_NEEDED_OTHER
                              , CASE
                                    WHEN LOAN_STATUS IN ('APPROVED', 'ONBOARDED', 'INITIAL_TIL_SUBMIT') THEN 1
                                    WHEN NEXT_LOAN_STATUS IN ('IPL_APPROVED', 'IPL_INITIAL_TIL_SUBMIT', 'IPL_ONBOARDED')
                                        THEN 1
                                    ELSE 0
                                    END AS APPROVED_FLAG
                              , CASE
                                    WHEN APPROVED_FLAG = 1 THEN 1
                                    WHEN LOAN_STATUS IN ('BACK_END_DECLINED') OR
                                         NEXT_LOAN_STATUS IN ('IPL_BACK_END_DECLINED') THEN 1
                                    WHEN NEXT_LOAN_STATUS IN ('IPL_EXPIRED', 'IPL_PENDING', 'IPL_WITHDRAWN')
                                        THEN 0
                                    ELSE 0
                                    END AS DECISION_FLAG
                              , CASE WHEN DECISION_FLAG = 1 THEN DAYS_TO_NEXT_STATUS END AS DAYS_TO_DECISION
                              , CASE WHEN APPROVED_FLAG = 1 THEN DAYS_TO_NEXT_STATUS END AS DAYS_TO_APPROVAL
                              , CASE
                                    WHEN DECISION_FLAG = 1 OR
                                         NOT exists(SELECT 1
                                                    FROM DOCUMENT_DATA
                                                    WHERE LOAN_ID = D.LOAN_ID AND TODO_DOC_SUBMITTED_FLAG = 0)
                                        THEN max(DAYS_TO_SUBMIT) OVER (PARTITION BY D.LOAN_ID)
                                    END AS DAYS_TO_ALL_DOCS_SUBMIT
                         FROM DOCUMENT_DATA D
                              LEFT JOIN DOC_GROUPING_CLASSIFICATION DGC ON D.LOAN_ID = DGC.LOAN_ID
                         )
   , DAY_SEQUENCE AS (
                     SELECT row_number() OVER (ORDER BY seq4()) - 1 AS DAYS
                     FROM TABLE (generator(ROWCOUNT => 29))
                     )

-- Total application funnel by day
SELECT APPLICATION_DATE - dayofweek(APPLICATION_DATE - 6) AS APPLICATION_WEEK
     , APPLICATION_DATE
     , UNIFIED_ID
     , DOC_VERIFICATION_GROUP_NEW AS DOC_VERIFICATION_GROUP
     , MONTHS_SINCE_ENROLLMENT
     , CASE
           WHEN MONTHS_SINCE_ENROLLMENT < 6 THEN '1) T3-5'
           WHEN MONTHS_SINCE_ENROLLMENT BETWEEN 6 AND 11 THEN '2) T6-11'
           WHEN MONTHS_SINCE_ENROLLMENT >= 12 THEN '3) T12+'
           END AS AGE_GROUP
     --
     , LOAN_STATUS
     , DOC_NEEDED_BANK_STATEMENT
     , DOC_NEEDED_IDENTITY
     , DOC_NEEDED_RESIDENCE
     , DOC_NEEDED_INCOME
     , DOC_NEEDED_OTHER
--      , DECISION_FLAG
--      , APPROVED_FLAG
     , DAYS_TO_ALL_DOCS_SUBMIT
     , DAYS_TO_DECISION
     , DAYS_TO_APPROVAL

     , D.DAYS
     , CASE
           WHEN APPLICATION_DATE + D.DAYS > CURRENT_DATE THEN NULL
           WHEN DAYS_TO_ALL_DOCS_SUBMIT <= D.DAYS THEN 1
           ELSE 0
           END AS ALL_DOCS_SUBMIT_FLG
     , CASE
           WHEN APPLICATION_DATE + D.DAYS > CURRENT_DATE THEN NULL
           WHEN DAYS_TO_DECISION <= D.DAYS THEN 1
           ELSE 0
           END AS DECISIONED_FLG
     , CASE
           WHEN APPLICATION_DATE + D.DAYS > CURRENT_DATE THEN NULL
           WHEN DAYS_TO_APPROVAL <= D.DAYS THEN 1
           ELSE 0
           END AS APPROVED_FLG
     , IFF(APPLICATION_DATE + D.DAYS <= CURRENT_DATE, 1, 0) AS IS_SEASONED
FROM APPLICATION_DATA
     CROSS JOIN DAY_SEQUENCE D
ORDER BY APPLICATION_DATE, UNIFIED_ID, DAYS
;