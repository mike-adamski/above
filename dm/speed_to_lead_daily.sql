WITH ALL_PROSPECTS AS (
                      SELECT ID AS PROSPECT_ID
                           , convert_timezone('America/Chicago', CREATED_AT) AS PROSPECT_CREATED_TS
                           , 'DM'::VARCHAR(256) AS PROSPECT_TYPE
                           , SUBSTR(PHONE_NUMBER, 2, 10) AS PHONE_NUMBER
                           , EMAIL AS EMAIL_ADDRESS
                           , STATE
                           , CODE
                      FROM ABOVE_DW_PROD.ABOVE_PUBLIC.DM2_PROSPECTS
--                       UNION
--                       SELECT L.ID
--                            , convert_timezone('America/Chicago', L.CREATED_AT)
--                            , A.NAME
--                            , DATA['phoneNumber']::TEXT
--                            , DATA['email']::TEXT
--                            , DATA['state']::TEXT
--                            , NULL
--                       FROM CERBERUS_PUBLIC.LEADS L
--                            LEFT JOIN CERBERUS_PUBLIC.AFFILIATES A ON L.AFFILIATE_ID = A.ID
                      )

   , SPEED_TO_LEAD AS (
                      SELECT PROSPECT_ID
                           , PROSPECT_CREATED_TS
                           , PROSPECT_TYPE
                           , RIGHT(P.PHONE_NUMBER, 10) AS PHONE_NUMBER
                           , P.CODE
                           , extract(DOW FROM PROSPECT_CREATED_TS) AS PROSPECT_CREATE_DOW
                           , extract(HOUR FROM PROSPECT_CREATED_TS) AS PROSPECT_CREATE_HOD
                           , CASE
                                 WHEN PROSPECT_CREATE_DOW IN (0, 6) THEN 0
                                 WHEN PROSPECT_CREATE_HOD < 8 OR
                                      PROSPECT_CREATE_HOD > 20 THEN 0
                                 WHEN PROSPECT_CREATED_TS::DATE IN ('2022-05-30', '2022-07-04', '2022-09-05') THEN 0
                                 ELSE 1
                                 END AS BIZ_HOURS_FLAG
                           , C.CALL_TIMESTAMP AS FIRST_CALL_TS
                           , C.CALL_TYPE AS FIRST_CALL_TYPE
                           , C.TALK_TIME_SECS AS FIRST_CALL_TALK_TIME
                           , C.CONTACTED AS FIRST_CALL_CONTACT_FLAG_FIVE9
                           , CALC_CONTACTED_FLAG AS FIRST_CALL_CONTACT_FLAG
                           , IFF(FIRST_CALL_TS IS NOT NULL, 1, 0) AS CALLED_FLAG
                           , datediff('second', PROSPECT_CREATED_TS, C.CALL_TIMESTAMP) AS STL_SECS
                           , iff(STL_SECS <= 30, 1, 0) AS STL_UNDER_30S_FLAG
                           , CASE
                                 WHEN STL_SECS IS NULL THEN 'No Call'
                                 WHEN STL_SECS <= 15 THEN '1) Within 15s'
                                 WHEN STL_SECS <= 30 THEN '2) 16 to 30s'
                                 WHEN STL_SECS <= 45 THEN '3) 31 to 60s'
                                 WHEN STL_SECS <= 90 THEN '4) 61 to 90s'
                                 WHEN STL_SECS <= 120 THEN '5) 91 to 120s'
                                 WHEN STL_SECS > 120 THEN '6) Over 120s'
                                 END AS STL_GROUP

                      FROM ALL_PROSPECTS P
                           LEFT JOIN CURATED_PROD.PHONE_DATA.FIVE9_CALLS C
                                     ON P.PHONE_NUMBER = C.CUSTOMER_PHONE_NUMBER
                                         AND C.CALL_TIMESTAMP >= PROSPECT_CREATED_TS
                      WHERE P.EMAIL_ADDRESS NOT ILIKE '%@above%'
                        AND P.EMAIL_ADDRESS NOT ILIKE '%@beyond%'
                        AND PROSPECT_CREATED_TS::DATE < CURRENT_DATE
                          QUALIFY row_number() OVER (PARTITION BY PROSPECT_ID ORDER BY C.CALL_TIMESTAMP) = 1
                      )

   , FUNNEL_W_BEYOND AS (
                        SELECT F.*
                             , iff(FIRST_CALL_CONTACT_FLAG = 1 OR exists(
                                SELECT 1
                                FROM CURATED_PROD.PHONE_DATA.FIVE9_CALLS
                                WHERE CUSTOMER_PHONE_NUMBER = F.PHONE_NUMBER
                                  AND CALL_TIMESTAMP::DATE = F.PROSPECT_CREATED_TS::DATE
                                  AND CALC_CONTACTED_FLAG = 1
                            ), 1, 0) AS DAY_0_CONTACT_FLAG
                             , iff(B.ENROLLED_FLAG, 1, 0) AS ENROLLED_FLAG
                             , iff(B.ENROLLED_FLAG AND FIRST_CALL_CONTACT_FLAG = 1
                                       AND NOT exists(SELECT 1
                                                      FROM CURATED_PROD.PHONE_DATA.FIVE9_CALLS
                                                      WHERE CUSTOMER_PHONE_NUMBER = F.PHONE_NUMBER
                                                        AND TALK_TIME_SECS > FIRST_CALL_TALK_TIME
                                                        AND CALL_TIMESTAMP > F.FIRST_CALL_TS
                                                        AND CALL_TIMESTAMP <= B.PROGRAM_CREATED_TIME_CST)
                            , 1, 0) AS FIRST_CALL_ENROLLED_FLAG
                             , iff(B.ENROLLED_FLAG AND PROSPECT_CREATED_TS::DATE = PROGRAM_CREATED_TIME_CST::DATE, 1,
                                   0) AS DAY_0_ENROLLED_FLAG
                             , iff(FIRST_CALL_ENROLLED_FLAG = 1, B.TOTAL_ENROLLED_DEBT_WITHOUT_CONDITIONAL_DEBT,
                                   NULL) AS FIRST_CALL_ENROLLED_AMOUNT
                        FROM SPEED_TO_LEAD F
                             LEFT JOIN BEYOND.OUTBOUND_ABOVELENDING.VW_LEAD_DETAIL B
                                       ON F.CODE = B.DIRECT_MAIL_CODE
                                           AND LEAD_CREATED_TIME_CST > F.PROSPECT_CREATED_TS - INTERVAL '1 day'
                            QUALIFY RANK()
                                            OVER (PARTITION BY DIRECT_MAIL_CODE
                                                ORDER BY CASE
                                                             WHEN FURTHEST_MILESTONE = 'Enrolled' THEN 1
                                                             WHEN FURTHEST_MILESTONE = 'Contacted + Credit Pulled'
                                                                 THEN 2
                                                             WHEN FURTHEST_MILESTONE = 'Contacted' THEN 3
                                                             WHEN FURTHEST_MILESTONE = 'Lead-to-Floor' THEN 4
                                                             WHEN FURTHEST_MILESTONE = 'None' THEN 5
                                                             ELSE 6
                                                             END,
                                                    LEAD_CREATED_TIME_CST
                                                ) = 1
                        )
   , FUNNEL_W_ABOVE_BEYOND AS (
                              SELECT F.*
                                   , iff(LS.UPDATED_AT IS NOT NULL
                                             AND NOT exists(SELECT 1
                                                            FROM CURATED_PROD.PHONE_DATA.FIVE9_CALLS
                                                            WHERE CUSTOMER_PHONE_NUMBER = F.PHONE_NUMBER
                                                              AND TALK_TIME_SECS > FIRST_CALL_TALK_TIME
                                                              AND CALL_TIMESTAMP > F.FIRST_CALL_TS
                                                              AND CALL_TIMESTAMP <= CONVERT_TIMEZONE('America/Chicago', L.CREATED_AT)
                                      ), 1, 0) AS FIRST_CALL_LOAN_FLAG
                                   , iff(PROSPECT_CREATED_TS::DATE =
                                         convert_timezone('America/Chicago', LS.UPDATED_AT)::DATE, 1,
                                         0) AS DAY_0_LOAN_FLAG
                              FROM FUNNEL_W_BEYOND F
                                   LEFT JOIN ABOVE_PUBLIC.LOANS L
                                             ON F.CODE = L.CODE
                                                 AND CONVERT_TIMEZONE('America/Chicago', L.CREATED_AT) >
                                                     F.PROSPECT_CREATED_TS
                                   LEFT JOIN (
                                             SELECT *
                                             FROM ABOVE_PUBLIC.LOAN_STATUS_HISTORY
                                             WHERE NEW_STATUS ILIKE '%ABOVE%SELECT%'
                                                OR NEW_STATUS ILIKE '%EVEN%SELECT%'
                                                 QUALIFY row_number() OVER (PARTITION BY LOAN_ID ORDER BY UPDATED_AT) = 1
                                             ) LS ON L.ID = LS.LOAN_ID
                                  QUALIFY RANK() OVER (PARTITION BY F.PROSPECT_CREATED_TS
                                      ORDER BY L.CREATED_AT) = 1

                              )
   , ALL_DATA AS (
                 SELECT PROSPECT_ID
                      , PROSPECT_CREATED_TS::TIMESTAMP_NTZ AS PROSPECT_CREATED_TS
                      , PROSPECT_CREATED_TS::DATE AS PROSPECT_CREATED_DATE
                      , date_trunc('week', PROSPECT_CREATED_TS)::DATE AS PROSPECT_CREATED_WEEK
                      , PHONE_NUMBER
                      , CODE
                      , PROSPECT_CREATE_DOW
                      , PROSPECT_CREATE_HOD
                      , BIZ_HOURS_FLAG
                      , FIRST_CALL_TYPE
                      , FIRST_CALL_CONTACT_FLAG_FIVE9
                      , FIRST_CALL_CONTACT_FLAG
                      , DAY_0_CONTACT_FLAG
                      , CALLED_FLAG
                      , STL_SECS
                      , STL_UNDER_30S_FLAG
                      , ENROLLED_FLAG
--                       , greatest(FIRST_CALL_ENROLLED_FLAG, FIRST_CALL_LOAN_FLAG) AS FIRST_CALL_ENROLLED_OR_LOAN
                      , FIRST_CALL_ENROLLED_FLAG
--                       , greatest(DAY_0_ENROLLED_FLAG, DAY_0_LOAN_FLAG) AS DAY_0_ENROLLED_OR_LOAN
                      , DAY_0_ENROLLED_FLAG
                      , FIRST_CALL_ENROLLED_AMOUNT
                      , STL_GROUP
                      , FIRST_CALL_LOAN_FLAG
--                       , DAY_0_LOAN_FLAG
                 FROM FUNNEL_W_ABOVE_BEYOND
                 )

SELECT PROSPECT_CREATED_DATE AS "Date"
     -- Raw lead counts
     , count(*) AS "# Leads Created"
     , sum(STL_UNDER_30S_FLAG) AS "# STL within 30s"
     , sum(FIRST_CALL_CONTACT_FLAG) AS "# First Call Contacted"
     , sum(FIRST_CALL_ENROLLED_FLAG) AS "# First Call Enrolled"
     , sum(DAY_0_CONTACT_FLAG) AS "# Day 0 Contacted"
     , sum(DAY_0_ENROLLED_FLAG) AS "# Day 0 Enrolled"
     -- Funnel ratios
     , "# STL within 30s" / "# Leads Created" AS "% STL within 30s"
     , "# First Call Contacted" / "# Leads Created" AS "% First Call Contact"
     , "# First Call Enrolled" / "# First Call Contacted" AS "% First Call Enroll / Contact"
     , "# Day 0 Contacted" / "# Leads Created" AS "% Day 0 Contact"
     , "# Day 0 Enrolled" / "# Day 0 Contacted" AS "% Day 0 Enroll / Contact"
     -- Drilldown of speed to lead by time buckets
     , count(CASE WHEN STL_GROUP = '1) Within 15s' THEN 1 END) AS "1) Within 15s"
     , count(CASE WHEN STL_GROUP = '2) 16 to 30s' THEN 1 END) AS "2) 16 to 30s"
     , count(CASE WHEN STL_GROUP = '3) 31 to 60s' THEN 1 END) AS "3) 31 to 60s"
     , count(CASE WHEN STL_GROUP = '4) 61 to 90s' THEN 1 END) AS "4) 61 to 90s"
     , count(CASE WHEN STL_GROUP = '5) 91 to 120s' THEN 1 END) AS "5) 91 to 120s"
     , count(CASE WHEN STL_GROUP = '6) Over 120s' THEN 1 END) AS "6) Over 120s"
     , count(CASE WHEN STL_GROUP = 'No Call' THEN 1 END) AS "No Call"
     , current_timestamp AS UPDATED_AT
FROM ALL_DATA
WHERE PROSPECT_CREATED_DATE BETWEEN current_date - 22 AND current_date - 1
  AND BIZ_HOURS_FLAG = 1
  AND FIRST_CALL_TYPE = 'Preview'
  AND FIRST_CALL_LOAN_FLAG = 0
GROUP BY 1
ORDER BY 1
;
