CREATE OR REPLACE VIEW SNO_SANDBOX.IPL.TARGETING_FILE_V AS
WITH ELIG_FILE_DATA AS (
                       SELECT DISTINCT
                              LOADED_DATE
                            , E.PROGRAM_ID AS PROGRAM_NAME
                            , E.CLIENT_ID AS PROGRAM_ID
                            , BEYOND_ENROLLMENT_DATE AS ENROLLED_DATE
                            , ZIP_CODE
                            , STATE
                            , E.SOURCE_SYSTEM
                            , iff(TELEPHONE_NUMBER ILIKE '%DONOTCALL%', NULL, TELEPHONE_NUMBER) AS TELEPHONE_NUMBER
                            , AMOUNT_FINANCED
                            , ((AMOUNT_FINANCED / .95) / (((POWER(1 + (0.229 / 12), 72) - 1)) /
                                                          ((0.229 / 12) * (POWER(1 + (0.229 / 12), 72))))) AS ABOVE_MONTHLY_PAYMENT
                            , DRAFT_AMOUNT * CASE
                                                 WHEN NU_DSE_PAYMENT_FREQUENCY_C = 'Monthly' THEN 1
                                                 WHEN NU_DSE_PAYMENT_FREQUENCY_C IN ('Semi-Monthly', 'Twice Monthly')
                                                     THEN 2
                                                 WHEN NU_DSE_PAYMENT_FREQUENCY_C = 'Bi-Weekly' THEN 26 / 12
                                                 ELSE -1 -- If payment frequency not in this list, make field negative for visibility
                                                 END AS BEYOND_MONTHLY_DEPOSIT
                            , NU_DSE_PAYMENT_FREQUENCY_C AS DEPOSIT_FREQUENCY
                            , IFF(NEXT_PAYMENT_DATE < CURRENT_DATE, AD.ALT_DEPOSIT_DATE,
                                  NEXT_PAYMENT_DATE) AS NEXT_DEPOSIT_DATE_SCHEDULED
                            , PAYMENT_ADHERENCE_RATIO_3_MONTHS AS DEPOSIT_ADHERENCE_3M
                            , PAYMENT_ADHERENCE_RATIO_6_MONTHS AS DEPOSIT_ADHERENCE_6M
                            , BF_REMAINING_DEPOSITS
                            , HISTORICAL_SETTLEMENT_PERCENT
                            , P.SERVICE_ENTITY_NAME
                       FROM SNO_SANDBOX.IPL.IPL_ELIGIBLE E
                            LEFT JOIN CURATED_PROD.CRM.PROGRAM P
                                      ON E.PROGRAM_ID = P.PROGRAM_NAME AND P.IS_CURRENT_RECORD_FLAG
                            LEFT JOIN (
                                      SELECT PROGRAM_NAME, min(SCHEDULED_DATE_CST) AS ALT_DEPOSIT_DATE
                                      FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION
                                      WHERE IS_CURRENT_RECORD_FLAG
                                        AND SCHEDULED_DATE_CST >= current_date
                                        AND NOT IS_DELETED_FLAG
                                        AND TRANSACTION_TYPE = 'Deposit'
                                      GROUP BY 1
                                      ) AD ON AD.PROGRAM_NAME = E.PROGRAM_ID
                       WHERE LOADED_DATE = CURRENT_DATE
                       )
   , FIVE9_PHONE_LOOKUP AS (
                           SELECT DISTINCT
                                  COALESCE(NUMBER1,
                                           IFF(CAMPAIGN_TYPE IN ('Inbound', '3rd party transfer'), ANI, DNIS)) AS TELEPHONE_NUMBER
                                , last_value(DISPOSITION)
                                             OVER (PARTITION BY TELEPHONE_NUMBER ORDER BY TIMESTAMP DESC) AS LAST_BAD_DISPOSITION
                                , last_value(TIMESTAMP)
                                             OVER (PARTITION BY TELEPHONE_NUMBER ORDER BY TIMESTAMP DESC)::TIMESTAMP AS LAST_BAD_DISPOSITION_TS
                           FROM REFINED_PROD.FIVE9_LEGACY.CALL_LOG
                           WHERE CASE
                                     WHEN DISPOSITION IN
                                          ('Transferred To 3rd Party', 'Transferred to Lender') AND
                                          try_to_timestamp(TIMESTAMP)::DATE > CURRENT_DATE - 14 THEN TRUE
                                     WHEN DISPOSITION IN ('Duplicate', 'Declined') AND
                                          try_to_timestamp(TIMESTAMP)::DATE > CURRENT_DATE - 3 THEN TRUE
                                     WHEN DISPOSITION IN ('Not Interested') AND
                                          try_to_timestamp(TIMESTAMP)::DATE > CURRENT_DATE - 90 THEN TRUE
                                     ELSE FALSE
                                     END
                           )
   , PROGRAM_TO_CREDIT_REPORT AS (
                                 SELECT DISTINCT
                                        P.NAME AS PROGRAM_NAME
                                      , CR_C.ID AS CREDIT_REPORT_ID_C
                                      , CR_C.RECEIVED_DATE_C AS CREDIT_REPORT_DATE
                                      , CR_C.CREATED_DATE_UTC AS REPORT_CREATED_DATE
                                      , datediff(MONTH, P.ENROLLMENT_DATE_C, current_date) AS MONTHS_SINCE_ENROLLMENT
                                 FROM REFINED_PROD.BEDROCK.OPPORTUNITY_TRADELINE_C OT
                                      LEFT JOIN "REFINED_PROD"."BEDROCK".OPPORTUNITY AS O
                                                ON O.ID = OT.OPPORTUNITY_ID_C AND O.IS_DELETED = 'FALSE'
                                      LEFT JOIN "REFINED_PROD"."BEDROCK".ACCOUNT AS A
                                                ON A.ID = O.ACCOUNT_ID AND A.IS_DELETED = 'FALSE'
                                      LEFT JOIN REFINED_PROD.BEDROCK.PROGRAM_C AS P
                                                ON P.ACCOUNT_ID_C = A.ID AND P.IS_DELETED = 'FALSE' AND
                                                   datediff(MONTH, P.ENROLLMENT_DATE_C, current_date) < 6
                                      LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_C AS CL
                                                ON CL.ID = OT.CR_LIABILITY_ID_C
                                      LEFT JOIN REFINED_PROD.BEDROCK.CR_C AS CR_C
                                                ON CL.CREDIT_REPORT_ID_C = CR_C.ID AND CR_C.IS_DELETED = FALSE
                                 WHERE 1 = 1
                                   AND OT.IS_DELETED = FALSE
                                   AND P.NAME IS NOT NULL
                                   AND CR_C.ID IS NOT NULL
                                 ORDER BY 1, 3
                                 )

   //Bankruptcy in last 7 years
   , CR_BANKRUPTCY AS (

                      SELECT DISTINCT
                             A.PROGRAM_NAME
                           , 'Bankruptcy in last 7 years' AS REASON
                           , max(B.FILE_DATE_C) AS MOST_RECENT_FILE_DATE
                           , datediff(MONTHS, max(B.FILE_DATE_C), current_date) AS MONTHS_SINCE_BANKRUPTCY_FILE
                      FROM PROGRAM_TO_CREDIT_REPORT AS A
                           LEFT JOIN REFINED_PROD.BEDROCK.CR_PUBLIC_RECORD_C AS B
                                     ON A.CREDIT_REPORT_ID_C = B.CREDIT_REPORT_ID_C AND IS_DELETED = FALSE
                      WHERE 1 = 1
                        AND A.CREDIT_REPORT_ID_C IS NOT NULL
                        AND B.RECORD_TYPE_C LIKE ('%Bankruptcy%')
                      GROUP BY 1, 2
                      HAVING MONTHS_SINCE_BANKRUPTCY_FILE <= 84
                      ORDER BY 1
                      )

   //Repossession/Foreclosure in last 7 years
   , CR_REPO_FORECLOSURE AS (

                            SELECT DISTINCT
                                   A.PROGRAM_NAME
                                 , 'Repo/Foreclosure in last 7 years' AS REASON
                                 , max(B.REPORT_DATE_C) AS MOST_RECENT_RECORD_DATE
                                 , datediff(MONTHS, max(B.REPORT_DATE_C), current_date) AS MONTHS_SINCE_REPO_FORECLOSURE
                            FROM PROGRAM_TO_CREDIT_REPORT AS A
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_C AS B
                                           ON A.CREDIT_REPORT_ID_C = B.CREDIT_REPORT_ID_C AND IS_DELETED = FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_COMMENT_C AS C
                                           ON B.ID = C.CR_LIABILITY_ID_C AND C.IS_DELETED = FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_COMMENT_DEF_C AS D
                                           ON D.ID = C.CR_COMMENT_DEF_ID_C AND D.IS_DELETED = FALSE
                            WHERE 1 = 1
                              AND A.CREDIT_REPORT_ID_C IS NOT NULL
                              AND B.STATUS_C IN ('Open', 'Closed')
                              AND (B.RATING_TYPE_C IN ('Repossession', 'Foreclosure') OR D.COMMENT_C IN
                                                                                         ('FORECLOSURE PROCESS STARTED',
                                                                                          'PAID REPOSSESSION',
                                                                                          'REDEEMED OR REINSTATED REPOSSESSION',
                                                                                          'INVOLUNTARY REPOSSESSION'))
                            GROUP BY 1, 2
                            HAVING MONTHS_SINCE_REPO_FORECLOSURE <= 84
                            ORDER BY 1
                            )

   //New Mortgage, Auto or Educational TL in last 6 months
   , CR_NEW_MORT_AUTO_ED AS (

                            SELECT DISTINCT
                                   A.PROGRAM_NAME
                                 , 'New Mortgage, Auto or Educational TL' AS REASON
                                 , count(B.ID) AS NEW_TL_COUNT
                            FROM PROGRAM_TO_CREDIT_REPORT AS A
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_C AS B
                                           ON A.CREDIT_REPORT_ID_C = B.CREDIT_REPORT_ID_C AND B.IS_DELETED = FALSE AND
                                              datediff(MONTHS, B.ACCOUNT_OPEN_DATE_C, current_date) <= 6
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_COMMENT_C AS C
                                           ON B.ID = C.CR_LIABILITY_ID_C AND C.IS_DELETED = FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_COMMENT_DEF_C AS D
                                           ON D.ID = C.CR_COMMENT_DEF_ID_C AND D.IS_DELETED = FALSE
                            WHERE 1 = 1
                              AND A.CREDIT_REPORT_ID_C IS NOT NULL
                              AND B.STATUS_C IN ('Open', 'Closed')
                              AND B.ACCOUNT_TYPE_C NOT IN ('CreditLine')
                              AND B.LOAN_TYPE_C IN ('Automobile', 'Educational', 'Mortgage')
                            GROUP BY 1, 2
                            HAVING NEW_TL_COUNT > 0
                            ORDER BY 1
                            )

   //Auto or Mortgage missed payment in last 6 months
   , CR_MORT_AUTO_MISSED AS (

                            SELECT DISTINCT
                                   A.PROGRAM_NAME
                                 , 'Auto or Mortgage missed payment' AS REASON
                                 , count(B.ID) AS MISSED_AUTO_MORT_COUNT
                            FROM PROGRAM_TO_CREDIT_REPORT AS A
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_C AS B
                                           ON A.CREDIT_REPORT_ID_C = B.CREDIT_REPORT_ID_C AND B.IS_DELETED = FALSE AND
                                              datediff(MONTHS, B.REPORT_DATE_C, current_date) <= 6
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_COMMENT_C AS C
                                           ON B.ID = C.CR_LIABILITY_ID_C AND C.IS_DELETED = FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_COMMENT_DEF_C AS D
                                           ON D.ID = C.CR_COMMENT_DEF_ID_C AND D.IS_DELETED = FALSE
                            WHERE 1 = 1
                              AND A.CREDIT_REPORT_ID_C IS NOT NULL
                              AND B.STATUS_C IN ('Open')
                              AND B.ACCOUNT_TYPE_C NOT IN ('CreditLine')
                              AND B.LOAN_TYPE_C IN ('Automobile', 'Mortgage')
                              AND B.RATING_TYPE_C IN ('Late30Days', 'Late60Days', 'Late90Days', 'LateOver120Days')
                            GROUP BY 1, 2
                            HAVING MISSED_AUTO_MORT_COUNT > 0
                            ORDER BY 1
                            )
   , FAIL_REASONS AS (
                     SELECT DISTINCT
                            PROGRAM_NAME
                          , REASON
                     FROM CR_BANKRUPTCY

                     UNION

                     SELECT DISTINCT
                            PROGRAM_NAME
                          , REASON
                     FROM CR_REPO_FORECLOSURE

                     UNION

                     SELECT DISTINCT
                            PROGRAM_NAME
                          , REASON
                     FROM CR_NEW_MORT_AUTO_ED

                     UNION

                     SELECT DISTINCT
                            PROGRAM_NAME
                          , REASON
                     FROM CR_MORT_AUTO_MISSED
                     )
   , CREDIT_FLAGS AS (
                     SELECT R.PROGRAM_NAME, array_agg(DISTINCT REASON) WITHIN GROUP (ORDER BY R.REASON) AS CREDIT_FLAGS
                     FROM PROGRAM_TO_CREDIT_REPORT P
                          JOIN FAIL_REASONS R ON P.PROGRAM_NAME = R.PROGRAM_NAME
                     WHERE P.MONTHS_SINCE_ENROLLMENT BETWEEN 3 AND 5
                     GROUP BY 1
                     )
   , ALL_DATA AS (
                 SELECT D.*
                      , ND.NEXT_DEPOSIT_DATE_PROCESSED
                      , CASE
                            WHEN ENROLLED_DATE > CURRENT_DATE - INTERVAL '3 months' THEN 'Too new'
                            WHEN ENROLLED_DATE > CURRENT_DATE - INTERVAL '6 months' THEN 'T3-T6'
                            WHEN ENROLLED_DATE > CURRENT_DATE - INTERVAL '12 months' THEN 'T6-T12'
                            WHEN ENROLLED_DATE <= CURRENT_DATE - INTERVAL '12 months' THEN 'T12+'
                            ELSE '???' -- Should never happen
                            END AS PROGRAM_AGE_GROUP
                      , coalesce(P_L.LOAN_INTEREST_STATUS_C, PLC.LOAN_APPLICATION_INTEREST_C) AS BEYOND_LOAN_STATUS
                      --Incorporate Bedrock Program Loan attributes
                      , PLC.LOAN_APPLICATION_STATUS_C AS BEDROCK_LOAN_APPLICATION_STATUS                                        -- This + loan_application_interest_c are two BR fields that had been a single combined field (loan_interest_status_c) in Legacy; splitting them up here so they can both be used in logic
                      , PLC.LOAN_APPLICATION_DATE_C AS BEDROCK_LOAN_APPLICATION_DATE
                      , COALESCE(P_L.LOAN_INTEREST_RESPONSE_DATE_C_CST,
                                 GREATEST(F9.LAST_BAD_DISPOSITION_TS, F9PL.LAST_BAD_DISPOSITION_TS)) AS BEYOND_LOAN_STATUS_DATE -- No timestamp for loan_application_status in legacy; using the last bad dispo timestamp as a proxy
                      , R.CURRENT_STATUS AS ABOVE_LOAN_STATUS
                      , COALESCE(LSR.UPDATED_LOAN_STATUS_LEGACY, BEYOND_LOAN_STATUS) AS BEYOND_LOAN_STATUS_CORRECTED
                      , R.APP_SUBMIT_DATE AS ABOVE_APPLICATION_DATE
                      , CASE
                            WHEN F9PL.LAST_BAD_DISPOSITION IS NULL THEN F9.LAST_BAD_DISPOSITION
                            WHEN F9.LAST_BAD_DISPOSITION IS NULL THEN F9PL.LAST_BAD_DISPOSITION
                            WHEN F9PL.LAST_BAD_DISPOSITION_TS >= F9.LAST_BAD_DISPOSITION_TS
                                THEN F9PL.LAST_BAD_DISPOSITION
                            WHEN F9.LAST_BAD_DISPOSITION_TS >= F9PL.LAST_BAD_DISPOSITION_TS THEN F9.LAST_BAD_DISPOSITION
                            END AS LAST_BAD_DISPOSITION
                      , GREATEST(F9.LAST_BAD_DISPOSITION_TS, F9PL.LAST_BAD_DISPOSITION_TS) AS LAST_BAD_DISPOSITION_TS
                      , CASE
                            WHEN P_L.HAS_CO_CLIENT_C OR P_B.CO_CLIENT_ID_C IS NOT NULL THEN TRUE
                            ELSE FALSE
                            END AS HAS_CO_CLIENT
                      , CASE
                            WHEN P_L.LANGUAGE_C = 'Spanish' OR PR.PREFERRED_LANGUAGE_C = 'Spanish' THEN TRUE
                            WHEN P_B.IS_SPANISH_PREFERRED_FLAG_C THEN TRUE
                            ELSE FALSE
                            END AS IS_SPANISH_SPEAKING
                      , CF.CREDIT_FLAGS
                 FROM ELIG_FILE_DATA D
                      LEFT JOIN SNO_SANDBOX.IPL.LOAN_STATUS_RECONCILIATION LSR
                                ON D.PROGRAM_NAME = LSR.PROGRAM_NAME
                                    AND LSR.LOADED_DATE = CURRENT_DATE
                                    AND LSR.SOURCE_SYSTEM = 'LEGACY'
                      LEFT JOIN (
                                SELECT *
                                FROM SNO_SANDBOX.IPL.IPL_RETURN_FILE
                                    QUALIFY rank()
                                                    OVER (PARTITION BY PROGRAM_ID
                                                        ORDER BY LOADED_DATE DESC, coalesce(APP_SUBMIT_DATE, '1901-01-01') DESC, LAST_UPDATED_T DESC, DECLINE_REASON_1 DESC) =
                                            1
                                ) R ON D.PROGRAM_NAME = R.PROGRAM_ID AND D.LOADED_DATE = R.LOADED_DATE
                      LEFT JOIN (
                                SELECT DISTINCT
                                       SALESFORCE_ID
                                     , last_value(DISPOSITION)
                                                  OVER (PARTITION BY SALESFORCE_ID ORDER BY TIMESTAMP DESC) AS LAST_BAD_DISPOSITION
                                     , last_value(TIMESTAMP)
                                                  OVER (PARTITION BY SALESFORCE_ID ORDER BY TIMESTAMP DESC)::TIMESTAMP AS LAST_BAD_DISPOSITION_TS
                                FROM REFINED_PROD.FIVE9_LEGACY.CALL_LOG
                                WHERE CASE
                                          WHEN DISPOSITION IN
                                               ('Transferred To 3rd Party', 'Transferred to Lender') AND
                                               try_to_timestamp(TIMESTAMP)::DATE > CURRENT_DATE - 14 THEN TRUE
                                          WHEN DISPOSITION IN ('Duplicate', 'Declined') AND
                                               try_to_timestamp(TIMESTAMP)::DATE > CURRENT_DATE - 3 THEN TRUE
                                          WHEN DISPOSITION IN ('Not Interested') AND
                                               try_to_timestamp(TIMESTAMP)::DATE > CURRENT_DATE - 90 THEN TRUE
                                          ELSE FALSE
                                          END
                                ) AS F9 ON F9.SALESFORCE_ID = D.PROGRAM_ID
                      LEFT JOIN FIVE9_PHONE_LOOKUP F9PL ON try_to_number(D.TELEPHONE_NUMBER) = F9PL.TELEPHONE_NUMBER
                      LEFT JOIN REFINED_PROD.SALESFORCE.NU_DSE_PROGRAM_C P_L
                                ON D.PROGRAM_NAME = P_L.NAME AND P_L.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
                      LEFT JOIN REFINED_PROD.BEDROCK.PROGRAM_C P_B ON D.PROGRAM_NAME = P_B.NAME
                      LEFT JOIN (
                                SELECT *
                                FROM REFINED_PROD.BEDROCK.PROGRAM_LOAN_C
                                WHERE NOT IS_DELETED
                                    QUALIFY RANK()
                                                    OVER (PARTITION BY PROGRAM_ID_C ORDER BY CREATED_DATE_CST DESC, NAME DESC) =
                                            1
                                ) PLC ON PLC.PROGRAM_ID_C = P_B.ID
                      LEFT JOIN REFINED_PROD.SALESFORCE.NU_DSE_PROSPECT_C AS PR
                                ON P_L.PROSPECT_ID_C = PR.ID AND PR.IS_DELETED_FLAG = FALSE AND
                                   PR.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
                      LEFT JOIN CREDIT_FLAGS CF ON D.PROGRAM_NAME = CF.PROGRAM_NAME
                    , LATERAL (
                     SELECT min(C.CALENDAR_DATE_CST) AS NEXT_DEPOSIT_DATE_PROCESSED
                     FROM SNO_SANDBOX.IPL.CALENDAR C
                     WHERE C.CALENDAR_DATE_CST >= D.NEXT_DEPOSIT_DATE_SCHEDULED
                       AND C.DAY_NAME NOT IN ('SATURDAY', 'SUNDAY')
                       AND NOT C.IS_HOLIDAY
                     ) AS ND -- When deposit scheduled on a weekend/holiday, find the next business day that it will process on
                 )

   , COHORT_FLAGS AS (
                     SELECT *
                          , iff(exists(SELECT 1
                                       FROM SNO_SANDBOX.IPL.IPL_DNC
                                       WHERE PROGRAM_NAME = ALL_DATA.PROGRAM_NAME), TRUE,
                                FALSE) AS ON_DNC_LIST
                          , (
                            SELECT max(LOADED_TIMESTAMP::DATE + DURATION_DAYS) AS EXP_DATE
                            FROM SNO_SANDBOX.IPL.IPL_REACTIVATION
                            WHERE PROGRAM_NAME = ALL_DATA.PROGRAM_NAME
                            HAVING EXP_DATE >= current_date
                            ) AS ON_REACTIVATION_LIST_THRU
                          , CASE
                                WHEN STATE IN ('CA') THEN 'Above Lending'
                                WHEN STATE IN
                                     ('TX', 'NC', 'IN', 'MO', 'AL', 'NM', 'TN', 'MS', 'MT', 'KY', 'FL', 'MI', 'AK',
                                      'SD', 'DC', 'OK', 'WI', 'NY', 'PA', 'VA', 'AZ', 'AR', 'UT', 'ID', 'LA') THEN 'CRB'
                                END AS LENDER
                          , CASE
                                WHEN LENDER IS NULL OR datediff('month', ENROLLED_DATE, current_date) < 6 THEN 'None'
                                WHEN SOURCE_SYSTEM = 'LEGACY' AND ABOVE_LOAN_STATUS IN ('EXPIRED')
                                    THEN 'AboveLending-RT-Expired-OB'
                                WHEN SOURCE_SYSTEM = 'BEDROCK' AND ABOVE_LOAN_STATUS IN ('EXPIRED')
                                    THEN 'None' // 'BRC-AboveLending-RT-Expired-OB'
                                WHEN SOURCE_SYSTEM = 'LEGACY' AND (ABOVE_LOAN_STATUS IN
                                                                   ('FRONT_END_DECLINED', 'BACK_END_DECLINED', 'NO_OFFERS')
                                    OR BEYOND_LOAN_STATUS_CORRECTED IN ('UW Declined'))
                                    THEN 'AboveLending-RT-Declined-OB'
                                WHEN SOURCE_SYSTEM = 'BEDROCK' AND (ABOVE_LOAN_STATUS IN
                                                                    ('FRONT_END_DECLINED', 'BACK_END_DECLINED', 'NO_OFFERS')
                                    OR BEYOND_LOAN_STATUS_CORRECTED IN ('UW Declined'))
                                    THEN 'BRC-AboveLending-RT-Declined-OB'
                                WHEN SOURCE_SYSTEM = 'LEGACY' AND
                                     (BEYOND_LOAN_STATUS_CORRECTED IN ('Not Interested', 'Withdrawn') OR
                                      ABOVE_LOAN_STATUS IN ('WITHDRAWN'))
                                    THEN 'AboveLending-Retarget-OB'
                                WHEN SOURCE_SYSTEM = 'BEDROCK' AND
                                     (BEYOND_LOAN_STATUS_CORRECTED IN ('Not Interested', 'Withdrawn') OR
                                      ABOVE_LOAN_STATUS IN ('WITHDRAWN'))
                                    THEN 'None' // 'BRC-AboveLending-RT-NotInterested-OB'
                                WHEN SOURCE_SYSTEM = 'BEDROCK'
                                    THEN 'BRC-Above-OB - Above Lending - Week 2'
                                WHEN SOURCE_SYSTEM = 'LEGACY' AND LENDER IN ('Above Lending', 'CRB') AND
                                     PROGRAM_AGE_GROUP IN ('T3-T6', 'T6-T12', 'T12+')
                                    THEN iff(AMOUNT_FINANCED > 40000,
                                             'Above Lending - HD - Day 1',
                                             'Above Lending - Day 1')
                                ELSE 'None'
                                END AS CLIENT_COHORT
                          , CASE
                                WHEN HISTORICAL_SETTLEMENT_PERCENT > .65 THEN TRUE
                                WHEN AMOUNT_FINANCED <= 10000 AND AMOUNT_FINANCED > BF_REMAINING_DEPOSITS * 1.5
                                    THEN TRUE
                                WHEN AMOUNT_FINANCED > 10000 AND AMOUNT_FINANCED > BF_REMAINING_DEPOSITS * 1.25
                                    THEN TRUE
                                ELSE FALSE
                                END AS IS_OFF_COURSE
                          , IFF(LAST_BAD_DISPOSITION IS NOT NULL AND ON_REACTIVATION_LIST_THRU IS NULL, FALSE,
                                TRUE) AS FILTER_BAD_DISPOSITION
                          , CASE
                                WHEN SOURCE_SYSTEM = 'LEGACY' AND BEYOND_LOAN_STATUS_CORRECTED NOT IN
                                                                  ('Withdrawn', 'UW Declined', 'Unable to Contact',
                                                                   'Interested - Above',
                                                                   'Not Interested') THEN FALSE
                                WHEN BEYOND_LOAN_STATUS_CORRECTED IN ('Withdrawn')
                                    AND BEYOND_LOAN_STATUS_DATE > current_date - 45
                                    THEN FALSE
                                WHEN BEYOND_LOAN_STATUS_CORRECTED IN ('Not Interested')
                                    AND BEYOND_LOAN_STATUS_DATE > current_date - 90
                                    THEN FALSE
                                WHEN BEYOND_LOAN_STATUS_CORRECTED IN ('UW Declined')
                                    AND BEYOND_LOAN_STATUS_DATE > current_date - 90
                                    AND NOT (PROGRAM_NAME IN (
                                                             SELECT PROGRAM_NAME
                                                             FROM SNO_SANDBOX.IPL.CP_DECLINE_RETARGETS
                                                             )
                                        AND (BEYOND_LOAN_STATUS_DATE IS NULL OR
                                             BEYOND_LOAN_STATUS_DATE < '2021-12-28'))
                                    THEN FALSE -- Holding out of the dialer until retargeting script can be implemented
                                WHEN SOURCE_SYSTEM = 'BEDROCK' AND
                                     BEDROCK_LOAN_APPLICATION_STATUS NOT IN ('Decline', 'Withdrawn', 'Expired')
                                    AND BEDROCK_LOAN_APPLICATION_STATUS IS NOT NULL
                                    THEN FALSE
                                WHEN BEDROCK_LOAN_APPLICATION_STATUS IN ('Decline')
                                    AND datediff(DAY, BEDROCK_LOAN_APPLICATION_DATE, current_date) <= 90
                                    AND NOT (PROGRAM_NAME IN (
                                                             SELECT PROGRAM_NAME
                                                             FROM SNO_SANDBOX.IPL.CP_DECLINE_RETARGETS
                                                             )
                                        AND (BEDROCK_LOAN_APPLICATION_DATE IS NULL OR
                                             BEDROCK_LOAN_APPLICATION_DATE < '2021-12-28'))
                                    THEN FALSE
                                WHEN BEDROCK_LOAN_APPLICATION_STATUS IN ('Expired') AND
                                     datediff(DAY, BEDROCK_LOAN_APPLICATION_DATE, current_date) <= (28 + 45) --45 days from loan expiration
                                    THEN FALSE
                                WHEN BEDROCK_LOAN_APPLICATION_STATUS IN ('Withdrawn') AND
                                     datediff(DAY, BEDROCK_LOAN_APPLICATION_DATE, current_date) < 45 THEN FALSE
                                WHEN exists(SELECT 1
                                            FROM SNO_SANDBOX.IPL.IPL_DNC
                                            WHERE PROGRAM_NAME = ALL_DATA.PROGRAM_NAME) THEN FALSE
                                WHEN PROGRAM_NAME IN ('P-113287',
                                                      'P-113427',
                                                      'P-113411',
                                                      'P-113322',
                                                      'P-113289',
                                                      'P-113302',
                                                      'P-113636',
                                                      'P-113421',
                                                      'P-113459',
                                                      'P-113381',
                                                      'BRP-005659',
                                                      'BRP-004295',
                                                      'BRP-008869',
                                                      'BRP-010606',
                                                      'BRP-010858',
                                                      'BRP-012060',
                                                      'BRP-013774',
                                                      'BRP-021267')
                                    THEN FALSE -- VA clients signed after 7/1; don't give them IPL until further notice
                                ELSE TRUE
                                END AS FILTER_BEYOND_STATUS
                          , CASE
                                WHEN ABOVE_LOAN_STATUS IN
                                     ('INITIAL_TIL_SUBMIT', 'APPROVED', 'OFFERED_SELECTED', 'ADD_INFO_COMPLETE',
                                      'OFFERED', 'PENDING', 'BASIC_INFO_COMPLETE', 'ONBOARDED') THEN FALSE
                                WHEN ABOVE_LOAN_STATUS IN ('FRONT_END_DECLINED')
                                    AND ABOVE_APPLICATION_DATE >= CURRENT_DATE - 90
                                    AND NOT (PROGRAM_NAME IN (
                                                             SELECT PROGRAM_NAME
                                                             FROM SNO_SANDBOX.IPL.CP_DECLINE_RETARGETS
                                                             )
                                        AND (ABOVE_APPLICATION_DATE IS NULL OR
                                             ABOVE_APPLICATION_DATE < '2021-12-28'))
                                    THEN FALSE
                                WHEN ABOVE_LOAN_STATUS IN ('BACK_END_DECLINED')
--                                     AND ABOVE_APPLICATION_DATE >= CURRENT_DATE - 90
                                    THEN FALSE -- Temporarily hold retargeting cohorts out of the dialer
                                WHEN ABOVE_LOAN_STATUS IN ('EXPIRED') AND
                                     ABOVE_APPLICATION_DATE >= CURRENT_DATE - (28 + 45) --45 days from loan expiration
                                    THEN FALSE
                                WHEN ABOVE_LOAN_STATUS IN ('WITHDRAWN') AND ABOVE_APPLICATION_DATE >= CURRENT_DATE - 45
                                    THEN FALSE -- Temporarily hold retargeting cohorts out of the dialer
                                ELSE TRUE
                                END AS FILTER_ABOVE_STATUS
                          , CASE
                                WHEN CLIENT_COHORT = 'Above Lending - Day 1' THEN
                                    CASE
                                        WHEN STATE = 'CA' AND AMOUNT_FINANCED BETWEEN 5000 AND 40000 THEN TRUE
                                        WHEN STATE <> 'CA' AND AMOUNT_FINANCED BETWEEN 1000 AND 40000 THEN TRUE
                                        ELSE FALSE
                                        END
                                WHEN CLIENT_COHORT = 'Above Lending - HD - Day 1' THEN
                                    CASE
                                        WHEN STATE = 'CA' AND AMOUNT_FINANCED BETWEEN 40000 AND 69250 THEN TRUE
                                        WHEN LENDER = 'CRB' AND AMOUNT_FINANCED BETWEEN 40000 AND 69250 THEN TRUE
                                        ELSE FALSE
                                        END
                                WHEN CLIENT_COHORT = 'BRC-Above-OB - Above Lending - Week 2'
                                    THEN
                                    CASE
                                        WHEN STATE = 'CA' AND AMOUNT_FINANCED BETWEEN 5000 AND 69250 THEN TRUE
                                        WHEN LENDER = 'CRB' AND AMOUNT_FINANCED BETWEEN 1000 AND 69250 THEN TRUE
                                        ELSE FALSE
                                        END
                                ELSE TRUE
                                END AS FILTER_LOAN_AMOUNT
                          , IFF(IS_SPANISH_SPEAKING, FALSE, TRUE) AS FILTER_LANGUAGE
                          , TRUE AS FILTER_NEXT_DEPOSIT_DATE
                          , TRUE AS FILTER_DEPOSIT_ADHERENCE
                          , TRUE AS FILTER_PAYMENT_INCREASE
                          , iff(CREDIT_FLAGS IS NOT NULL, FALSE, TRUE) AS FILTER_CREDIT_FLAGS
--                           , CASE
--                                 WHEN CLIENT_COHORT IN ('Above Lending - Day 1', 'Above Lending - HD - Day 1') AND
--                                      HAS_CO_CLIENT THEN
--                                     IFF(ABOVE_MONTHLY_PAYMENT <= BEYOND_MONTHLY_DEPOSIT * 1.0, TRUE, FALSE)
--                                 ELSE TRUE
--                                 END AS FILTER_PAYMENT_INCREASE
--                           , iff(BEYOND_LOAN_STATUS_CORRECTED = 'Interested - Above' AND
--                                 BEYOND_LOAN_STATUS_DATE::DATE BETWEEN CURRENT_DATE - 13 AND CURRENT_DATE, FALSE,
--                                 TRUE) AS FILTER_ABOVE_INTERESTED
                          , IFF(CLIENT_COHORT <> 'None'
                                    AND COALESCE(FILTER_ABOVE_STATUS, TRUE)
                                    AND COALESCE(FILTER_BEYOND_STATUS, TRUE)
                                    AND COALESCE(FILTER_BAD_DISPOSITION, TRUE)
--                                     AND coalesce(FILTER_ABOVE_INTERESTED, TRUE)
                                    AND COALESCE(FILTER_DEPOSIT_ADHERENCE, TRUE)
                                    AND COALESCE(FILTER_LANGUAGE, TRUE)
                                    AND COALESCE(FILTER_LOAN_AMOUNT, TRUE)
                                    AND COALESCE(FILTER_NEXT_DEPOSIT_DATE, TRUE)
                                    AND COALESCE(FILTER_PAYMENT_INCREASE, TRUE)
                                    AND COALESCE(FILTER_CREDIT_FLAGS, TRUE), TRUE, FALSE) AS IS_TARGETED
                          , IFF(IS_TARGETED, CLIENT_COHORT, NULL) AS DIALER_CAMPAIGN
                     FROM ALL_DATA
                     )

SELECT LOADED_DATE
     , PROGRAM_NAME
     , PROGRAM_ID
     , ENROLLED_DATE
     , PROGRAM_AGE_GROUP
     , SOURCE_SYSTEM
     , STATE
     , TELEPHONE_NUMBER
     , AMOUNT_FINANCED
     , HAS_CO_CLIENT
     , CLIENT_COHORT
     , BEYOND_LOAN_STATUS
     , BEYOND_LOAN_STATUS_DATE
     , BEYOND_LOAN_STATUS_CORRECTED
     ----
--      , BEDROCK_LOAN_APPLICATION_DATE
--      , BEDROCK_LOAN_APPLICATION_STATUS
     ----
     , ABOVE_LOAN_STATUS
     , ABOVE_APPLICATION_DATE
     , LAST_BAD_DISPOSITION
     , LAST_BAD_DISPOSITION_TS
     , DEPOSIT_FREQUENCY
     , BEYOND_MONTHLY_DEPOSIT
     , ABOVE_MONTHLY_PAYMENT
     , NEXT_DEPOSIT_DATE_SCHEDULED
     , NEXT_DEPOSIT_DATE_PROCESSED
     , DEPOSIT_ADHERENCE_3M
     , DEPOSIT_ADHERENCE_6M
     , BF_REMAINING_DEPOSITS
     , HISTORICAL_SETTLEMENT_PERCENT
     , IS_OFF_COURSE
     , FILTER_BAD_DISPOSITION
     , FILTER_BEYOND_STATUS
     , FILTER_ABOVE_STATUS
     , FILTER_LOAN_AMOUNT
     , FILTER_PAYMENT_INCREASE
     , FILTER_NEXT_DEPOSIT_DATE
     , FILTER_LANGUAGE
     , FILTER_DEPOSIT_ADHERENCE
     , IS_TARGETED
     , DIALER_CAMPAIGN
     , SERVICE_ENTITY_NAME
     , ON_REACTIVATION_LIST_THRU
     , ON_DNC_LIST
     , FILTER_CREDIT_FLAGS
     , CREDIT_FLAGS
FROM COHORT_FLAGS;

