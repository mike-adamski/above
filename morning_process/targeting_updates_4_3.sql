WITH ELIG_FILE_DATA AS (
                       SELECT DISTINCT
                              E.CALENDAR_DATE_CST AS CALENDAR_DATE_CST
                            , E.PROGRAM_ID AS PROGRAM_NAME
                            , E.CLIENT_ID AS PROGRAM_ID
                            , BEYOND_ENROLLMENT_DATE AS ENROLLED_DATE_CST
                            , ZIP_CODE
                            , STATE AS CLIENT_MAILING_STATE
                            , E.SOURCE_SYSTEM
                            , iff(TELEPHONE_NUMBER ILIKE '%DONOTCALL%', NULL, TELEPHONE_NUMBER) AS TELEPHONE_NUMBER
                            , AMOUNT_FINANCED
                            , ((AMOUNT_FINANCED / .95) / (((POWER(1 + (0.229 / 12), 72) - 1)) / ((0.229 / 12) * (POWER(1 + (0.229 / 12), 72))))) AS ABOVE_MONTHLY_PAYMENT
                            , DRAFT_AMOUNT * CASE
                                                 WHEN E.PAYMENT_FREQUENCY = 'Monthly' THEN 1
                                                 WHEN E.PAYMENT_FREQUENCY IN ('Semi-Monthly', 'Twice Monthly') THEN 2
                                                 WHEN E.PAYMENT_FREQUENCY = 'Bi-Weekly' THEN 26 / 12
                                                 ELSE -1 -- If payment frequency not in this list, make field negative for visibility
                                                 END AS BEYOND_MONTHLY_DEPOSIT
                            , E.PAYMENT_FREQUENCY AS DEPOSIT_FREQUENCY
                            , IFF(NEXT_PAYMENT_DATE < CURRENT_DATE, AD.ALT_DEPOSIT_DATE, NEXT_PAYMENT_DATE) AS NEXT_SCHEDULED_DEPOSIT_DATE_CST
                            , PAYMENT_ADHERENCE_RATIO_3_MONTHS AS DEPOSIT_ADHERENCE_3M
                            , PAYMENT_ADHERENCE_RATIO_6_MONTHS AS DEPOSIT_ADHERENCE_6M
                            , P.SERVICE_ENTITY_NAME
                       FROM CURATED_PROD.SUMMARY.IPL_ELIGIBILITY_DAILY_SUMMARY E
                            LEFT JOIN CURATED_PROD.CRM.PROGRAM P ON E.PROGRAM_ID = P.PROGRAM_NAME AND P.IS_CURRENT_RECORD_FLAG
                            LEFT JOIN (
                                      SELECT PROGRAM_NAME, min(SCHEDULED_DATE_CST) AS ALT_DEPOSIT_DATE
                                      FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION
                                      WHERE IS_CURRENT_RECORD_FLAG
                                        AND SCHEDULED_DATE_CST >= current_date
                                        AND NOT IS_DELETED_FLAG
                                        AND TRANSACTION_TYPE = 'Deposit'
                                      GROUP BY 1
                                      ) AD ON AD.PROGRAM_NAME = E.PROGRAM_ID
                       WHERE CALENDAR_DATE_CST = CURRENT_DATE
                       )
   , FIVE9_PHONE_LOOKUP AS (
                           SELECT DISTINCT
                                  IFF(CAMPAIGN_TYPE IN ('Inbound', '3rd party transfer'), try_to_number(ANI), try_to_number(DNIS)) AS TELEPHONE_NUMBER
                                , last_value(DISPOSITION) OVER (PARTITION BY TELEPHONE_NUMBER ORDER BY TIMESTAMP_CST DESC) AS LAST_BAD_DISPOSITION
                                , last_value(TIMESTAMP_CST) OVER (PARTITION BY TELEPHONE_NUMBER ORDER BY TIMESTAMP_CST DESC)::TIMESTAMP AS LAST_BAD_DISPOSITION_DATE_TIME_CST
                           FROM REFINED_PROD.FIVE9.CALL
                           WHERE CASE
                                     WHEN DISPOSITION IN ('Transferred To 3rd Party', 'Transferred to Lender', 'Attempted Transfer - Transferred') AND TIMESTAMP_CST::DATE > CURRENT_DATE - 14 THEN TRUE
                                     WHEN DISPOSITION IN ('Duplicate', 'Declined') AND TIMESTAMP_CST::DATE > CURRENT_DATE - 3 THEN TRUE
                                     WHEN DISPOSITION IN ('Not Interested', 'Not Interested - Post Pitch', 'Not Interested - Pre Pitch') AND TIMESTAMP_CST::DATE > CURRENT_DATE - 90 THEN TRUE
                                     WHEN DISPOSITION IN ('Attempted Transfer - Call Back Scheduled', 'Client Not Available - Post Pitch - Call Back Scheduled',
                                                          'Client Not Available - Pre Pitch - Call Back Scheduled') AND TIMESTAMP_CST::DATE > CURRENT_DATE - 7 THEN TRUE
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
                                      LEFT JOIN REFINED_PROD.BEDROCK.OPPORTUNITY AS O ON O.ID = OT.OPPORTUNITY_ID_C AND O.IS_DELETED = 'FALSE'
                                      LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT AS A ON A.ID = O.ACCOUNT_ID AND A.IS_DELETED = 'FALSE'
                                      LEFT JOIN REFINED_PROD.BEDROCK.PROGRAM_C AS P ON P.ACCOUNT_ID_C = A.ID AND P.IS_DELETED = 'FALSE' AND datediff(MONTH, P.ENROLLMENT_DATE_C, current_date) < 6
                                      LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_C AS CL ON CL.ID = OT.CR_LIABILITY_ID_C
                                      LEFT JOIN REFINED_PROD.BEDROCK.CR_C AS CR_C ON CL.CREDIT_REPORT_ID_C = CR_C.ID AND CR_C.IS_DELETED = FALSE
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
                           LEFT JOIN REFINED_PROD.BEDROCK.CR_PUBLIC_RECORD_C AS B ON A.CREDIT_REPORT_ID_C = B.CREDIT_REPORT_ID_C AND IS_DELETED = FALSE
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
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_C AS B ON A.CREDIT_REPORT_ID_C = B.CREDIT_REPORT_ID_C AND IS_DELETED = FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_COMMENT_C AS C ON B.ID = C.CR_LIABILITY_ID_C AND C.IS_DELETED = FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_COMMENT_DEF_C AS D ON D.ID = C.CR_COMMENT_DEF_ID_C AND D.IS_DELETED = FALSE
                            WHERE 1 = 1
                              AND A.CREDIT_REPORT_ID_C IS NOT NULL
                              AND B.STATUS_C IN ('Open', 'Closed')
                              AND (B.RATING_TYPE_C IN ('Repossession', 'Foreclosure') OR D.COMMENT_C IN ('FORECLOSURE PROCESS STARTED',
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
                                           ON A.CREDIT_REPORT_ID_C = B.CREDIT_REPORT_ID_C AND B.IS_DELETED = FALSE AND datediff(MONTHS, B.ACCOUNT_OPEN_DATE_C, current_date) <= 6
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_COMMENT_C AS C ON B.ID = C.CR_LIABILITY_ID_C AND C.IS_DELETED = FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_COMMENT_DEF_C AS D ON D.ID = C.CR_COMMENT_DEF_ID_C AND D.IS_DELETED = FALSE
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
                                           ON A.CREDIT_REPORT_ID_C = B.CREDIT_REPORT_ID_C AND B.IS_DELETED = FALSE AND datediff(MONTHS, B.REPORT_DATE_C, current_date) <= 6
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_COMMENT_C AS C ON B.ID = C.CR_LIABILITY_ID_C AND C.IS_DELETED = FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_COMMENT_DEF_C AS D ON D.ID = C.CR_COMMENT_DEF_ID_C AND D.IS_DELETED = FALSE
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
   , CREDIT_REPORT_FAILED_REASON AS (
                                    SELECT R.PROGRAM_NAME, array_agg(DISTINCT REASON) WITHIN GROUP (ORDER BY R.REASON) AS CREDIT_REPORT_FAILED_REASON
                                    FROM PROGRAM_TO_CREDIT_REPORT P
                                         JOIN FAIL_REASONS R ON P.PROGRAM_NAME = R.PROGRAM_NAME
                                    WHERE P.MONTHS_SINCE_ENROLLMENT BETWEEN 3 AND 5
                                    GROUP BY 1
                                    )
   , TRADES_MISSING_FEES_PAYMENTS AS (
                                     SELECT DISTINCT
                                            T.TRADELINE_NAME
                                          , T.PROGRAM_NAME
                                     FROM CURATED_PROD.CRM.TRADELINE AS T
                                          LEFT JOIN CURATED_PROD.CFT.ACTUAL_TRANSACTION AS CP
                                                    ON T.OFFER_ID = CP.OFFER_ID AND CP.IS_CURRENT_RECORD_FLAG = TRUE AND
                                                       CP.TRANSACTION_STATUS IN ('Scheduled') AND
                                                       CP.TRANSACTION_TYPE IN ('Payment')
                                          LEFT JOIN CURATED_PROD.CFT.ACTUAL_TRANSACTION AS BF
                                                    ON T.OFFER_ID = BF.OFFER_ID AND BF.IS_CURRENT_RECORD_FLAG = TRUE AND
                                                       BF.TRANSACTION_STATUS IN ('Scheduled') AND
                                                       BF.TRANSACTION_TYPE IN ('Settlement Fee')
                                          LEFT JOIN CURATED_PROD.CRM.OFFER AS O
                                                    ON T.OFFER_ID = O.OFFER_ID AND O.IS_CURRENT_RECORD_FLAG = TRUE
                                     WHERE 1 = 1
                                       AND T.TRADELINE_SETTLEMENT_SUB_STATUS IN ('APPROVED')
                                       AND T.IS_CURRENT_RECORD_FLAG = TRUE
                                       AND O.OFFER_STATUS IN ('Approved')
                                       AND (T.FEES_COLLECTED_AMOUNT = 0 OR T.FEES_COLLECTED_AMOUNT IS NULL)
                                       AND (T.FEES_OUTSTANDING_AMOUNT = 0 OR T.FEES_OUTSTANDING_AMOUNT IS NULL)
                                     )
   , OB_DIALS AS (
                 SELECT PROGRAM_NAME, count(*) AS OUTBOUND_DIAL_COUNT
                 FROM (
                      SELECT CALL.CALL_ID
                           , PROGRAM.PROGRAM_NAME
                      FROM CURATED_PROD.CALL.CALL CALL
                           LEFT JOIN CURATED_PROD.CALL.AGENT_SEGMENT AGENT_SEGMENT
                                     ON AGENT_SEGMENT.CALL_ID = CALL.CALL_ID
                                         AND AGENT_SEGMENT.DOMAIN_ID = CALL.DOMAIN_ID
                           LEFT JOIN CURATED_PROD.CALL.AGENT AGENT
                                     ON AGENT.AGENT_KEY = AGENT_SEGMENT.AGENT_KEY AND
                                        AGENT.IS_CURRENT_RECORD_FLAG = TRUE
                           LEFT JOIN CURATED_PROD.CRM.PROGRAM PROGRAM ON PROGRAM.PROGRAM_ID = CALL.PROGRAM_ID
                      WHERE CALL.CALL_CAMPAIGN ILIKE '%above%'
                        AND CALL.CALL_CAMPAIGN NOT ILIKE '%sales%'
                        AND CALL.CALL_CAMPAIGN_TYPE = 'Outbound'
                          QUALIFY row_number() OVER (PARTITION BY CALL.CALL_ID ORDER BY CALL.START_DATE_TIME_CST) = 1
                      )
                 GROUP BY 1
                 )
   , ALL_DATA AS (
                 SELECT D.*
                      , CASE
                            WHEN ENROLLED_DATE_CST > CURRENT_DATE - INTERVAL '3 months' THEN 'Too new'
                            WHEN ENROLLED_DATE_CST > CURRENT_DATE - INTERVAL '6 months' THEN 'T3-T6'
                            WHEN ENROLLED_DATE_CST > CURRENT_DATE - INTERVAL '12 months' THEN 'T6-T12'
                            WHEN ENROLLED_DATE_CST <= CURRENT_DATE - INTERVAL '12 months' THEN 'T12+'
                            ELSE '???'
                            END AS PROGRAM_AGE_GROUP
                      , coalesce(P_L.LOAN_INTEREST_STATUS_C, PLC.LOAN_APPLICATION_INTEREST_C) AS CLIENT_LOAN_INTEREST_STATUS
                      --Incorporate Bedrock Program Loan attributes
                      , PLC.LOAN_APPLICATION_STATUS_C AS BEDROCK_LOAN_APPLICATION_STATUS -- This and loan_application_interest_c are two BR fields that had been a single combined field loan_interest_status_c in Legacy splitting them up here so they can both be used in logic
                      , PLC.LOAN_APPLICATION_DATE_C AS BEDROCK_LOAN_APPLICATION_DATE
                      , COALESCE(P_L.LOAN_INTEREST_RESPONSE_DATE_C_CST, BR_INTEREST.BR_INTEREST_TS,
                                 GREATEST(F9.LAST_BAD_DISPOSITION_DATE_TIME_CST, F9PL.LAST_BAD_DISPOSITION_DATE_TIME_CST)) AS CLIENT_LOAN_INTEREST_LAST_UPDATED_DATE_TIME_CST
                      , R.CURRENT_STATUS AS ABOVE_LOAN_STATUS
                      -- Reconcile statuses in Beyond Salesforce with those coming from Above
                      , CASE
                            WHEN SOURCE_SYSTEM = 'BEDROCK' THEN CLIENT_LOAN_INTEREST_STATUS
                            WHEN CLIENT_LOAN_INTEREST_STATUS IN ('Funded', 'Graduated', 'Not Interested') THEN CLIENT_LOAN_INTEREST_STATUS
                            WHEN ABOVE_LOAN_STATUS IN ('FRONT_END_DECLINED', 'BACK_END_DECLINED') THEN 'UW Declined'
                            WHEN CLIENT_LOAN_INTEREST_STATUS = 'Interested - Skybridge' THEN 'Above - Loan In progress'
                            WHEN ABOVE_LOAN_STATUS IN ('EXPIRED') THEN NULL
                            WHEN ABOVE_LOAN_STATUS IN ('WITHDRAWN') THEN NULL
                            WHEN ABOVE_LOAN_STATUS IN ('BASIC_INFO_COMPLETE', 'ADD_INFO_COMPLETE', 'OFFERED', 'OFFERED_SELECTED', 'PENDING', 'APPROVED', 'INITIAL_TIL_SUBMIT')
                                THEN 'Above - Loan In progress'
                            WHEN ABOVE_LOAN_STATUS IN ('ONBOARDED') AND CLIENT_LOAN_INTEREST_STATUS NOT IN ('Funded', 'Graduated') THEN 'Above - Loan In progress'
                            ELSE CLIENT_LOAN_INTEREST_STATUS
                            END AS FUTURE_LOAN_STATUS
                      , R.APP_SUBMIT_DATE AS LOAN_APPLICATION_DATE_CST
                      , CASE
                            WHEN F9PL.LAST_BAD_DISPOSITION IS NULL THEN F9.LAST_BAD_DISPOSITION
                            WHEN F9.LAST_BAD_DISPOSITION IS NULL THEN F9PL.LAST_BAD_DISPOSITION
                            WHEN F9PL.LAST_BAD_DISPOSITION_DATE_TIME_CST >= F9.LAST_BAD_DISPOSITION_DATE_TIME_CST THEN F9PL.LAST_BAD_DISPOSITION
                            WHEN F9.LAST_BAD_DISPOSITION_DATE_TIME_CST >= F9PL.LAST_BAD_DISPOSITION_DATE_TIME_CST THEN F9.LAST_BAD_DISPOSITION
                            END AS LAST_BAD_DISPOSITION
                      , CASE
                            WHEN F9.LAST_BAD_DISPOSITION_DATE_TIME_CST IS NULL THEN F9PL.LAST_BAD_DISPOSITION_DATE_TIME_CST
                            WHEN F9PL.LAST_BAD_DISPOSITION_DATE_TIME_CST IS NULL THEN F9.LAST_BAD_DISPOSITION_DATE_TIME_CST
                            ELSE GREATEST(F9.LAST_BAD_DISPOSITION_DATE_TIME_CST, F9PL.LAST_BAD_DISPOSITION_DATE_TIME_CST)
                            END AS LAST_BAD_DISPOSITION_DATE_TIME_CST
                      , CASE
                            WHEN P_L.HAS_CO_CLIENT_C OR P_B.CO_CLIENT_ID_C IS NOT NULL THEN TRUE
                            ELSE FALSE
                            END AS HAS_CO_CLIENT_FLAG
                      , CASE
                            WHEN P_L.LANGUAGE_C = 'Spanish' OR PR.PREFERRED_LANGUAGE_C = 'Spanish' THEN TRUE
                            WHEN P_B.IS_SPANISH_PREFERRED_FLAG_C THEN TRUE
                            ELSE FALSE
                            END AS IS_SPANISH_SPEAKING
                      , CF.CREDIT_REPORT_FAILED_REASON
                      , coalesce(OUTBOUND_DIAL_COUNT, 0) AS OUTBOUND_DIAL_COUNT
                      , coalesce(P_B.IS_REMOVED_FROM_IPL_MARKETING_FLAG_C, FALSE) AS ON_DNC_LIST_FLAG
                 FROM ELIG_FILE_DATA D
                      LEFT JOIN (
                                SELECT *
                                FROM REFINED_PROD.ABOVE_LENDING.AGL_COMBINED_DETAIL
                                    QUALIFY rank() OVER (PARTITION BY PROGRAM_NAME ORDER BY coalesce(APP_SUBMIT_DATE, '1901-01-01') DESC, LAST_UPDATE_DATE_CST DESC , DECLINE_REASON_TEXT DESC) = 1
                                ) R ON D.PROGRAM_NAME = R.PROGRAM_NAME
                      LEFT JOIN (
                                SELECT DISTINCT
                                       coalesce(SALESFORCE_PROGRAM_ID, NU_DSE_PROGRAM_C_ID) AS SALESFORCE_ID
                                     , last_value(DISPOSITION) OVER (PARTITION BY coalesce(SALESFORCE_PROGRAM_ID, NU_DSE_PROGRAM_C_ID) ORDER BY TIMESTAMP_CST DESC) AS LAST_BAD_DISPOSITION
                                     , last_value(TIMESTAMP_CST)
                                                  OVER (PARTITION BY coalesce(SALESFORCE_PROGRAM_ID, NU_DSE_PROGRAM_C_ID) ORDER BY TIMESTAMP_CST DESC)::TIMESTAMP AS LAST_BAD_DISPOSITION_DATE_TIME_CST
                                FROM REFINED_PROD.FIVE9.CALL
                                WHERE CASE
                                          WHEN DISPOSITION IN ('Transferred To 3rd Party', 'Transferred to Lender', 'Attempted Transfer - Transferred') AND TIMESTAMP_CST::DATE > CURRENT_DATE - 14
                                              THEN TRUE
                                          WHEN DISPOSITION IN ('Duplicate', 'Declined') AND TIMESTAMP_CST::DATE > CURRENT_DATE - 3 THEN TRUE
                                          WHEN DISPOSITION IN ('Not Interested', 'Not Interested - Post Pitch', 'Not Interested - Pre Pitch') AND TIMESTAMP_CST::DATE > CURRENT_DATE - 90 THEN TRUE
                                          ELSE FALSE
                                          END
                                ) AS F9 ON F9.SALESFORCE_ID = D.PROGRAM_ID
                      LEFT JOIN FIVE9_PHONE_LOOKUP F9PL ON try_to_number(D.TELEPHONE_NUMBER) = F9PL.TELEPHONE_NUMBER
                      LEFT JOIN REFINED_PROD.SALESFORCE.NU_DSE_PROGRAM_C P_L ON D.PROGRAM_NAME = P_L.NAME AND P_L.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
                      LEFT JOIN REFINED_PROD.BEDROCK.PROGRAM_C P_B ON D.PROGRAM_NAME = P_B.NAME
                      LEFT JOIN (
                                SELECT *
                                FROM REFINED_PROD.BEDROCK.PROGRAM_LOAN_C
                                WHERE NOT IS_DELETED
                                    QUALIFY RANK() OVER (PARTITION BY PROGRAM_ID_C ORDER BY CREATED_DATE_CST DESC, NAME DESC) = 1
                                ) PLC ON PLC.PROGRAM_ID_C = P_B.ID
                      LEFT JOIN (
                                SELECT NAME
                                     , LAST_MODIFIED_DATE_CST AS BR_INTEREST_TS
                                     , LOAN_APPLICATION_INTEREST_C
                                FROM REFINED_PROD.BEDROCK.PROGRAM_LOAN_C_ARCHIVE PLA
                                WHERE TRUE
                                    QUALIFY LOAN_APPLICATION_INTEREST_C IS DISTINCT FROM LAG(LOAN_APPLICATION_INTEREST_C) OVER (PARTITION BY NAME ORDER BY LAST_MODIFIED_DATE_CST DESC)
                                        AND rank() OVER (PARTITION BY NAME ORDER BY LAST_MODIFIED_DATE_CST DESC) = 1
                                ) BR_INTEREST ON BR_INTEREST.NAME = PLC.NAME
                      LEFT JOIN REFINED_PROD.SALESFORCE.NU_DSE_PROSPECT_C AS PR ON P_L.PROSPECT_ID_C = PR.ID AND PR.IS_DELETED_FLAG = FALSE AND PR.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
                      LEFT JOIN CREDIT_REPORT_FAILED_REASON CF ON D.PROGRAM_NAME = CF.PROGRAM_NAME
                      LEFT JOIN OB_DIALS OBD ON D.PROGRAM_NAME = OBD.PROGRAM_NAME
                 )
   , COHORT_FLAGS AS (
                     SELECT *
                          , CASE
                                WHEN CLIENT_MAILING_STATE IN ('CA') THEN 'Above Lending'
                                ELSE 'CRB'
                                END AS LENDER
                          , CASE
                                WHEN datediff('month', ENROLLED_DATE_CST, current_date) = 6 THEN 'None'
                                WHEN LENDER IS NULL OR PROGRAM_NAME IN (
                                                                       SELECT PROGRAM_NAME
                                                                       FROM TRADES_MISSING_FEES_PAYMENTS
                                                                       ) THEN 'None'
                                WHEN ABOVE_LOAN_STATUS IN ('EXPIRED') THEN 'Expired App Retarget'
                                WHEN ABOVE_LOAN_STATUS IN ('FRONT_END_DECLINED', 'BACK_END_DECLINED', 'NO_OFFERS') OR FUTURE_LOAN_STATUS IN ('UW Declined') THEN 'Declined App Retarget'
                                WHEN FUTURE_LOAN_STATUS IN ('Not Interested', 'Withdrawn') OR ABOVE_LOAN_STATUS IN ('WITHDRAWN') THEN 'Not Interested Retarget'
                                WHEN SOURCE_SYSTEM = 'BEDROCK' THEN 'Regular Campaign'
                                WHEN SOURCE_SYSTEM = 'LEGACY' AND LENDER IN ('Above Lending', 'CRB') AND PROGRAM_AGE_GROUP IN ('T3-T6', 'T6-T12', 'T12+')
                                    THEN iff(AMOUNT_FINANCED > 40000, 'High Dollar Loan', 'Regular Campaign')
                                ELSE 'None'
                                END AS CLIENT_COHORT
                          , IFF(LAST_BAD_DISPOSITION IS NOT NULL, FALSE, TRUE) AS BAD_DISPOSITION_FLAG
                          , CASE
                                WHEN SOURCE_SYSTEM = 'LEGACY' AND FUTURE_LOAN_STATUS NOT IN ('Withdrawn', 'UW Declined', 'Unable to Contact', 'Interested - Above', 'Not Interested') THEN FALSE
                                WHEN FUTURE_LOAN_STATUS IN ('Withdrawn') AND CLIENT_LOAN_INTEREST_LAST_UPDATED_DATE_TIME_CST > current_date - 45 THEN FALSE
                                WHEN FUTURE_LOAN_STATUS IN ('Not Interested') AND CLIENT_LOAN_INTEREST_LAST_UPDATED_DATE_TIME_CST > current_date - 90 THEN FALSE
                                WHEN FUTURE_LOAN_STATUS IN ('UW Declined') AND CLIENT_LOAN_INTEREST_LAST_UPDATED_DATE_TIME_CST > current_date - 90 THEN FALSE
                                WHEN SOURCE_SYSTEM = 'BEDROCK' AND BEDROCK_LOAN_APPLICATION_STATUS NOT IN ('Decline', 'Withdrawn', 'Expired') AND BEDROCK_LOAN_APPLICATION_STATUS IS NOT NULL THEN FALSE
                                WHEN BEDROCK_LOAN_APPLICATION_STATUS IN ('Decline') AND datediff(DAY, BEDROCK_LOAN_APPLICATION_DATE, current_date) <= 90 THEN FALSE
                                WHEN BEDROCK_LOAN_APPLICATION_STATUS IN ('Expired') AND datediff(DAY, BEDROCK_LOAN_APPLICATION_DATE, current_date) <= (28 + 45) --45 days from loan expiration
                                    THEN FALSE
                                WHEN BEDROCK_LOAN_APPLICATION_STATUS IN ('Withdrawn') AND datediff(DAY, BEDROCK_LOAN_APPLICATION_DATE, current_date) < 45 THEN FALSE
                                WHEN ON_DNC_LIST_FLAG THEN FALSE
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
                                    THEN FALSE -- VA clients signed after 7/1 don't give them IPL until further notice
                                ELSE TRUE
                                END AS IPL_LOAN_QUALIFY_FLAG
                          , CASE
                                WHEN ABOVE_LOAN_STATUS IN ('INITIAL_TIL_SUBMIT', 'APPROVED', 'OFFERED_SELECTED', 'ADD_INFO_COMPLETE', 'OFFERED', 'PENDING', 'BASIC_INFO_COMPLETE', 'ONBOARDED')
                                    THEN FALSE
                                WHEN ABOVE_LOAN_STATUS IN ('FRONT_END_DECLINED') AND LOAN_APPLICATION_DATE_CST >= CURRENT_DATE - 90 THEN FALSE
                                WHEN ABOVE_LOAN_STATUS IN ('BACK_END_DECLINED') AND LOAN_APPLICATION_DATE_CST >= CURRENT_DATE - 90 THEN FALSE -- Temporarily hold retargeting cohorts out of the dialer
                                WHEN ABOVE_LOAN_STATUS IN ('EXPIRED') AND LOAN_APPLICATION_DATE_CST >= CURRENT_DATE - (28 + 45) --45 days from loan expiration
                                    THEN FALSE
                                WHEN ABOVE_LOAN_STATUS IN ('WITHDRAWN') AND LOAN_APPLICATION_DATE_CST >= CURRENT_DATE - 45 THEN FALSE -- Temporarily hold retargeting cohorts out of the dialer
                                ELSE TRUE
                                END AS CLIENT_CONTACT_QUALIFY_FLAG
                          , CASE
                                WHEN CLIENT_MAILING_STATE = 'CA' AND AMOUNT_FINANCED BETWEEN 5000 AND 69250 THEN TRUE
                                WHEN CLIENT_MAILING_STATE <> 'CA' AND AMOUNT_FINANCED BETWEEN 1000 AND 69250 THEN TRUE
                                ELSE FALSE
                                END AS LOAN_AMOUNT_QUALIFY_FLAG
                          , IFF(IS_SPANISH_SPEAKING, FALSE, TRUE) AS IS_LANGUAGE_ENGLISH_FLAG
                          , iff(CREDIT_REPORT_FAILED_REASON IS NOT NULL, FALSE, TRUE) AS CREDIT_REPORT_QUALIFY_LOAN_FLAG
                          , iff(datediff('month', ENROLLED_DATE_CST, current_date) >= 6 AND current_date - date_trunc('week', ENROLLED_DATE_CST)::DATE >= 180, TRUE, FALSE) AS IS_OLD_AGE_BOOK_FLAG
                          , IFF(CLIENT_COHORT <> 'None'
                                    AND COALESCE(CLIENT_CONTACT_QUALIFY_FLAG, TRUE)
                                    AND COALESCE(IPL_LOAN_QUALIFY_FLAG, TRUE)
                                    AND COALESCE(BAD_DISPOSITION_FLAG, TRUE)
                                    AND COALESCE(IS_LANGUAGE_ENGLISH_FLAG, TRUE)
                                    AND COALESCE(LOAN_AMOUNT_QUALIFY_FLAG, TRUE)
                                    AND COALESCE(CREDIT_REPORT_QUALIFY_LOAN_FLAG, TRUE)
                                    AND COALESCE(IS_OLD_AGE_BOOK_FLAG, TRUE), TRUE, FALSE) AS IS_TARGETED_FLAG
                          , IFF(IS_TARGETED_FLAG, IFF(SOURCE_SYSTEM = 'BEDROCK', 'BRC-AboveLending-OB', 'AboveLending-OB'), NULL) AS DIALER_CAMPAIGN_NAME
                          , CASE
                                WHEN NOT IS_TARGETED_FLAG THEN NULL
                                WHEN CLIENT_COHORT IN ('High Dollar Loan', 'Regular Campaign') AND OUTBOUND_DIAL_COUNT <= 5 THEN 'High Priority'
                                WHEN CLIENT_COHORT IN ('Declined App Retarget') THEN 'High Priority'
                                WHEN CLIENT_COHORT IN ('Expired App Retarget') THEN 'Medium Priority'
                                WHEN CLIENT_COHORT IN ('High Dollar Loan', 'Regular Campaign') AND OUTBOUND_DIAL_COUNT BETWEEN 6 AND 15 THEN 'Medium Priority'
                                WHEN CLIENT_COHORT IN ('Not Interested Retarget') AND OUTBOUND_DIAL_COUNT <= 15 THEN 'Medium Priority'
                                WHEN CLIENT_COHORT IN ('Not Interested Retarget') AND OUTBOUND_DIAL_COUNT >= 16 THEN 'Low Priority'
                                WHEN CLIENT_COHORT IN ('High Dollar Loan', 'Regular Campaign') AND OUTBOUND_DIAL_COUNT >= 16 THEN 'Low Priority'
                                ELSE '???'
                                END AS DIALER_PRIORITY_LIST
                     FROM ALL_DATA
                     )

SELECT CALENDAR_DATE_CST
     , PROGRAM_NAME
     , PROGRAM_ID
     , ENROLLED_DATE_CST
     , PROGRAM_AGE_GROUP
     , SOURCE_SYSTEM
     , CLIENT_MAILING_STATE
     , TELEPHONE_NUMBER
     , AMOUNT_FINANCED
     , HAS_CO_CLIENT_FLAG
     , CLIENT_COHORT
     , CLIENT_LOAN_INTEREST_STATUS
     , CLIENT_LOAN_INTEREST_LAST_UPDATED_DATE_TIME_CST
     , FUTURE_LOAN_STATUS
     , LOAN_APPLICATION_DATE_CST
     , LAST_BAD_DISPOSITION
     , LAST_BAD_DISPOSITION_DATE_TIME_CST
     , DEPOSIT_FREQUENCY
     , BEYOND_MONTHLY_DEPOSIT
     , ABOVE_MONTHLY_PAYMENT
     , NEXT_SCHEDULED_DEPOSIT_DATE_CST
     , DEPOSIT_ADHERENCE_3M
     , DEPOSIT_ADHERENCE_6M
     , CREDIT_REPORT_FAILED_REASON
     , OUTBOUND_DIAL_COUNT
     , ON_DNC_LIST_FLAG
     , BAD_DISPOSITION_FLAG
     , IPL_LOAN_QUALIFY_FLAG
     , CLIENT_CONTACT_QUALIFY_FLAG
     , LOAN_AMOUNT_QUALIFY_FLAG
     , IS_LANGUAGE_ENGLISH_FLAG
     , CREDIT_REPORT_QUALIFY_LOAN_FLAG
     , IS_OLD_AGE_BOOK_FLAG
     , IS_TARGETED_FLAG
     , DIALER_CAMPAIGN_NAME
     , DIALER_PRIORITY_LIST
     , SERVICE_ENTITY_NAME
FROM COHORT_FLAGS
WHERE SERVICE_ENTITY_NAME = 'Beyond Finance';