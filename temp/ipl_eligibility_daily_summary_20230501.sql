SELECT current_date AS CALENDAR_DATE_CST
     , IPL.CLIENT_ID
     , ACTIVATION_CODE
     , FIRST_NAME
     , LAST_NAME
     , MAILING_ADDRESS
     , CITY
     , STATE
     , ZIP_CODE
     , EMAIL_ADDRESS
     , TELEPHONE_NUMBER
     , DOB
     , SSN
     , DRAFT_AMOUNT
     , IPL.PAYMENT_FREQUENCY
     , LAST_PAYMENT_DATE
     , LAST_PAYMENT_DATE2
     , AMOUNT_FINANCED
     , ESTIMATED_BEYOND_PROGRAM_FEES
     , TOTAL_DEPOSITS
     , SETTLEMENT_AMOUNT
     , TRADELINE_NAME
     , TRADELINE_ACCOUNT_NUMBER
     , CFT_ROUTING_NUMBER
     , CFT_ACCOUNT_NUMBER
     , CFT_BANK_NAME
     , CFT_ACCOUNT_HOLDER_NAME
     , EXTERNAL_ID
     , CO_CLIENT
     , IPL.PROGRAM_ID
     , MONTHS_SINCE_ENROLLMENT
     , NEXT_PAYMENT_DATE
     , TOTAL_AMOUNT_ENROLLED_DEBT
     , BEYOND_ENROLLMENT_DATE
     , BEYOND_ENROLLMENT_STATUS
     , NSFS_3_MONTHS
     , ORIGINAL_CREDITOR
     , SETTLEMENT_PERCENT
     , SETTLED_TRADELINED_FLAG
     , PAYMENT_ADHERENCE_RATIO_3_MONTHS
     , PAYMENT_ADHERENCE_RATIO_4_MONTHS
     , PAYMENT_ADHERENCE_RATIO_6_MONTHS
     , HOLDERS_NAME
     , BANK_NAME
     , BANK_ACCOUNT_NUMBER
     , BANK_ROUTING_NUMBER
     , BANK_ACCOUNT_TYPE
     , HISTORICAL_SETTLEMENT_PERCENT
     , BF_REMAINING_DEPOSITS
     , CONSECUTIVE_PAYMENTS_COUNT
     , LIFETIME_PAYMENT_ADHERENCE
     , PROGRAM_TRADELINE_NAME
     , P.SOURCE_SYSTEM
     , P.SERVICE_ENTITY_NAME
     , LOAN_AMOUNT_DISCOUNT_FACTOR
     , ESTIMATED_LOAN_PAYMENT
     , ESTIMATED_LOAN_PAYMENT_INCREASE
     , DAYS_DELINQUENT
     , IS_LEGAL_FLAG
     , TRADELINE_BALANCE
     , SETTLEMENT_SOURCE
     , PRIOR_APPLICATION_STATUS
     , PRIOR_APPLICATION_DATE
     , TRADELINE_BEYOND_FEES
     , SETTLEMENT_PAYMENTS
     , ESTIMATED_SETTLEMENT_AMOUNT
FROM (
     SELECT CLIENT_ID
          , ACTIVATION_CODE
          , FIRST_NAME
          , LAST_NAME
          , replace(MAILING_ADDRESS, '"') AS MAILING_ADDRESS
          , CITY
          , STATE
          , ZIP_CODE
          , EMAIL_ADDRESS
          , TELEPHONE_NUMBER
          , BIRTHDATE AS DOB
          , SSN
          , DRAFT_AMOUNT
          , PAYMENT_FREQUENCY
          , LAST_PAYMENT_DATE
          , LAST_PAYMENT_DATE2
          , AMOUNT_FINANCED
          , ESTIMATED_BEYOND_PROGRAM_FEES
          , TOTAL_DEPOSITS
          , SETTLEMENT_AMOUNT
          , TRADELINE_NAME
          , TRADELINE_ACCOUNT_NUMBER
          , CFT_ROUTING_NUMBER
          , CFT_ACCOUNT_NUMBER
          , EXTERNAL_ID
          , CO_CLIENT
          , PROGRAM_ID
          , MONTHS_SINCE_ENROLLMENT
          , NEXT_PAYMENT_DATE
          , TOTAL_AMOUNT_ENROLLED_DEBT
          , BEYOND_ENROLLMENT_DATE
          , BEYOND_ENROLLMENT_STATUS
          , NSFS_3_MONTHS
          , ORIGINAL_CREDITOR
          , SETTLEMENT_PERCENT
          , SETTLED_TRADELINED_FLAG
          , PAYMENT_ADHERENCE_RATIO_3_MONTHS
          , PAYMENT_ADHERENCE_RATIO_6_MONTHS
          , HOLDER_S_NAME_C AS HOLDERS_NAME
          , BANK_NAME_C AS BANK_NAME
          , ACCOUNT_NUMBER_C AS BANK_ACCOUNT_NUMBER
          , ROUTING_NUMBER_C AS BANK_ROUTING_NUMBER
          , TYPE_C AS BANK_ACCOUNT_TYPE
          , HISTORICAL_SETTLEMENT_PERCENT
          , BF_REMAINING_DEPOSITS
          , CFT_BANK_NAME
          , CFT_ACCOUNT_HOLDER_NAME
          , PAYMENT_ADHERENCE_RATIO_4_MONTHS
          , CONSECUTIVE_PAYMENTS_COUNT
          , LIFETIME_PAYMENT_ADHERENCE
          , TL_NAME AS PROGRAM_TRADELINE_NAME
          , LOAN_AMOUNT_DISCOUNT_FACTOR
          , EST_LOAN_PAYMENT AS ESTIMATED_LOAN_PAYMENT
          , ESTIMATED_LOAN_PAYMENT_INCREASE
          , DAYS_DELINQUENT
          , IS_LEGAL_FLAG
          , TRADELINE_BALANCE
          , SETTLEMENT_SOURCE
          , PRIOR_APPLICATION_STATUS
          , PRIOR_APPLICATION_DATE
          , TRADELINE_BEYOND_FEES
          , SETTLEMENT_PAYMENTS
          , ESTIMATED_SETTLEMENT_AMOUNT
     FROM (
          WITH TRADELINE_CAL AS (
                                SELECT P.NAME AS PROGRAM_NAME
                                     , PT.NAME AS TRADELINE_NAME
                                     , PT.PROGRAM_ID_C AS PROGRAM_ID
                                     , ETC.*
                                FROM REFINED_PROD.GLUE_SERVICE_PUBLIC.ELIGIBILITY_TRADELINE_CALCS ETC
                                     JOIN REFINED_PROD.BEDROCK.PROGRAM_TRADELINE_C PT ON ETC.TRADELINE_SFID = PT.ID AND NOT PT.IS_DELETED
                                     JOIN REFINED_PROD.BEDROCK.PROGRAM_C P ON PT.PROGRAM_ID_C = P.ID AND NOT P.IS_DELETED
                                WHERE ETC.CREATED_AT_UTC::DATE = CURRENT_DATE
                                  AND PT.TRADELINE_STATUS_C NOT IN ('Completed', 'Not Enrolled', 'Removed')
                                  AND ESTIMATED_SETTLEMENT_AMOUNT > 0
                                )
             , PAYMENT_DATES AS (
                                SELECT *
                                FROM (
                                     SELECT *
                                     FROM (
                                          SELECT PROGRAM.PROGRAM_ID
                                               , PROGRAM.PROGRAM_NAME
                                               , PROGRAM.PAYMENT_FREQUENCY
                                               , PAYMENT.SCHEDULED_DATE_CST
                                               , row_number() OVER (PARTITION BY PROGRAM.PROGRAM_NAME ORDER BY PAYMENT.SCHEDULED_DATE_CST DESC) AS SEQ
                                          FROM CURATED_PROD.CRM.PROGRAM PROGRAM
                                               JOIN CURATED_PROD.CRM.SCHEDULED_TRANSACTION PAYMENT ON PAYMENT.PROGRAM_NAME = PROGRAM.PROGRAM_NAME AND PAYMENT.IS_CURRENT_RECORD_FLAG = TRUE
                                          WHERE PAYMENT.TRANSACTION_TYPE IN ('Deposit', 'Draft')
                                            AND PAYMENT.TRANSACTION_STATUS = 'Completed' AND PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
                                          )
                                     WHERE 1 = 1
                                       AND SEQ <= CASE WHEN PAYMENT_FREQUENCY = 'Monthly' THEN 1 ELSE 2 END
                                     ) TT
                                         PIVOT (max(SCHEDULED_DATE_CST) FOR SEQ IN (1,2)) AS P (PROGRAM_ID, PROGRAM_NAME, PAYMENT_FREQUENCY, PAYMENT_DATE1, PAYMENT_DATE2)
                                )
             , CFT_ACCOUNT_BALANCE AS (
                                      SELECT DISTINCT
                                             P.PROGRAM_ID AS PROGRAM_ID
                                           , P.PROGRAM_NAME
                                           , DA.PROCESSOR_CLIENT_ID
                                           , DA.CURRENT_BALANCE
                                           , DA.AVAILABLE_BALANCE
                                      FROM CURATED_PROD.CRM.PROGRAM P
                                           LEFT JOIN CURATED_PROD.CFT.DEPOSIT_ACCOUNT_BALANCE DA ON P.PROGRAM_ID = DA.PROGRAM_ID
                                      WHERE AS_OF_DATE_CST = current_date() AND P.IS_CURRENT_RECORD_FLAG = TRUE
                                      )
             , COCLIENTS AS (
                            SELECT P.PROGRAM_NAME, count(COCLIENT.CONTACT_ID) AS CT
                            FROM CURATED_PROD.CRM.PROGRAM P
                                 JOIN REFINED_PROD.BEDROCK.PROGRAM_C PC ON PC.NAME = P.PROGRAM_NAME AND PC.IS_DELETED = FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT ACT ON ACT.ID = PC.ACCOUNT_ID_C AND ACT.IS_DELETED = FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT_CONTACT_RELATION ACR ON ACR.ACCOUNT_ID = ACT.ID AND ACR.RELATIONSHIP_C = 'Client' AND ACR.IS_DELETED = FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.CONTACT CT ON ACR.CONTACT_ID = CT.ID AND CT.IS_DELETED = FALSE
                                --left join refined_prod.BEDROCK.ACCOUNT_CONTACT_RELATION ACR on ACR.CONTACT_ID=CT.ID and ACR.RELATIONSHIP_C='Client' and ACR.IS_DELETED=FALSE
                                 LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT_CONTACT_RELATION COCLIENT
                                           ON COCLIENT.ACCOUNT_ID = PC.ACCOUNT_ID_C AND COCLIENT.RELATIONSHIP_C = 'Co-Client' AND COCLIENT.IS_DELETED = FALSE
                            WHERE IS_CURRENT_RECORD_FLAG = TRUE
                            GROUP BY 1
                            )
             , NEXT_DRAFT_DATE AS (
                                  SELECT PROGRAM_ID
                                       , PROGRAM_NAME
                                       , min(SCHEDULED_DATE_CST) AS NEXT_DRAFT_DATE
                                  FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION
                                  WHERE 1 = 1
                                    AND IS_CURRENT_RECORD_FLAG = TRUE
                                    AND TRANSACTION_TYPE = 'Deposit'
                                    AND SOURCE_SYSTEM = 'BEDROCK'
                                    AND TRANSACTION_STATUS IN ('Scheduled')
                                  GROUP BY PROGRAM_NAME, PROGRAM_ID
                                  )
             , SETTLEMENT_SCHEDULE
              AS
              (
              SELECT PROGRAM_NAME
                   , PROGRAM_ID
                   , OFFER_ID
                   , OFFER_NAME
                   , TRANSACTION_STATUS
                   , TRANSACTION_TYPE
                   , -1 * SUM(TRANSACTION_AMOUNT) AS AMT
              FROM CURATED_PROD.CFT.ACTUAL_TRANSACTION
              WHERE OFFER_ID IS NOT NULL AND TRANSACTION_TYPE IN ('Settlement Fee', 'Payment')
                AND TRANSACTION_STATUS IN ('Completed', 'Scheduled', 'In_Transit', 'Pending')
                AND IS_CURRENT_RECORD_FLAG = TRUE
              GROUP BY PROGRAM_NAME, PROGRAM_ID, OFFER_ID, OFFER_NAME, TRANSACTION_STATUS, TRANSACTION_TYPE
              )
             , TL_LIST AS (
                          SELECT TL.TRADELINE_NAME
                               , TL.PROGRAM_NAME
                               , coalesce(C_ORIG.CREDITOR_NAME, CA_ORIG.CREDITOR_ALIAS_NAME) AS ORIGINAL_CREDITOR
                               , coalesce(TL.COLLECTION_AGENCY_PARENT_NAME, TL.COLLECTION_AGENCY_NAME, C_CURR.CREDITOR_NAME, CA_CURR.CREDITOR_ALIAS_NAME, C_ORIG.CREDITOR_NAME,
                                          CA_ORIG.CREDITOR_ALIAS_NAME) AS CURRENT_CREDITOR
                               , cast(LLTB.AMOUNT_C AS DECIMAL(18, 2)) AS LATEST_TL_BALANCE_AMOUNT
                               , CASE
                                     WHEN TL.ORIGINAL_SOURCE_SYSTEM IN ('LEGACY') THEN CASE WHEN TL.NEGOTIATION_BALANCE = 0 THEN NULL ELSE cast(TL.NEGOTIATION_BALANCE AS DECIMAL(18, 2)) END
                                     WHEN TL.NEGOTIATION_BALANCE IS NULL AND LATEST_TL_BALANCE_AMOUNT > TL.ENROLLED_BALANCE THEN LATEST_TL_BALANCE_AMOUNT
                                     WHEN TL.NEGOTIATION_BALANCE > 0 AND nvl(TL.NEGOTIATION_BALANCE, 0) <= nvl(LATEST_TL_BALANCE_AMOUNT, 0) THEN LATEST_TL_BALANCE_AMOUNT
                                     WHEN TL.NEGOTIATION_BALANCE > 0 AND nvl(TL.NEGOTIATION_BALANCE, 0) >= nvl(LATEST_TL_BALANCE_AMOUNT, 0) THEN cast(TL.NEGOTIATION_BALANCE AS DECIMAL(18, 2))
                                     ELSE NULL
                                     END AS NEGOTIATION_BALANCE
                               , cast(TL.ENROLLED_BALANCE AS DECIMAL(18, 2)) AS ORIGINAL_BALANCE
                               , SSP.AMT AS CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
                               , SCP.AMT AS COMPLETED_PAYMENTS
                               , cast(P.SETTLEMENT_FEE_PERCENTAGE * coalesce(TL.FEE_BASIS_BALANCE, TL.ENROLLED_BALANCE) / 100 AS DECIMAL(18, 2)) AS ESTIMATED_FEES
                               , SSF.AMT AS FEES_OUTSTANDING_AMOUNT
                               , SCF.AMT AS COMPLETED_FEES
                               , OFFER.OFFER_NAME
                               , OFFER.SETTLEMENT_AMOUNT
                               , OFFER.SETTLEMENT_SERVICE_FEE
                               , nvl(TL.CURRENT_ACCOUNT_NUMBER, TL.ORIGINAL_ACCOUNT_NUMBER) AS ACCOUNT_NUMBER
                               , TL.TRADELINE_SETTLEMENT_STATUS
                               , TL.TRADELINE_SETTLEMENT_SUB_STATUS
                               , TL.CONDITIONAL_DEBT_STATUS
                               , TL.INCLUDE_IN_PROGRAM_FLAG
                               , TL.FEE_BASIS_BALANCE
                          FROM CURATED_PROD.CRM.TRADELINE TL
                               LEFT JOIN CURATED_PROD.CRM.OFFER OFFER ON TL.TRADELINE_NAME = OFFER.TRADELINE_NAME
                              AND OFFER.IS_CURRENT_RECORD_FLAG = TRUE
                              AND OFFER.IS_CURRENT_OFFER = TRUE AND OFFER.CFT_PAYMENT_SCHEDULED_CREATED_DATE_CST IS NOT NULL
                               LEFT JOIN CURATED_PROD.CRM.CREDITOR C_ORIG ON TL.ORIGINAL_CREDITOR_ID = C_ORIG.CREDITOR_ID
                              AND C_ORIG.IS_CURRENT_RECORD_FLAG
                               LEFT JOIN CURATED_PROD.CRM.CREDITOR_ALIAS CA_ORIG ON TL.ORIGINAL_CREDITOR_ALIAS_ID = CA_ORIG.CREDITOR_ALIAS_ID
                              AND CA_ORIG.IS_CURRENT_RECORD_FLAG
                               LEFT JOIN CURATED_PROD.CRM.CREDITOR C_CURR ON TL.CURRENT_CREDITOR_ID = C_CURR.CREDITOR_ID
                              AND C_CURR.IS_CURRENT_RECORD_FLAG
                               LEFT JOIN CURATED_PROD.CRM.CREDITOR_ALIAS CA_CURR ON TL.CURRENT_CREDITOR_ALIAS_ID = CA_CURR.CREDITOR_ALIAS_ID
                              AND CA_CURR.IS_CURRENT_RECORD_FLAG
                               LEFT JOIN CURATED_PROD.CRM.PROGRAM P ON P.PROGRAM_ID = TL.PROGRAM_ID AND P.IS_CURRENT_RECORD_FLAG = TRUE
                               LEFT JOIN REFINED_PROD.BEDROCK.PROGRAM_TRADELINE_C AS TLR ON TL.TRADELINE_ID = TLR.ID AND TLR.IS_DELETED = FALSE
                               LEFT JOIN
                               (
                               SELECT ID
                                    , PROGRAM_TRADELINE_ID_C
                                    , AMOUNT_C
                                    , coalesce(BALANCE_AS_OF_DATE_TIME_C_CST, CREATED_DATE_CST) AS EFF_DATE
                                    , row_number() OVER (PARTITION BY PROGRAM_TRADELINE_ID_C ORDER BY EFF_DATE DESC ,AMOUNT_C DESC) AS RN
                               FROM REFINED_PROD.BEDROCK.TRADELINE_BALANCE_C
                               WHERE AMOUNT_C IS NOT NULL
                                   QUALIFY RN = 1
                               ) AS LTB ON LTB.PROGRAM_TRADELINE_ID_C = TLR.ID
                               LEFT JOIN REFINED_PROD.BEDROCK.TRADELINE_BALANCE_C AS LLTB ON LLTB.ID = TLR.LATEST_TRADELINE_BALANCE_ID_C
                               LEFT JOIN SETTLEMENT_SCHEDULE SSP ON OFFER.OFFER_ID = SSP.OFFER_ID AND SSP.TRANSACTION_STATUS = 'Scheduled' AND SSP.TRANSACTION_TYPE = 'Payment'
                               LEFT JOIN SETTLEMENT_SCHEDULE SSF ON OFFER.OFFER_ID = SSF.OFFER_ID AND SSF.TRANSACTION_STATUS = 'Scheduled' AND SSF.TRANSACTION_TYPE = 'Settlement Fee'
                               LEFT JOIN SETTLEMENT_SCHEDULE SCP ON OFFER.OFFER_ID = SCP.OFFER_ID AND SCP.TRANSACTION_STATUS = 'Completed' AND SCP.TRANSACTION_TYPE = 'Payment'
                               LEFT JOIN SETTLEMENT_SCHEDULE SCF ON OFFER.OFFER_ID = SCF.OFFER_ID AND SCF.TRANSACTION_STATUS = 'Completed' AND SCF.TRANSACTION_TYPE = 'Settlement Fee'
                          WHERE TL.IS_CURRENT_RECORD_FLAG = TRUE
                            AND NOT (TL.TRADELINE_SETTLEMENT_STATUS = 'SETTLED' AND TL.TRADELINE_SETTLEMENT_SUB_STATUS = 'PAID OFF') AND TL.SOURCE_SYSTEM = 'BEDROCK'
                          )
             , BANK_ACCOUNT AS (
                               SELECT CT.NAME AS HOLDER_S_NAME_C
                                    , BANK_NAME_C
                                    , ACCOUNT_NUMBER_C
                                    , ROUTING_NUMBER_C
                                    , TYPE_C
                                    , P.NAME AS NU_DSE_PROGRAM_C
                                    , STATUS_C
                                    , STATUS_DETAIL_C
                                    , VERIFICATION_STATUS_C
                                    , row_number() OVER (PARTITION BY P.NAME ORDER BY BANK_ACCOUNT.LAST_MODIFIED_DATE_CST DESC) AS RNK
                               FROM REFINED_PROD.BEDROCK.BANK_ACCOUNT_C BANK_ACCOUNT
                                    LEFT JOIN REFINED_PROD.BEDROCK.PROGRAM_C P ON P.ACCOUNT_ID_C = BANK_ACCOUNT.ACCOUNT_ID_C
                                    LEFT JOIN REFINED_PROD.BEDROCK.CONTACT CT ON CT.ACCOUNT_ID = BANK_ACCOUNT.ACCOUNT_ID_C
                                    LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT_CONTACT_RELATION ACR ON ACR.CONTACT_ID = CT.ID
                               WHERE lower(STATUS_C) = 'active'
                                 AND lower(STATUS_DETAIL_C) = 'verified'
                                 AND lower(BANK_ACCOUNT.OFAC_STATUS_C) = 'verified'
                                 AND ACR.RELATIONSHIP_C = 'Client' AND CT.IS_DELETED = FALSE AND BANK_ACCOUNT.IS_DELETED = FALSE
                                   QUALIFY RNK = 1
                               )
             , HISTORICAL_SETTLEMENT_PERCENT AS (
                                                SELECT PROGRAM_NAME
                                                     , sum(coalesce(TL_LIST.NEGOTIATION_BALANCE, TL_LIST.FEE_BASIS_BALANCE, TL_LIST.ORIGINAL_BALANCE)) AS SETTLED_BALANCE_TOTAL
                                                     , sum(TL_LIST.SETTLEMENT_AMOUNT) AS SETTLEMENT_AMOUNT_TOTAL
                                                     , CASE WHEN SETTLED_BALANCE_TOTAL > 0 THEN div0(SETTLEMENT_AMOUNT_TOTAL, SETTLED_BALANCE_TOTAL) ELSE NULL END AS HISTORICAL_SETTLEMENT_PERCENT
                                                FROM TL_LIST
                                                WHERE TRADELINE_SETTLEMENT_STATUS = 'SETTLED'
                                                  AND TRADELINE_SETTLEMENT_SUB_STATUS NOT ILIKE 'BUSTED%'
                                                GROUP BY 1
                                                )
             , REMAINING_DEPOSITS AS (
                                     SELECT T.PROGRAM_NAME
                                          , sum(TOTAL_AMOUNT) AS BF_REMAINING_DEPOSITS
                                     FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION T
                                          JOIN CURATED_PROD.CRM.PROGRAM P ON T.PROGRAM_NAME = P.PROGRAM_NAME AND P.IS_CURRENT_RECORD_FLAG
                                     WHERE T.IS_CURRENT_RECORD_FLAG
                                       AND TRANSACTION_TYPE = 'Deposit'
                                       AND SCHEDULED_DATE_CST::DATE > current_date()
                                       AND TRANSACTION_STATUS IS DISTINCT FROM 'Cancelled'
                                     GROUP BY 1
                                     )
             , DEPOSIT_ADHERENCE AS (
                                    WITH DATES AS (
--                                                   SELECT 90 AS TERM
--                                                        , DATEADD('day', -90, CURRENT_DATE()) AS STARTDATE
--                                                        , CURRENT_DATE() AS ENDDATE
--                                                   UNION ALL
                                                  SELECT 120 AS TERM
                                                       , DATEADD('day', -120, CURRENT_DATE()) AS STARTDATE
                                                       , CURRENT_DATE() AS ENDDATE
--                                                   UNION ALL
--                                                   SELECT 180 AS TERM
--                                                        , DATEADD('day', -180, CURRENT_DATE()) AS STARTDATE
--                                                        , CURRENT_DATE() AS ENDDATE
                                                  )
                                       , PRG AS (
                                                SELECT P.PROGRAM_NAME
                                                     , Last_Day(P.ENROLLED_DATE_CST) AS VINTAGE
                                                     , P.ENROLLED_DATE_CST
                                                     , P.PROGRAM_STATUS
                                                     , E.EFFECTIVE_DATE
                                                FROM CURATED_PROD.CRM.PROGRAM P
                                                     LEFT JOIN (
                                                               SELECT S.PROGRAM_NAME, min(S.RECORD_EFFECTIVE_START_DATE_TIME_CST) AS EFFECTIVE_DATE
                                                               FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION S
                                                                    LEFT JOIN (
                                                                              SELECT PROGRAM_NAME, ENROLLED_DATE_CST
                                                                              FROM CURATED_PROD.CRM.PROGRAM
                                                                              WHERE IS_CURRENT_RECORD_FLAG = TRUE
                                                                              ) P
                                                                              ON S.PROGRAM_NAME = P.PROGRAM_NAME
                                                               WHERE TRANSACTION_TYPE = 'Deposit' AND SCHEDULED_DATE_CST IS NOT NULL AND TRANSACTION_NUMBER IS NOT NULL
                                                                 AND S.SCHEDULED_DATE_CST >= P.ENROLLED_DATE_CST
                                                               GROUP BY S.PROGRAM_NAME
                                                               ) E ON P.PROGRAM_NAME = E.PROGRAM_NAME
                                                WHERE P.IS_CURRENT_RECORD_FLAG = TRUE AND P.SOURCE_SYSTEM = 'BEDROCK'
                                                )
                                       , SCHEDULES AS (
                                                      SELECT TERM
                                                           , STARTDATE
                                                           , ENDDATE
                                                           , PROGRAM_NAME
                                                           , TRANSACTION_TYPE
                                                           , ENROLLED_DATE_CST
                                                           , count(TOTAL_AMOUNT) AS SCHEDULED_COUNT
                                                           , Sum(TOTAL_AMOUNT) AS SCHEDULED_AMOUNT
                                                      FROM (


                                                           SELECT DATES.TERM
                                                                , DATES.STARTDATE
                                                                , DATES.ENDDATE
                                                                , S.PROGRAM_NAME
                                                                , S.TRANSACTION_TYPE
                                                                , S.TOTAL_AMOUNT
                                                                , S.TRANSACTION_STATUS
                                                                , S.SCHEDULED_DATE_CST
                                                                , P.ENROLLED_DATE_CST
                                                                , row_number() OVER (PARTITION BY TRANSACTION_NUMBER,DATES.TERM ORDER BY RECORD_EFFECTIVE_START_DATE_TIME_CST DESC) AS RNK
                                                           FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION S
                                                                INNER JOIN PRG P ON S.PROGRAM_NAME = P.PROGRAM_NAME
                                                                CROSS JOIN DATES
                                                           WHERE 1 = 1
                                                             AND S.RECORD_EFFECTIVE_START_DATE_TIME_CST <= greatest(DATES.STARTDATE, P.EFFECTIVE_DATE)
                                                               QUALIFY RNK = 1
                                                           )
                                                      WHERE TRANSACTION_STATUS IN
                                                            ('Scheduled', 'In Progress', 'Pending', 'Tentative', 'Completed', 'Failed', 'Processing Failed', 'Returned', 'In_Transit', 'Suspended')
                                                        AND TRANSACTION_TYPE = 'Deposit' AND SCHEDULED_DATE_CST >= STARTDATE AND SCHEDULED_DATE_CST <= ENDDATE
                                                      GROUP BY TERM, PROGRAM_NAME, TRANSACTION_TYPE, ENROLLED_DATE_CST, STARTDATE, ENDDATE
                                                      )
                                       , ACTUALS AS (
                                                    SELECT TERM
                                                         , STARTDATE
                                                         , ENDDATE
                                                         , PROGRAM_NAME
                                                         , ENROLLED_DATE_CST
                                                         , count(TRANSACTION_AMOUNT) AS ACTUAL_COUNT
                                                         , Sum(TRANSACTION_AMOUNT) AS ACTUAL_AMOUNT
                                                    FROM (
                                                         SELECT DATES.TERM
                                                              , DATES.STARTDATE
                                                              , DATES.ENDDATE
                                                              , A.PROGRAM_NAME
                                                              , A.TRANSACTION_TYPE
                                                              , A.TRANSACTION_AMOUNT
                                                              , A.TRANSACTION_STATUS
                                                              , S.SCHEDULED_DATE_CST AS TRANSACTION_DATE_CST
                                                              , P.ENROLLED_DATE_CST
                                                         FROM CURATED_PROD.CFT.ACTUAL_TRANSACTION A
                                                              CROSS JOIN DATES
                                                              LEFT JOIN CURATED_PROD.CRM.SCHEDULED_TRANSACTION S ON S.TRANSACTION_NUMBER = A.TRANSACTION_NUMBER AND S.IS_CURRENT_RECORD_FLAG = TRUE
                                                              INNER JOIN PRG P ON A.PROGRAM_NAME = P.PROGRAM_NAME
                                                         WHERE S.SCHEDULED_DATE_CST >= DATES.STARTDATE AND S.SCHEDULED_DATE_CST <= DATES.ENDDATE
                                                           AND A.IS_CURRENT_RECORD_FLAG = TRUE
                                                         )
                                                    WHERE upper(TRANSACTION_TYPE) IN ('DEPOSIT') AND upper(TRANSACTION_STATUS) IN ('COMPLETED', 'IN_TRANSIT', 'SCHEDULED', 'IN PROGRESS')
                                                    GROUP BY TERM, PROGRAM_NAME, ENROLLED_DATE_CST, STARTDATE, ENDDATE
                                                    )

                                    SELECT P.PROGRAM_NAME
                                         , DATES.TERM
                                         , any_value(DATES.STARTDATE)
                                         , any_value(DATES.ENDDATE)
                                         , any_value(VINTAGE)
                                         , nvl(sum(A.ACTUAL_AMOUNT), 0) AS ACTUAL_AMOUNT
                                         , sum(S.SCHEDULED_AMOUNT) AS SCHEDULED_AMOUNT
                                         , CASE WHEN sum(S.SCHEDULED_AMOUNT) > 0 THEN div0(nvl(sum(A.ACTUAL_AMOUNT), 0), sum(S.SCHEDULED_AMOUNT)) END AS DEPADHERENCE
                                    FROM PRG P
                                         INNER JOIN SCHEDULES S ON P.PROGRAM_NAME = S.PROGRAM_NAME
                                         INNER JOIN DATES ON S.TERM = DATES.TERM
                                         LEFT JOIN ACTUALS A ON P.PROGRAM_NAME = A.PROGRAM_NAME AND S.STARTDATE = A.STARTDATE AND A.TERM = S.TERM AND A.TERM = DATES.TERM
                                    WHERE P.PROGRAM_STATUS NOT IN ('Closed', 'Sold to 3rd Party')
                                    GROUP BY DATES.TERM, P.PROGRAM_NAME
                                    )
             , CONSECUTIVE_PAYMENTS AS (
                                       WITH ALL_PERIODS AS (
                                                           SELECT DISTINCT
                                                                  last_day(C.CALENDAR_DATE_CST) AS MONTH_END
                                                                , PROGRAM_NAME
                                                                , PAYMENT_FREQUENCY
                                                                , PROGRAM_STATUS
                                                           FROM CURATED_PROD.CRM.PROGRAM P
                                                                JOIN CURATED_PROD.CRM.CALENDAR C ON C.MONTH_END_DATE_CST BETWEEN P.ENROLLED_DATE_CST AND date_trunc('month', current_date)
                                                           WHERE P.IS_CURRENT_RECORD_FLAG
                                                             AND P.PROGRAM_STATUS <> 'Terminated'
                                                           )
                                          , CALC_PERIOD AS (
                                                           SELECT P.PROGRAM_NAME
                                                                , P.PROGRAM_STATUS
                                                                , P.PAYMENT_FREQUENCY
                                                                , AT.TRANSACTION_STATUS
                                                                , AT.SCHEDULED_DATE_CST
                                                                , P.MONTH_END AS PERIOD_DATE
                                                                , dense_rank() OVER (PARTITION BY P.PROGRAM_NAME,P.PAYMENT_FREQUENCY ORDER BY P.MONTH_END ASC) AS PERIOD
                                                           FROM ALL_PERIODS P
                                                                LEFT JOIN CURATED_PROD.CFT.ACTUAL_TRANSACTION AS AT ON P.PROGRAM_NAME = AT.PROGRAM_NAME
                                                               AND P.MONTH_END = last_day(AT.SCHEDULED_DATE_CST)
                                                               AND AT.IS_CURRENT_RECORD_FLAG
                                                               AND AT.TRANSACTION_TYPE = 'Deposit'
                                                               AND AT.SCHEDULED_DATE_CST < date_trunc('month', current_date()) -- pulling scheduled till past month only
                                                           )
                                          ,

--Counting 'Completed' transactions
                                           COMPLETED_DEPOSIT_BY_PERIOD AS
                                               (
                                               SELECT DISTINCT
                                                      PROGRAM_NAME
                                                    , PROGRAM_STATUS
                                                    , PAYMENT_FREQUENCY
                                                    , PERIOD
                                                    , sum(CASE WHEN TRANSACTION_STATUS = 'Completed' THEN 1 ELSE 0 END) OVER (PARTITION BY PROGRAM_NAME,PERIOD) AS COMPLETED_DEPOSIT_COUNT_BY_PERIOD
                                               FROM CALC_PERIOD
                                               )
                                          ,

--Finding latest period where no completed transaction has been made
                                           FIND_LATEST_FAILED_PAYMENT_PERIOD AS
                                               (
                                               SELECT DISTINCT
                                                      PROGRAM_NAME
                                                    , PROGRAM_STATUS
                                                    , PAYMENT_FREQUENCY
                                                    , PERIOD
                                                    , COMPLETED_DEPOSIT_COUNT_BY_PERIOD
                                                    , max(CASE
                                                              WHEN PAYMENT_FREQUENCY = 'Bi-Weekly' AND COMPLETED_DEPOSIT_COUNT_BY_PERIOD < 2 THEN PERIOD
                                                              WHEN PAYMENT_FREQUENCY = 'Semi-Monthly' AND COMPLETED_DEPOSIT_COUNT_BY_PERIOD < 2 THEN PERIOD
                                                              WHEN PAYMENT_FREQUENCY = 'Monthly' AND COMPLETED_DEPOSIT_COUNT_BY_PERIOD < 1 THEN PERIOD
                                                              ELSE 0
                                                              END) OVER (PARTITION BY PROGRAM_NAME) AS LATEST_FAILED_PAYMENT_PERIOD
                                               FROM COMPLETED_DEPOSIT_BY_PERIOD
                                               )

                                       SELECT PROGRAM_NAME
                                            , PROGRAM_STATUS
                                            , PAYMENT_FREQUENCY
                                            , sum(COMPLETED_DEPOSIT_COUNT_BY_PERIOD) AS CONSECUTIVE_PAYMENTS_COUNT
                                       FROM FIND_LATEST_FAILED_PAYMENT_PERIOD
                                       WHERE PERIOD > LATEST_FAILED_PAYMENT_PERIOD
                                       GROUP BY 1, 2, 3
                                       )
             , LIFETIME_DEPOSIT_ADHERENCE AS
              (
              WITH SCHEDULES AS (
                                SELECT S.PROGRAM_ID
                                     , S.PROGRAM_NAME
                                     , SUM(TOTAL_AMOUNT) AS SCHEDULED_AMOUNT
                                FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION S
                                WHERE S.SCHEDULED_DATE_CST <= CURRENT_DATE - 1
                                  AND IS_ORIGINAL_ENROLLMENT_FLAG = TRUE
                                  AND TRANSACTION_STATUS IN
                                      ('Scheduled', 'In Progress', 'Pending', 'Tentative', 'Completed', 'Failed', 'Processing Failed', 'Returned', 'In_Transit', 'Suspended', 'Cancelled')
                                  AND TRANSACTION_TYPE = 'Deposit'
                                GROUP BY S.PROGRAM_ID, S.PROGRAM_NAME
                                )
                 , ACTUALS AS (
                              SELECT A.PROGRAM_ID
                                   , A.PROGRAM_NAME
                                   , SUM(TRANSACTION_AMOUNT) AS ACTUAL_AMOUNT
                              FROM CURATED_PROD.CFT.ACTUAL_TRANSACTION A
                              WHERE A.SCHEDULED_DATE_CST <= CURRENT_DATE - 1
                                AND A.IS_CURRENT_RECORD_FLAG = TRUE
                                AND UPPER(TRANSACTION_TYPE) IN ('DEPOSIT', 'LOAN') AND UPPER(TRANSACTION_STATUS) IN ('COMPLETED', 'IN_TRANSIT')
                              GROUP BY A.PROGRAM_ID, A.PROGRAM_NAME
                              )

              SELECT S.PROGRAM_ID
                   , S.PROGRAM_NAME
                   , div0(nvl(ACTUAL_AMOUNT, 0), SCHEDULED_AMOUNT) AS LIFETIME_PAYMENT_ADHERENCE
              FROM SCHEDULES S
                   INNER JOIN ACTUALS A ON S.PROGRAM_ID = A.PROGRAM_ID
              )
             , DNC AS (
                      SELECT DISTINCT cast(DNC_NUMBER AS NVARCHAR) AS DNC_NUMBER
                      FROM CURATED_PROD.CALL.DNC_REPORT
                      )
             , CCPA_PHONE AS (
                             SELECT DISTINCT REPLACE(regexp_replace(CONTACT.PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') AS CCPA_PHONE
                             FROM REFINED_PROD.SALESFORCE.COMPLIANCE_REQUEST_C_VW COMPLIANCE
                                  LEFT JOIN REFINED_PROD.SALESFORCE.CONTACT ON COMPLIANCE.CONTACT_C = CONTACT.ID
                                 AND CONTACT.PHONE IS NOT NULL
                             UNION
                             SELECT REPLACE(regexp_replace(CONTACT.MOBILE_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') AS CCPA_PHONE
                             FROM REFINED_PROD.SALESFORCE.COMPLIANCE_REQUEST_C_VW COMPLIANCE
                                  LEFT JOIN REFINED_PROD.SALESFORCE.CONTACT ON COMPLIANCE.CONTACT_C = CONTACT.ID
                                 AND CONTACT.MOBILE_PHONE IS NOT NULL
                             UNION
                             SELECT REPLACE(regexp_replace(CONTACT.HOME_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') AS CCPA_PHONE
                             FROM REFINED_PROD.SALESFORCE.COMPLIANCE_REQUEST_C_VW COMPLIANCE
                                  LEFT JOIN REFINED_PROD.SALESFORCE.CONTACT ON COMPLIANCE.CONTACT_C = CONTACT.ID
                                 AND CONTACT.HOME_PHONE IS NOT NULL
                             )
             , CCPA_EMAIL AS (
                             SELECT DISTINCT COMPLIANCE.FROM_ADDRESS_C AS CCPA_EMAIL
                             FROM REFINED_PROD.SALESFORCE.COMPLIANCE_REQUEST_C_VW COMPLIANCE
                                  LEFT JOIN REFINED_PROD.SALESFORCE.CONTACT ON COMPLIANCE.CONTACT_C = CONTACT.ID
                                 AND COMPLIANCE.FROM_ADDRESS_C IS NOT NULL
                             UNION
                             SELECT CONTACT.EMAIL AS CCPA_EMAIL
                             FROM REFINED_PROD.SALESFORCE.COMPLIANCE_REQUEST_C_VW COMPLIANCE
                                  LEFT JOIN REFINED_PROD.SALESFORCE.CONTACT ON COMPLIANCE.CONTACT_C = CONTACT.ID
                                 AND CONTACT.EMAIL IS NOT NULL
                             UNION
                             --Include all Sendgrid and Maripost OptOuts
                             SELECT DISTINCT NATIVE_ID AS EMAIL
                             FROM REFINED_PROD.SIMON_DATA.SENDGRID_UNSUBSCRIBES
                             --UNION
                             --Select Distinct EMAIL
                             --FROM refined_prod.MAROPOST.UNSUBSCRIBES U
                             --LEFT JOIN refined_prod.MAROPOST.CONTACTS  C on U.CONTACT_ID=C.CONTACT_ID
                             )
             , CURRENT_DRAFT_AMT AS (
                                    SELECT A.PROGRAM_ID
                                         , A.PROGRAM_NAME
                                         , P.PROGRAM_STATUS
                                         , P.PAYMENT_FREQUENCY
                                         , A.PRINCIPAL_AMOUNT_INCLUDING_FEES_PER_FREQUENCY AS PER_FREQ_AMOUNT
                                         , CASE
                                               WHEN P.PAYMENT_FREQUENCY IN ('Twice Monthly', 'Semi-Monthly') THEN PER_FREQ_AMOUNT * 24 / 12
                                               WHEN P.PAYMENT_FREQUENCY IN ('Bi-Weekly') THEN PER_FREQ_AMOUNT * 26 / 12
                                               WHEN P.PAYMENT_FREQUENCY IN ('Monthly') THEN PER_FREQ_AMOUNT
                                               END AS AMOUNT
                                         , CASE
                                               WHEN datediff(MONTH, ENROLLED_DATE_CST, current_date) <= 3 THEN ((POWER(1 + (0.27 / 12), 57) - 1)) /
                                                                                                               ((0.27 / 12) * (POWER(1 + (0.27 / 12), 57)))
                                               WHEN datediff(MONTH, ENROLLED_DATE_CST, current_date) = 4 THEN ((POWER(1 + (0.256 / 12), 57) - 1)) /
                                                                                                              ((0.2560 / 12) * (POWER(1 + (0.2560 / 12), 57)))
                                               WHEN datediff(MONTH, ENROLLED_DATE_CST, current_date) = 5 THEN ((POWER(1 + (0.249 / 12), 57) - 1)) /
                                                                                                              ((0.249 / 12) * (POWER(1 + (0.249 / 12), 57)))
                                               WHEN datediff(MONTH, ENROLLED_DATE_CST, current_date) >= 6 THEN ((POWER(1 + (0.249 / 12), 57) - 1)) /
                                                                                                               ((0.249 / 12) * (POWER(1 + (0.249 / 12), 57)))
                                               END AS DISCOUNT_FACTOR
                                    FROM (
                                         SELECT DISTINCT
                                                PROGRAM_ID
                                              , PROGRAM_NAME
                                              , MODE(TOTAL_AMOUNT) AS PRINCIPAL_AMOUNT_INCLUDING_FEES_PER_FREQUENCY
                                         FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION
                                         WHERE IS_CURRENT_RECORD_FLAG = TRUE
                                           AND TRANSACTION_TYPE = 'Deposit'
                                           AND SOURCE_SYSTEM = 'BEDROCK'
                                           AND SCHEDULED_DATE_CST >= current_date()
                                           AND TRANSACTION_STATUS NOT IN ('Cancelled', 'Suspended')
                                         GROUP BY PROGRAM_NAME, PROGRAM_ID
                                         ) A
                                         LEFT JOIN CURATED_PROD.CRM.PROGRAM P ON P.IS_CURRENT_RECORD_FLAG = TRUE AND P.PROGRAM_NAME = A.PROGRAM_NAME
                                    )
             , ACTIVE_DEBTS AS (
                               SELECT T.PROGRAM_ID
                                    , count(DISTINCT T.PROGRAM_ID) AS ACTIVE_DEBTS
                                    , sum(coalesce(T.NEGOTIATION_BALANCE, T.FEE_BASIS_BALANCE, T.ENROLLED_BALANCE)) AS CURRENT_DEBT_BALANCE
                                    , sum(nvl(O.SETTLEMENT_AMOUNT, 0)) AS OFFER_AMOUNT
                                    , CASE
                                          WHEN OFFER_AMOUNT IS NOT NULL THEN CURRENT_DEBT_BALANCE
                                          ELSE 0
                                          END AS UNSETTLED_DEBT
                               FROM CURATED_PROD.CRM.TRADELINE T
                                    LEFT JOIN CURATED_PROD.CRM.OFFER O
                                              ON O.TRADELINE_NAME = T.TRADELINE_NAME AND O.IS_CURRENT_RECORD_FLAG = TRUE AND O.IS_CURRENT_OFFER = TRUE AND O.OFFER_ACCEPTED_DATE_CST IS NOT NULL AND
                                                 coalesce(O.OFFER_CANCELLED_DATE_CST, O.SETTLEMENT_BUST_DATE_CST) IS NULL
                               WHERE T.IS_CURRENT_RECORD_FLAG = TRUE AND (T.INCLUDE_IN_PROGRAM_FLAG = TRUE OR (T.CONDITIONAL_DEBT_STATUS IN ('Pending') AND T.TRADELINE_SETTLEMENT_STATUS = 'ENROLLED'))
                                 AND O.OFFER_ID IS NULL
                               GROUP BY 1
                               )
             , NUM_OF_SETTLEMENTS AS (
                                     SELECT T.PROGRAM_NAME
                                          , T.PROGRAM_ID
                                          , count(*) AS CT_OF_SETTLEMENTS
                                          , SUM(SSP.AMT) AS TERM_PAY_BALANCE
                                          , SUM(SSF.AMT) AS FEES_OUTSTANDING
                                     FROM CURATED_PROD.CRM.TRADELINE T
                                          LEFT JOIN CURATED_PROD.CRM.OFFER O ON T.TRADELINE_NAME = O.TRADELINE_NAME AND O.IS_CURRENT_RECORD_FLAG = TRUE AND O.IS_CURRENT_OFFER = TRUE
                                          LEFT JOIN SETTLEMENT_SCHEDULE SSP ON O.OFFER_ID = SSP.OFFER_ID AND SSP.TRANSACTION_TYPE = 'Payment' AND SSP.TRANSACTION_STATUS IN ('Pending', 'Scheduled')
                                          LEFT JOIN SETTLEMENT_SCHEDULE SSF
                                                    ON O.OFFER_ID = SSF.OFFER_ID AND SSF.TRANSACTION_TYPE = 'Settlement Fee' AND SSF.TRANSACTION_STATUS IN ('Pending', 'Scheduled')
                                     WHERE T.IS_CURRENT_RECORD_FLAG = TRUE
                                       AND (T.INCLUDE_IN_PROGRAM_FLAG = TRUE OR (T.CONDITIONAL_DEBT_STATUS IN ('Pending') AND T.TRADELINE_SETTLEMENT_STATUS = 'ENROLLED'))
                                       AND O.CFT_PAYMENT_SCHEDULED_CREATED_DATE_CST IS NOT NULL
                                       AND coalesce(O.SETTLEMENT_BUST_DATE_CST, O.OFFER_CANCELLED_DATE_CST) IS NULL
                                     GROUP BY T.PROGRAM_NAME, T.PROGRAM_ID
                                     )
             -- Get client credit score at enrollment; This is a temporary workaround while product work is done to write this data point directly to the database
             , ENROLLMENT_CREDIT_SCORE AS (
                                          -- Union together results and take lower (more conservative) credit score if credit score is found through both join methods
                                          SELECT PROGRAM_NAME, MIN(CREDIT_SCORE_C) AS ENROLLMENT_CREDIT_SCORE
                                          FROM (
                                               /*
                                               The majority of enrollment credit report data is linked to contacts, some are linked to opportunities, some are linked to both.
                                               Join credit reports to programs via both objects and take the lower score of the two
                                                 */

                                               -- Join via opportunity id
                                               SELECT P.NAME AS PROGRAM_NAME, CR.CREDIT_SCORE_C
                                               FROM REFINED_PROD.BEDROCK.PROGRAM_C P
                                                    LEFT JOIN (
                                                              SELECT *
                                                              FROM REFINED_PROD.BEDROCK.CR_C
                                                              WHERE PURPOSE_C IS DISTINCT FROM 'Credit Refresh' AND STATUS_C = 'COMPLETED'
                                                                  QUALIFY RANK() OVER (PARTITION BY OPPORTUNITY_ID_C ORDER BY CREATED_DATE_CST) = 1
                                                              ) CR ON P.OPPORTUNITY_ID_C = CR.OPPORTUNITY_ID_C
                                               WHERE PROGRAM_STATUS_C = 'Enrolled'

                                               UNION

                                               -- Join via contact id
                                               SELECT P.NAME, CR.CREDIT_SCORE_C
                                               FROM REFINED_PROD.BEDROCK.CR_C CR
                                                    JOIN REFINED_PROD.BEDROCK.CONTACT C ON CR.CONTACT_ID_C = C.ID AND NOT C.IS_DELETED
                                                    JOIN REFINED_PROD.BEDROCK.ACCOUNT A ON C.ACCOUNT_ID = A.ID AND NOT A.IS_DELETED
                                                    JOIN (
                                                         SELECT *
                                                         FROM REFINED_PROD.BEDROCK.PROGRAM_C
                                                         WHERE PROGRAM_STATUS_C = 'Enrolled'
                                                             QUALIFY RANK() OVER (PARTITION BY ACCOUNT_ID_C ORDER BY ENROLLMENT_DATE_C DESC) = 1
                                                         ) P ON P.ACCOUNT_ID_C = A.ID AND NOT P.IS_DELETED
                                               WHERE CR.STATUS_C = 'COMPLETED'
                                                 AND CR.PURPOSE_C IS DISTINCT FROM 'Credit Refresh'
                                                   QUALIFY RANK() OVER (PARTITION BY P.ID ORDER BY CR.CREATED_DATE_CST) = 1
                                               )
                                          GROUP BY 1
                                          )

          SELECT DISTINCT
                 EPC.PROGRAM_SFID AS CLIENT_ID
               , right(left(P.PROGRAM_ID, 15), 3) || right(left(P.CLIENT_ID, 15), 3) AS ACTIVATION_CODE
               , CT.FIRST_NAME
               , CT.LAST_NAME
               , CT.MAILING_STREET AS MAILING_ADDRESS
               , CT.MAILING_CITY AS CITY
               , left(CT.MAILING_POSTAL_CODE, 5) AS ZIP_CODE
               , CT.MAILING_STATE AS STATE
               , CT.EMAIL AS EMAIL_ADDRESS
               , REPLACE(regexp_replace(nvl(CT.MOBILE_PHONE, nvl(CT.HOME_PHONE, nvl(CT.PHONE, CT.OTHER_PHONE))), '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') AS TELEPHONE_NUMBER
               , CT.BIRTHDATE
               , CT.SSN_C AS SSN
               , nvl(CURRENT_DRAFT_AMT.PER_FREQ_AMOUNT, 0) AS DRAFT_AMOUNT
               , CASE WHEN P.PAYMENT_FREQUENCY = 'Semi-Monthly' THEN 'Twice Monthly' ELSE P.PAYMENT_FREQUENCY END AS PAYMENT_FREQUENCY
               , PD.PAYMENT_DATE1 AS LAST_PAYMENT_DATE
               , PD.PAYMENT_DATE2 AS LAST_PAYMENT_DATE2
               , EPC.ESTIMATED_LOAN_AMOUNT AS AMOUNT_FINANCED
               , sum(coalesce(TC.BEYOND_FEES, 0)) OVER (PARTITION BY P.PROGRAM_ID) + COALESCE(/*EPC.CFT_MONHTLY_FEE*/ 64.5, 0) +
                 COALESCE(/*LEGAL_SERVICE_FEE*/ 89.7, 0) AS ESTIMATED_BEYOND_PROGRAM_FEES
               , TA.BALANCE_C AS TOTAL_DEPOSITS
               , TC.SETTLEMENT_PAYMENTS AS SETTLEMENT_AMOUNT
               , coalesce(C2.CREDITOR_NAME, TL_LIST.CURRENT_CREDITOR) AS TRADELINE_NAME -- Current creditor
               , '' AS TRADELINE_ACCOUNT_NUMBER
               , try_to_number(TA.ROUTING_NUMBER_C) AS CFT_ROUTING_NUMBER
               , try_to_number(TA.ACCOUNT_NUMBER_C) AS CFT_ACCOUNT_NUMBER
               , CASE
                     WHEN try_to_number(TA.ROUTING_NUMBER_C) = 053101561 THEN 'WELLS FARGO BANK'
                     WHEN try_to_number(TA.ROUTING_NUMBER_C) = 053112505 THEN 'AXOS BANK'
                     ELSE NULL
                     END AS CFT_BANK_NAME
               , concat(CT.FIRST_NAME, ' ', CT.LAST_NAME) AS CFT_ACCOUNT_HOLDER_NAME
               , CFT_ACCOUNT_BALANCE.PROCESSOR_CLIENT_ID :: VARCHAR AS EXTERNAL_ID
               , CASE WHEN COCLIENTS.CT > 0 THEN TRUE ELSE FALSE END AS CO_CLIENT
               , P.PROGRAM_NAME AS PROGRAM_ID
               , PC.IPL_MONTHS_ENROLLED_C AS MONTHS_SINCE_ENROLLMENT
               , NEXT_DRAFT_DATE.NEXT_DRAFT_DATE AS NEXT_PAYMENT_DATE
               , nvl(ACTIVE_DEBTS.UNSETTLED_DEBT, 0) + nvl(NUM_OF_SETTLEMENTS.TERM_PAY_BALANCE, 0) AS TOTAL_AMOUNT_ENROLLED_DEBT
               , PC.ENROLLMENT_DATE_C AS BEYOND_ENROLLMENT_DATE
               , P.PROGRAM_STATUS AS BEYOND_ENROLLMENT_STATUS
               , PC.IPL_DEPOSITS_FAILED_LAST_90_DAYS_C AS NSFS_3_MONTHS
               , coalesce(C.CREDITOR_NAME, TL_LIST.ORIGINAL_CREDITOR) AS ORIGINAL_CREDITOR -- Enrolled creditor
               , COALESCE(TC.SETTLEMENT_RATE / 100, ROUND(DIV0(O.OFFER_AMOUNT_C, NTB.AMOUNT_C), 2), 0) AS SETTLEMENT_PERCENT
               , TL_LIST.TRADELINE_SETTLEMENT_STATUS || ' - ' || TL_LIST.TRADELINE_SETTLEMENT_SUB_STATUS AS SETTLED_TRADELINED_FLAG
               , round(div0(EPC.COMPLETED_DEPOSITS_AMOUNT_LAST_90_DAYS, EPC.SCHEDULED_DEPOSITS_AMOUNT_LAST_90_DAYS), 2) AS PAYMENT_ADHERENCE_RATIO_3_MONTHS
               , coalesce(DA120.DEPADHERENCE, 0) AS PAYMENT_ADHERENCE_RATIO_4_MONTHS
               , round(div0(EPC.COMPLETED_DEPOSITS_AMOUNT_LAST_180_DAYS, EPC.SCHEDULED_DEPOSITS_AMOUNT_LAST_180_DAYS), 2) AS PAYMENT_ADHERENCE_RATIO_6_MONTHS
               , BANK_ACCOUNT.HOLDER_S_NAME_C
               , BANK_ACCOUNT.BANK_NAME_C
               , BANK_ACCOUNT.ACCOUNT_NUMBER_C
               , BANK_ACCOUNT.ROUTING_NUMBER_C
               , BANK_ACCOUNT.TYPE_C
               , HISTORICAL_SETTLEMENT_PERCENT.HISTORICAL_SETTLEMENT_PERCENT
               , REMAINING_DEPOSITS.BF_REMAINING_DEPOSITS
               , TC.TRADELINE_NAME AS TL_NAME
               , CONSECUTIVE_PAYMENTS.CONSECUTIVE_PAYMENTS_COUNT
               , LDA.LIFETIME_PAYMENT_ADHERENCE
               , DISCOUNT_FACTOR AS LOAN_AMOUNT_DISCOUNT_FACTOR
--                , EPC.LOAN_AMOUNT_DISCOUNT_FACTOR
               , EPC.ESTIMATED_LOAN_AMOUNT / .95 / DISCOUNT_FACTOR AS EST_LOAN_PAYMENT
--                , EPC.ESTIMATED_LOAN_PAYMENT
                 -- , EPC.ESTIMATED_LOAN_PAYMENT_INCREASE
                 ---- Temporary logic which can be reverted once product fixes the field
               , EST_LOAN_PAYMENT / NULLIFZERO(CURRENT_DRAFT_AMT.AMOUNT) AS ESTIMATED_LOAN_PAYMENT_INCREASE
               , TC.DAYS_DELINQUENT
               , TC.IS_LEGAL_FLAG
               , TC.TRADELINE_BALANCE
               , TC.SETTLEMENT_SOURCE
               , EPC.PRIOR_APPLICATION_STATUS
               , EPC.PRIOR_APPLICATION_DATE
               , TC.BEYOND_FEES AS TRADELINE_BEYOND_FEES
               , TC.SETTLEMENT_PAYMENTS
               , TC.ESTIMATED_SETTLEMENT_AMOUNT
          FROM CURATED_PROD.CRM.PROGRAM P
               JOIN REFINED_PROD.BEDROCK.PROGRAM_C PC ON PC.NAME = P.PROGRAM_NAME AND PC.IS_DELETED = FALSE
               JOIN REFINED_PROD.GLUE_SERVICE_PUBLIC.ELIGIBILITY_PROGRAM_CALCS EPC ON EPC.PROGRAM_SFID = P.PROGRAM_ID AND EPC.CREATED_AT_UTC::DATE = CURRENT_DATE
               LEFT JOIN TRADELINE_CAL TC ON TC.PROGRAM_ID = EPC.PROGRAM_SFID
               LEFT JOIN REFINED_PROD.BEDROCK.PROGRAM_TRADELINE_C PT ON TC.TRADELINE_SFID = PT.ID AND NOT PT.IS_DELETED
               LEFT JOIN REFINED_PROD.BEDROCK.OFFER_C O ON PT.ACTIVE_OFFER_ID_C = O.ID AND NOT O.IS_DELETED
               LEFT JOIN REFINED_PROD.BEDROCK.TRADELINE_BALANCE_C NTB
                         ON NTB.ID = coalesce(O.NEGOTIATION_TRADELINE_BALANCE_ID_C, PT.NEGOTIATION_TRADELINE_BALANCE_ID_C, PT.LATEST_TRADELINE_BALANCE_ID_C) AND NOT NTB.IS_DELETED
               LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT ACT ON ACT.ID = PC.ACCOUNT_ID_C AND ACT.IS_DELETED = FALSE
               LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT_CONTACT_RELATION ACR ON ACR.ACCOUNT_ID = ACT.ID AND ACR.RELATIONSHIP_C = 'Client' AND ACR.IS_DELETED = FALSE
               LEFT JOIN REFINED_PROD.BEDROCK.CONTACT CT ON ACR.CONTACT_ID = CT.ID AND CT.IS_DELETED = FALSE
               LEFT JOIN CURATED_PROD.CRM.CREDITOR C ON C.CREDITOR_ID = TC.ENROLLED_CREDITOR_SFID AND C.IS_CURRENT_RECORD_FLAG
               LEFT JOIN CURATED_PROD.CRM.CREDITOR C2 ON C2.CREDITOR_ID = TC.NEGOTIATING_CREDITOR_SFID AND C2.IS_CURRENT_RECORD_FLAG
               LEFT JOIN PAYMENT_DATES PD ON PD.PROGRAM_ID = P.PROGRAM_ID
               LEFT JOIN CFT_ACCOUNT_BALANCE ON CFT_ACCOUNT_BALANCE.PROGRAM_ID = P.PROGRAM_ID
               LEFT JOIN COCLIENTS ON COCLIENTS.PROGRAM_NAME = P.PROGRAM_NAME
               LEFT JOIN NEXT_DRAFT_DATE ON NEXT_DRAFT_DATE.PROGRAM_NAME = P.PROGRAM_NAME
               LEFT JOIN TL_LIST ON TL_LIST.TRADELINE_NAME = TC.TRADELINE_NAME AND
                                    (TL_LIST.INCLUDE_IN_PROGRAM_FLAG = TRUE OR (TL_LIST.CONDITIONAL_DEBT_STATUS IN ('Pending') AND TL_LIST.TRADELINE_SETTLEMENT_STATUS = 'ENROLLED'))
               LEFT JOIN BANK_ACCOUNT ON BANK_ACCOUNT.NU_DSE_PROGRAM_C = P.PROGRAM_NAME
               LEFT JOIN HISTORICAL_SETTLEMENT_PERCENT ON HISTORICAL_SETTLEMENT_PERCENT.PROGRAM_NAME = P.PROGRAM_NAME
               LEFT JOIN REMAINING_DEPOSITS ON REMAINING_DEPOSITS.PROGRAM_NAME = P.PROGRAM_NAME
               LEFT JOIN REFINED_PROD.BEDROCK.TRUST_ACCOUNT_C TA ON P.PROGRAM_ID = TA.PROGRAM_ID_C AND TA.IS_DELETED = FALSE
               LEFT JOIN DEPOSIT_ADHERENCE DA120 ON DA120.PROGRAM_NAME = P.PROGRAM_NAME AND DA120.TERM = 120
               LEFT JOIN CONSECUTIVE_PAYMENTS ON CONSECUTIVE_PAYMENTS.PROGRAM_NAME = P.PROGRAM_NAME
               LEFT JOIN LIFETIME_DEPOSIT_ADHERENCE LDA ON LDA.PROGRAM_NAME = P.PROGRAM_NAME
               LEFT JOIN CCPA_PHONE CCPA1 ON REPLACE(regexp_replace(CT.MOBILE_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = CCPA1.CCPA_PHONE
               LEFT JOIN CCPA_PHONE CCPA2 ON REPLACE(regexp_replace(CT.HOME_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = CCPA2.CCPA_PHONE
               LEFT JOIN CCPA_PHONE CCPA3 ON REPLACE(regexp_replace(CT.OTHER_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = CCPA3.CCPA_PHONE
               LEFT JOIN CCPA_PHONE CCPA4 ON REPLACE(regexp_replace(CT.PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') = CCPA4.CCPA_PHONE
               LEFT JOIN CCPA_EMAIL CCPA5 ON CT.EMAIL = CCPA5.CCPA_EMAIL
               LEFT JOIN DNC DNC1 ON DNC1.DNC_NUMBER = REPLACE(regexp_replace(CT.MOBILE_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
               LEFT JOIN DNC DNC2 ON DNC2.DNC_NUMBER = REPLACE(regexp_replace(CT.PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
               LEFT JOIN DNC DNC3 ON DNC3.DNC_NUMBER = REPLACE(regexp_replace(CT.HOME_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
               LEFT JOIN DNC DNC4 ON DNC4.DNC_NUMBER = REPLACE(regexp_replace(CT.OTHER_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
               LEFT JOIN CURRENT_DRAFT_AMT ON CURRENT_DRAFT_AMT.PROGRAM_ID = P.PROGRAM_ID
               LEFT JOIN ACTIVE_DEBTS ON ACTIVE_DEBTS.PROGRAM_ID = P.PROGRAM_ID
               LEFT JOIN NUM_OF_SETTLEMENTS ON NUM_OF_SETTLEMENTS.PROGRAM_ID = P.PROGRAM_ID
               LEFT JOIN ENROLLMENT_CREDIT_SCORE ECS ON ECS.PROGRAM_NAME = P.PROGRAM_NAME
          WHERE 1 = 1
            AND P.IS_CURRENT_RECORD_FLAG = TRUE
            AND PC.IPL_MONTHS_ENROLLED_C >= 6
            AND PC.LOAN_ELIGIBILITY_STATUS_C = 'Eligible'
            AND PC.PROGRAM_STATUS_C = 'Enrolled'
            AND PC.IPL_DEPOSITS_FAILED_LAST_90_DAYS_C = 0
            AND CCPA1.CCPA_PHONE IS NULL
            AND CCPA2.CCPA_PHONE IS NULL
            AND CCPA3.CCPA_PHONE IS NULL
            AND CCPA4.CCPA_PHONE IS NULL
            AND CCPA5.CCPA_EMAIL IS NULL
            AND DNC1.DNC_NUMBER IS NULL
            AND DNC2.DNC_NUMBER IS NULL
            AND DNC3.DNC_NUMBER IS NULL
            AND DNC4.DNC_NUMBER IS NULL
            AND EMAIL_ADDRESS IS NOT NULL
            AND TELEPHONE_NUMBER IS NOT NULL
            AND P.PROGRAM_NAME NOT IN (
                                      SELECT DISTINCT P.NAME
                                      FROM REFINED_PROD.BEDROCK.PROGRAM_TRADELINE_C T
                                           JOIN REFINED_PROD.BEDROCK.CREDITOR_C C ON T.NEGOTIATING_CREDITOR_ID_C = C.ID
                                           JOIN REFINED_PROD.BEDROCK.PROGRAM_C P ON T.PROGRAM_ID_C = P.ID
                                      WHERE C.NAME ILIKE '%above%lending%'
                                      )
            AND PAYMENT_ADHERENCE_RATIO_6_MONTHS >= 1
            -- Temporary logic which can be removed after this logic is built into Bedrock soon after launch
            AND NOT (STATE = 'CA' AND AMOUNT_FINANCED < 5000)
            AND NOT (STATE = 'GA' AND AMOUNT_FINANCED < 3100)
            AND NOT (STATE = 'MA' AND AMOUNT_FINANCED < 6001)
            AND NOT (STATE = 'OH' AND AMOUNT_FINANCED < 5100)
            AND NOT (STATE = 'SC' AND AMOUNT_FINANCED < 4300)
            --Temporary logic which can be reverted once product fixes the field
            AND EST_LOAN_PAYMENT / NULLIFZERO(CURRENT_DRAFT_AMT.AMOUNT) <= 1.48
            AND COALESCE(ECS.ENROLLMENT_CREDIT_SCORE, 0) >= 510
            AND EPC.ESTIMATED_LOAN_AMOUNT / 0.95 / CURRENT_DRAFT_AMT.DISCOUNT_FACTOR / nullifzero(CURRENT_DRAFT_AMT.AMOUNT) <= 1.48
            -- Temporary logic
            AND NOT STATE = 'CA'
            AND div0(AMOUNT_FINANCED / .95 / (((POWER(1 + (0.249 / 12), 60) - 1)) / ((0.249 / 12) * (POWER(1 + (0.249 / 12), 60)))), DRAFT_AMOUNT *
                                                                                                                                     CASE
                                                                                                                                         WHEN P.PAYMENT_FREQUENCY = 'Bi-Weekly' THEN 26 / 12
                                                                                                                                         WHEN P.PAYMENT_FREQUENCY = 'Monthly' THEN 1
                                                                                                                                         WHEN P.PAYMENT_FREQUENCY IN ('Semi-Monthly', 'Twice Monthly')
                                                                                                                                             THEN 2
                                                                                                                                         END) <= 1.4
          ) A
     ) IPL
     LEFT JOIN CURATED_PROD.CRM.PROGRAM P ON P.PROGRAM_NAME = IPL.PROGRAM_ID AND P.IS_CURRENT_RECORD_FLAG
-- WHERE P.SERVICE_ENTITY_NAME = 'Beyond Finance'
     -- Temporary logic in preparation for Above FLLG launch
WHERE NOT (P.SERVICE_ENTITY_NAME = 'Beyond Finance' AND IPL.STATE IN ('GA', 'SC', 'OH'))
  AND NOT (P.SERVICE_ENTITY_NAME = 'Five Lakes Law Group' AND IPL.STATE IN ('VA'));