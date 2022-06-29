CREATE OR REPLACE VIEW SNO_SANDBOX.IPL.ELIGIBLE_RULES_BEDROCK_V AS
WITH LAST_NSF AS (
                 SELECT PROGRAM.PROGRAM_ID AS PROGRAM_ID
                      , PROGRAM.PROGRAM_NAME AS PROGRAM_NAME
                      , count(PAYMENT.TRANSACTION_ID) AS NSFS
                      , count(CASE
                                  WHEN dateadd(MONTH, -3, cast(current_date - 1 AS DATE)) < SCHEDULED_DATE_CST
                                      THEN PAYMENT.TRANSACTION_ID
                                  END) AS NSF_3_MOS
                      , max(SCHEDULED_DATE_CST) AS LAST_NSF_DT
                 FROM CURATED_PROD.CRM.PROGRAM PROGRAM
                      JOIN CURATED_PROD.CRM.SCHEDULED_TRANSACTION PAYMENT
                           ON PAYMENT.PROGRAM_ID = PROGRAM.PROGRAM_ID AND PAYMENT.IS_CURRENT_RECORD_FLAG = TRUE
                 WHERE 1 = 1
                   AND PAYMENT.TRANSACTION_TYPE IN ('Deposit')
                   AND PAYMENT.TRANSACTION_STATUS IN ('Failed', 'Returned')
                   AND PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
                   AND PROGRAM.SOURCE_SYSTEM = 'BEDROCK'
                 GROUP BY PROGRAM.PROGRAM_ID, PROGRAM.PROGRAM_NAME
                 )
   , CURRENT_DRAFT_AMT AS (
                          SELECT A.PROGRAM_ID
                               , A.PROGRAM_NAME
                               , P.PROGRAM_STATUS
                               , P.PAYMENT_FREQUENCY
                               , A.PRINCIPAL_AMOUNT_INCLUDING_FEES_PER_FREQUENCY AS PER_FREQ_AMOUNT
                               , CASE
                                     WHEN P.PAYMENT_FREQUENCY IN ('Twice Monthly', 'Semi-Monthly')
                                         THEN PER_FREQ_AMOUNT * 24 / 12
                                     WHEN P.PAYMENT_FREQUENCY IN ('Bi-Weekly') THEN PER_FREQ_AMOUNT * 26 / 12
                                     WHEN P.PAYMENT_FREQUENCY IN ('Monthly') THEN PER_FREQ_AMOUNT
                                     END AS AMOUNT
                               , CASE
                                     WHEN datediff(MONTH, P.ENROLLED_DATE_CST, current_date) <= 3 THEN
                                             ((POWER(1 + (0.27 / 12), 55) - 1)) /
                                             ((0.27 / 12) * (POWER(1 + (0.27 / 12), 55)))
                                     WHEN datediff(MONTH, P.ENROLLED_DATE_CST, current_date) = 4 THEN
                                             ((POWER(1 + (0.256 / 12), 55) - 1)) /
                                             ((0.2560 / 12) * (POWER(1 + (0.2560 / 12), 55)))
                                     WHEN datediff(MONTH, P.ENROLLED_DATE_CST, current_date) = 5 THEN
                                             ((POWER(1 + (0.2435 / 12), 55) - 1)) /
                                             ((0.2435 / 12) * (POWER(1 + (0.2435 / 12), 55)))
                                     WHEN datediff(MONTH, P.ENROLLED_DATE_CST, current_date) >= 6 THEN
                                             ((POWER(1 + (0.229 / 12), 55) - 1)) /
                                             ((0.229 / 12) * (POWER(1 + (0.229 / 12), 55)))
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
                               LEFT JOIN CURATED_PROD.CRM.PROGRAM P
                                         ON P.IS_CURRENT_RECORD_FLAG = TRUE AND P.PROGRAM_NAME = A.PROGRAM_NAME
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
   , SETTLEMENT_SCHEDULE AS (
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
                            GROUP BY PROGRAM_NAME, PROGRAM_ID, OFFER_ID, OFFER_NAME, TRANSACTION_STATUS
                                   , TRANSACTION_TYPE
                            )
   , NUM_OF_SETTLEMENTS AS (
                           --   select tl.NU_DSE_PROGRAM_C program_id,
                           --         count(*) ct_of_settlements,
                           --         SUM(cast(tl.creditor_payments_outstanding_c as decimal(10, 2))) term_pay_balance,
                           --         SUM(tl.fees_outstanding_c) fees_outstanding
                           --   from REFINED_PROD.SALESFORCE.NU_DSE_TRADE_LINE_C tl
                           --   join REFINED_PROD.SALESFORCE.NU_DSE_OFFER_C offer on offer.NU_DSE_TRADE_LINE_C = tl.id
                           --   where lower(offer.NU_DSE_STATUS_C) like '%accepted%'
                           --   group by 1
                           SELECT T.PROGRAM_NAME
                                , T.PROGRAM_ID
                                , count(*) AS CT_OF_SETTLEMENTS
                                , SUM(SSP.AMT) AS TERM_PAY_BALANCE
                                , SUM(SSF.AMT) AS FEES_OUTSTANDING
                           FROM CURATED_PROD.CRM.TRADELINE T
                                LEFT JOIN CURATED_PROD.CRM.OFFER O
                                          ON T.TRADELINE_NAME = O.TRADELINE_NAME AND O.IS_CURRENT_RECORD_FLAG = TRUE AND
                                             O.IS_CURRENT_OFFER = TRUE
                                LEFT JOIN SETTLEMENT_SCHEDULE SSP
                                          ON O.OFFER_ID = SSP.OFFER_ID AND SSP.TRANSACTION_TYPE = 'Payment' AND
                                             SSP.TRANSACTION_STATUS IN ('Pending', 'Scheduled')
                                LEFT JOIN SETTLEMENT_SCHEDULE SSF
                                          ON O.OFFER_ID = SSF.OFFER_ID AND SSF.TRANSACTION_TYPE = 'Settlement Fee' AND
                                             SSF.TRANSACTION_STATUS IN ('Pending', 'Scheduled')
                           WHERE T.IS_CURRENT_RECORD_FLAG = TRUE
                             AND (T.INCLUDE_IN_PROGRAM_FLAG = TRUE OR (T.CONDITIONAL_DEBT_STATUS IN ('Pending') AND
                                                                       T.TRADELINE_SETTLEMENT_STATUS = 'ENROLLED'))
                             AND O.CFT_PAYMENT_SCHEDULED_CREATED_DATE_CST IS NOT NULL
                             AND coalesce(O.SETTLEMENT_BUST_DATE_CST, O.OFFER_CANCELLED_DATE_CST) IS NULL
                           GROUP BY T.PROGRAM_NAME, T.PROGRAM_ID
                           )
   , ACTIVE_DEBTS AS (
                     SELECT T.PROGRAM_ID
                          , count(DISTINCT T.PROGRAM_ID) AS ACTIVE_DEBTS
                          , sum(coalesce(T.NEGOTIATION_BALANCE, T.FEE_BASIS_BALANCE,
                                         T.ENROLLED_BALANCE)) AS CURRENT_DEBT_BALANCE
                          , sum(nvl(O.SETTLEMENT_AMOUNT, 0)) AS OFFER_AMOUNT
                          , CASE
                                WHEN OFFER_AMOUNT IS NOT NULL THEN CURRENT_DEBT_BALANCE
                                ELSE 0
                                END AS UNSETTLED_DEBT
                     FROM CURATED_PROD.CRM.TRADELINE T
                          LEFT JOIN CURATED_PROD.CRM.OFFER O
                                    ON O.TRADELINE_NAME = T.TRADELINE_NAME AND O.IS_CURRENT_RECORD_FLAG = TRUE AND
                                       O.IS_CURRENT_OFFER = TRUE AND O.OFFER_ACCEPTED_DATE_CST IS NOT NULL AND
                                       coalesce(O.OFFER_CANCELLED_DATE_CST, O.SETTLEMENT_BUST_DATE_CST) IS NULL
                     WHERE T.IS_CURRENT_RECORD_FLAG = TRUE
                       AND (T.INCLUDE_IN_PROGRAM_FLAG = TRUE OR
                            (T.CONDITIONAL_DEBT_STATUS IN ('Pending') AND T.TRADELINE_SETTLEMENT_STATUS = 'ENROLLED'))
                       AND O.OFFER_ID IS NULL
                     GROUP BY 1
                     )
   , TL_LIST AS (
                SELECT TL.TRADELINE_NAME
                     , TL.PROGRAM_NAME
                     , coalesce(C_ORIG.CREDITOR_NAME, CA_ORIG.CREDITOR_ALIAS_NAME) AS ORIGINAL_CREDITOR
                     , coalesce(TL.COLLECTION_AGENCY_PARENT_NAME, TL.COLLECTION_AGENCY_NAME, C_CURR.CREDITOR_NAME,
                                CA_CURR.CREDITOR_ALIAS_NAME, C_ORIG.CREDITOR_NAME,
                                CA_ORIG.CREDITOR_ALIAS_NAME) AS CURRENT_CREDITOR
                     , cast(LLTB.AMOUNT_C AS DECIMAL(18, 2)) AS LATEST_TL_BALANCE_AMOUNT
                     , CASE
                           WHEN TL.ORIGINAL_SOURCE_SYSTEM IN ('LEGACY') THEN CASE
                                                                                 WHEN TL.NEGOTIATION_BALANCE = 0
                                                                                     THEN NULL
                                                                                 ELSE cast(TL.NEGOTIATION_BALANCE AS DECIMAL(18, 2))
                                                                                 END
                           WHEN TL.NEGOTIATION_BALANCE IS NULL AND LATEST_TL_BALANCE_AMOUNT > TL.ENROLLED_BALANCE
                               THEN LATEST_TL_BALANCE_AMOUNT
                           WHEN TL.NEGOTIATION_BALANCE > 0 AND
                                nvl(TL.NEGOTIATION_BALANCE, 0) <= nvl(LATEST_TL_BALANCE_AMOUNT, 0)
                               THEN LATEST_TL_BALANCE_AMOUNT
                           WHEN TL.NEGOTIATION_BALANCE > 0 AND
                                nvl(TL.NEGOTIATION_BALANCE, 0) >= nvl(LATEST_TL_BALANCE_AMOUNT, 0)
                               THEN cast(TL.NEGOTIATION_BALANCE AS DECIMAL(18, 2))
                           ELSE NULL
                           END AS NEGOTIATION_BALANCE
                     , cast(TL.ENROLLED_BALANCE AS DECIMAL(18, 2)) AS ORIGINAL_BALANCE
                     , SSP.AMT AS CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
                     , SCP.AMT AS COMPLETED_PAYMENTS
                     , cast(P.SETTLEMENT_FEE_PERCENTAGE * coalesce(TL.FEE_BASIS_BALANCE, TL.ENROLLED_BALANCE) /
                            100 AS DECIMAL(18, 2)) AS ESTIMATED_FEES
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
                     LEFT JOIN CURATED_PROD.CRM.CREDITOR_ALIAS CA_ORIG
                               ON TL.ORIGINAL_CREDITOR_ALIAS_ID = CA_ORIG.CREDITOR_ALIAS_ID
                                   AND CA_ORIG.IS_CURRENT_RECORD_FLAG
                     LEFT JOIN CURATED_PROD.CRM.CREDITOR C_CURR ON TL.CURRENT_CREDITOR_ID = C_CURR.CREDITOR_ID
                    AND C_CURR.IS_CURRENT_RECORD_FLAG
                     LEFT JOIN CURATED_PROD.CRM.CREDITOR_ALIAS CA_CURR
                               ON TL.CURRENT_CREDITOR_ALIAS_ID = CA_CURR.CREDITOR_ALIAS_ID
                                   AND CA_CURR.IS_CURRENT_RECORD_FLAG
                     LEFT JOIN CURATED_PROD.CRM.PROGRAM P
                               ON P.PROGRAM_ID = TL.PROGRAM_ID AND P.IS_CURRENT_RECORD_FLAG = TRUE
                     LEFT JOIN REFINED_PROD.BEDROCK.PROGRAM_TRADELINE_C AS TLR
                               ON TL.TRADELINE_ID = TLR.ID AND TLR.IS_DELETED = FALSE
                     LEFT JOIN (
                               SELECT ID
                                    , PROGRAM_TRADELINE_ID_C
                                    , AMOUNT_C
                                    , coalesce(BALANCE_AS_OF_DATE_TIME_C_CST, CREATED_DATE_CST) AS EFF_DATE
                                    , row_number()
                                       OVER (PARTITION BY PROGRAM_TRADELINE_ID_C ORDER BY EFF_DATE DESC ,AMOUNT_C DESC) AS RN
                               FROM REFINED_PROD.BEDROCK.TRADELINE_BALANCE_C
                               WHERE AMOUNT_C IS NOT NULL
                                   QUALIFY RN = 1
                               ) AS LTB ON LTB.PROGRAM_TRADELINE_ID_C = TLR.ID
                     LEFT JOIN REFINED_PROD.BEDROCK.TRADELINE_BALANCE_C AS LLTB
                               ON LLTB.ID = TLR.LATEST_TRADELINE_BALANCE_ID_C
                     LEFT JOIN SETTLEMENT_SCHEDULE SSP
                               ON OFFER.OFFER_ID = SSP.OFFER_ID AND SSP.TRANSACTION_STATUS = 'Scheduled' AND
                                  SSP.TRANSACTION_TYPE = 'Payment'
                     LEFT JOIN SETTLEMENT_SCHEDULE SSF
                               ON OFFER.OFFER_ID = SSF.OFFER_ID AND SSF.TRANSACTION_STATUS = 'Scheduled' AND
                                  SSF.TRANSACTION_TYPE = 'Settlement Fee'
                     LEFT JOIN SETTLEMENT_SCHEDULE SCP
                               ON OFFER.OFFER_ID = SCP.OFFER_ID AND SCP.TRANSACTION_STATUS = 'Completed' AND
                                  SCP.TRANSACTION_TYPE = 'Payment'
                     LEFT JOIN SETTLEMENT_SCHEDULE SCF
                               ON OFFER.OFFER_ID = SCF.OFFER_ID AND SCF.TRANSACTION_STATUS = 'Completed' AND
                                  SCF.TRANSACTION_TYPE = 'Settlement Fee'
                WHERE TL.IS_CURRENT_RECORD_FLAG = TRUE
                  AND NOT (TL.TRADELINE_SETTLEMENT_STATUS = 'SETTLED' AND
                           TL.TRADELINE_SETTLEMENT_SUB_STATUS = 'PAID OFF')
                  AND TL.SOURCE_SYSTEM = 'BEDROCK'
                )
   , PROGRAM_NO_CRED AS (
                        SELECT DISTINCT
                               PROGRAM_NAME
                             , ORIGINAL_CREDITOR
                             , CURRENT_CREDITOR
                        FROM TL_LIST
                        WHERE 1 = 1
                          AND PROGRAM_NAME IS NOT NULL
                          AND ORIGINAL_CREDITOR IS NULL
                          AND CURRENT_CREDITOR IS NULL
                        )
   , HISTORICAL_SETTLEMENT_PERCENT AS (
                                      SELECT PROGRAM_NAME
                                           , sum(coalesce(TL_LIST.NEGOTIATION_BALANCE, TL_LIST.FEE_BASIS_BALANCE,
                                                          TL_LIST.ORIGINAL_BALANCE)) AS SETTLED_BALANCE_TOTAL
                                           , sum(TL_LIST.SETTLEMENT_AMOUNT) AS SETTLEMENT_AMOUNT_TOTAL
                                           , CASE
                                                 WHEN SETTLED_BALANCE_TOTAL > 0
                                                     THEN (SETTLEMENT_AMOUNT_TOTAL / SETTLED_BALANCE_TOTAL)
                                                 ELSE NULL
                                                 END AS HISTORICAL_SETTLEMENT_PERCENT
                                      FROM TL_LIST
                                      WHERE TRADELINE_SETTLEMENT_STATUS = 'SETTLED'
                                        AND TRADELINE_SETTLEMENT_SUB_STATUS NOT ILIKE 'BUSTED%'
                                      GROUP BY 1
                                      )
   , DEFERRAL AS (
                 SELECT T.*
                      , ((datediff(MONTH, MIN_DT, MAX_DT) + 1) - CT) AS DEFERRAL_CT
                 FROM (
--          select
--            program.id program_id,
--            min(last_day(payment.NU_DSE_SCHEDULE_DATE_C)) min_dt,
--            max(last_day(payment.NU_DSE_SCHEDULE_DATE_C)) max_dt,
--            count(distinct last_day(payment.NU_DSE_SCHEDULE_DATE_C)) ct
--          from REFINED_PROD.SALESFORCE.NU_DSE_PROGRAM_C program
--          join REFINED_PROD.SALESFORCE.NU_DSE_PAYMENT_C payment on payment.NU_DSE_PROGRAM_C = program.id
--          where payment.NU_DSE_PAYMENT_TYPE_C in ('Deposit', 'Draft')
--          and payment.NU_DSE_TRANSACTION_STATUS_C = 'Completed'
--          group by program.id
                      SELECT PROGRAM_ID
                           , PROGRAM_NAME
                           , min(last_day(SCHEDULED_DATE_CST)) AS MIN_DT
                           , max(last_day(SCHEDULED_DATE_CST)) AS MAX_DT
                           , count(DISTINCT last_day(SCHEDULED_DATE_CST)) AS CT
                      FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION S
                      WHERE S.TRANSACTION_TYPE IN ('Deposit', 'Draft')
                        AND S.TRANSACTION_STATUS = 'Completed' AND S.IS_CURRENT_RECORD_FLAG = TRUE
                      GROUP BY PROGRAM_ID, PROGRAM_NAME

                      ) T
                 )
   , PAYMENTS AS (
                 SELECT *
                 FROM (
                      SELECT PROGRAM.PROGRAM_ID AS PROGRAM_ID
                           , PROGRAM.PROGRAM_NAME
                           , PROGRAM.PAYMENT_FREQUENCY
                           , PAYMENT.SCHEDULED_DATE_CST AS PAYMENT_DATE
                           , row_number() OVER (PARTITION BY PAYMENT.PROGRAM_ID ORDER BY PAYMENT_DATE DESC) AS SEQ
                           , count(PAYMENT.SCHEDULED_DATE_CST)
                                   OVER (PARTITION BY PAYMENT.PROGRAM_ID, last_day(PAYMENT.SCHEDULED_DATE_CST)
                                       ORDER BY PAYMENT.SCHEDULED_DATE_CST DESC) AS PAYMENTS_COUNT
                      FROM CURATED_PROD.CRM.PROGRAM PROGRAM
                           JOIN CURATED_PROD.CRM.SCHEDULED_TRANSACTION PAYMENT
                                ON PAYMENT.PROGRAM_NAME = PROGRAM.PROGRAM_NAME AND PAYMENT.IS_CURRENT_RECORD_FLAG = TRUE
                      WHERE PAYMENT.TRANSACTION_TYPE IN ('Deposit', 'Draft')
                        AND PAYMENT.TRANSACTION_STATUS = 'Completed' AND PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
                      )
                 WHERE SEQ > CASE WHEN PAYMENT_FREQUENCY = 'Monthly' THEN 6 ELSE 12 END
                 ORDER BY PROGRAM_ID
                 )
   , RECENT_PAYMENTS AS (
                        SELECT *
                        FROM (
                             SELECT PROGRAM.PROGRAM_ID
                                  , PROGRAM.PROGRAM_NAME
                                  , PROGRAM.PAYMENT_FREQUENCY
                                  , PAYMENT.SCHEDULED_DATE_CST AS PAYMENT_DATE
                                  , datediff(MONTH, PAYMENT.SCHEDULED_DATE_CST, current_date)
                                  , row_number()
                                     OVER (PARTITION BY PROGRAM.PROGRAM_ID ORDER BY PAYMENT_DATE DESC) AS SEQ
                                  , count(PAYMENT.SCHEDULED_DATE_CST)
                                          OVER (PARTITION BY PROGRAM.PROGRAM_ID, last_day(PAYMENT.SCHEDULED_DATE_CST)
                                              ORDER BY PAYMENT.SCHEDULED_DATE_CST DESC) AS PAYMENTS_COUNT
                             FROM CURATED_PROD.CRM.PROGRAM PROGRAM
                                  JOIN CURATED_PROD.CRM.SCHEDULED_TRANSACTION PAYMENT
                                       ON PAYMENT.PROGRAM_NAME = PROGRAM.PROGRAM_NAME AND
                                          PAYMENT.IS_CURRENT_RECORD_FLAG = TRUE
                             WHERE PAYMENT.TRANSACTION_TYPE IN ('Deposit', 'Draft')
                               AND PAYMENT.TRANSACTION_STATUS IN ('Completed', 'In Progress', 'In Process')
                               AND PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
                             )
                        WHERE SEQ = CASE WHEN PAYMENT_FREQUENCY = 'Monthly' THEN 3 ELSE 6 END
                          AND datediff(MONTH, PAYMENT_DATE, current_date) < 4
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
                                     , row_number()
                                        OVER (PARTITION BY PROGRAM.PROGRAM_NAME ORDER BY PAYMENT.SCHEDULED_DATE_CST DESC) AS SEQ
                                FROM CURATED_PROD.CRM.PROGRAM PROGRAM
                                     JOIN CURATED_PROD.CRM.SCHEDULED_TRANSACTION PAYMENT
                                          ON PAYMENT.PROGRAM_NAME = PROGRAM.PROGRAM_NAME AND
                                             PAYMENT.IS_CURRENT_RECORD_FLAG = TRUE
                                WHERE PAYMENT.TRANSACTION_TYPE IN ('Deposit', 'Draft')
                                  AND PAYMENT.TRANSACTION_STATUS = 'Completed' AND PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
                                )
                           WHERE 1 = 1
                             AND SEQ <= CASE WHEN PAYMENT_FREQUENCY = 'Monthly' THEN 1 ELSE 2 END
                           ) TT
                               PIVOT (max(SCHEDULED_DATE_CST) FOR SEQ IN (1,2)) AS P (PROGRAM_ID, PROGRAM_NAME,
                                                                                      PAYMENT_FREQUENCY, PAYMENT_DATE1,
                                                                                      PAYMENT_DATE2)
                      )
   , SCHEDULE_ADHERENCE AS (
                           SELECT *
                                , CASE
                                      WHEN TOTAL_PAYMENTS <> 0
                                          THEN cast(SUCCESSFUL_PAYMENTS AS FLOAT) / (TOTAL_PAYMENTS)
                                      END AS SCHEDULE_ADHERENCE
                           FROM (
                                SELECT DISTINCT
                                       PROGRAM.PROGRAM_ID
                                     , PROGRAM.PROGRAM_NAME
                                     , sum(CASE
                                               WHEN PAYMENT.SCHEDULED_DATE_CST < current_date AND
                                                    TRANSACTION_STATUS NOT IN
                                                    ('Failed', 'Cancelled', 'Returned', 'Scheduled')
                                                   THEN 1
                                               ELSE 0
                                               END) OVER (PARTITION BY PROGRAM.PROGRAM_ID) AS SUCCESSFUL_PAYMENTS
                                     , sum(CASE
                                               WHEN PAYMENT.SCHEDULED_DATE_CST < current_date AND
                                                    TRANSACTION_STATUS IN ('Failed', 'Returned', 'Cancelled')
                                                   THEN 1
                                               ELSE 0
                                               END) OVER (PARTITION BY PROGRAM.PROGRAM_ID) AS INCOMPLETE_PAYMENTS
                                     , sum(CASE
                                               WHEN PAYMENT.SCHEDULED_DATE_CST < current_date
                                                   THEN 1
                                               ELSE NULL
                                               END) OVER (PARTITION BY PROGRAM.PROGRAM_ID) AS TOTAL_PAYMENTS
                                FROM CURATED_PROD.CRM.PROGRAM PROGRAM
                                     JOIN CURATED_PROD.CRM.SCHEDULED_TRANSACTION PAYMENT
                                          ON PAYMENT.PROGRAM_NAME = PROGRAM.PROGRAM_NAME AND
                                             PAYMENT.IS_CURRENT_RECORD_FLAG = TRUE
                                WHERE PAYMENT.TRANSACTION_TYPE IN ('Deposit', 'Draft')
                                  AND PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
                                ) A
                           )
   , CREDITOR_SETTLEMENTS AS (
                             SELECT ORIGINAL_CREDITOR
                                  , CURRENT_CREDITOR
                                  , sum(SETTLEMENT_AMOUNT) AS "Settlement Amount"
                                  , sum(TRADELINE_ORIGINAL_AMOUNT) AS "Tradeline Original Amount"
                                  , avg(SETTLEMENT_PCT_ORIGINAL) AS SETTLEMENT_PCT_ORIGINAL
                                  , round(avg(SETTLEMENT_AMOUNT)) AS AVERAGE_SETTLEMENT_AMT
                                  , count(TRADELINE_NAME) AS TRADELINE_COUNT
                                  , avg(SETTLEMENT_PCT_ORIGINAL) AS AVERAGE_SETTLEMENT_PCT
                                  , percentile_cont(0.25) WITHIN GROUP (ORDER BY SETTLEMENT_PCT_ORIGINAL) AS BOTTOM_25_PERCENT_SETTLEMENT_PCT
                                  , percentile_cont(0.50) WITHIN GROUP (ORDER BY SETTLEMENT_PCT_ORIGINAL) AS MIDDLE_50_PERCENT_SETTLEMENT_PCT
                                  , percentile_cont(0.75) WITHIN GROUP (ORDER BY SETTLEMENT_PCT_ORIGINAL) AS TOP_25_PERCENT_SETTLEMENT_PCT
                                  , percentile_cont(0.9) WITHIN GROUP (ORDER BY SETTLEMENT_PCT_ORIGINAL) AS TOP_90_PERCENT_SETTLEMENT_PCT
                             FROM (
                                  SELECT DISTINCT --ifnull(t.original_creditor_parent_name,t.original_creditor_name) original_creditor
                                         coalesce(C_ORIG.CREDITOR_NAME, CA_ORIG.CREDITOR_ALIAS_NAME) AS ORIGINAL_CREDITOR
                                         -- , coalesce(t.collection_agency_parent_name,t.collection_agency_name, t.current_creditor_parent_name,t.current_creditor_name,t.original_creditor_parent_name,t.original_creditor_name) current_creditor
                                       , coalesce(T.COLLECTION_AGENCY_PARENT_NAME, T.COLLECTION_AGENCY_NAME,
                                                  C_CURR.CREDITOR_NAME, CA_CURR.CREDITOR_ALIAS_NAME,
                                                  C_ORIG.CREDITOR_NAME, CA_ORIG.CREDITOR_ALIAS_NAME) AS CURRENT_CREDITOR
                                       , O.TRADELINE_ORIGINAL_AMOUNT
                                       , O.SETTLEMENT_AMOUNT
                                       , CASE
                                             WHEN coalesce(T.NEGOTIATION_BALANCE, T.FEE_BASIS_BALANCE,
                                                           T.ENROLLED_BALANCE) <> 0 THEN (
                                                     cast(O.SETTLEMENT_AMOUNT AS FLOAT) /
                                                     coalesce(T.NEGOTIATION_BALANCE, T.FEE_BASIS_BALANCE,
                                                              T.ENROLLED_BALANCE))
                                             END AS SETTLEMENT_PCT_ORIGINAL
                                       , CASE
                                             WHEN O.TRADELINE_ORIGINAL_AMOUNT <> 0
                                                 THEN (cast(O.SETTLEMENT_SERVICE_FEE AS FLOAT) / O.TRADELINE_ORIGINAL_AMOUNT)
                                             END AS SETTLEMENT_FEE_PCT_ORIGINAL
                                       , O.SETTLEMENT_SERVICE_FEE
                                       , O.TRADELINE_NAME
                                       , O.OFFER_ACCEPTED_DATE_CST
                                  FROM CURATED_PROD.CRM.OFFER O
                                       LEFT JOIN CURATED_PROD.CRM.TRADELINE T ON O.TRADELINE_NAME = T.TRADELINE_NAME
                                       LEFT JOIN CURATED_PROD.CRM.CREDITOR C_ORIG
                                                 ON T.ORIGINAL_CREDITOR_ID = C_ORIG.CREDITOR_ID
                                                     AND C_ORIG.IS_CURRENT_RECORD_FLAG
                                       LEFT JOIN CURATED_PROD.CRM.CREDITOR_ALIAS CA_ORIG
                                                 ON T.ORIGINAL_CREDITOR_ALIAS_ID = CA_ORIG.CREDITOR_ALIAS_ID
                                                     AND CA_ORIG.IS_CURRENT_RECORD_FLAG
                                       LEFT JOIN CURATED_PROD.CRM.CREDITOR C_CURR
                                                 ON T.CURRENT_CREDITOR_ID = C_CURR.CREDITOR_ID
                                                     AND C_CURR.IS_CURRENT_RECORD_FLAG
                                       LEFT JOIN CURATED_PROD.CRM.CREDITOR_ALIAS CA_CURR
                                                 ON T.CURRENT_CREDITOR_ALIAS_ID = CA_CURR.CREDITOR_ALIAS_ID
                                                     AND CA_CURR.IS_CURRENT_RECORD_FLAG
                                  WHERE datediff(DAY, O.OFFER_ACCEPTED_DATE_CST, current_date) <= 90
                                    AND O.IS_CURRENT_RECORD_FLAG = 'TRUE'
                                    AND O.IS_CURRENT_OFFER = 'TRUE'
                                    AND T.IS_CURRENT_RECORD_FLAG = 'TRUE'
                                    AND O.CFT_PAYMENT_SCHEDULED_CREATED_DATE_CST IS NOT NULL
                                  ) A
                             WHERE SETTLEMENT_PCT_ORIGINAL < 1
                               AND SETTLEMENT_PCT_ORIGINAL > 0.05
                             GROUP BY ORIGINAL_CREDITOR
                                    , CURRENT_CREDITOR
                             )
    /*fee_template as (
        select
            distinct NU_DSE_PROGRAM_C.ID program_id
            , NU_DSE_PROGRAM_C.NAME
            , nu_dse_settlement_fee_percentage_c settlement_fee_pct
        from  REFINED_PROD.SALESFORCE.NU_DSE_PROGRAM_C
        inner join REFINED_PROD.SALESFORCE.NU_DSE_FEE_TEMPLATE_C_VW on NU_DSE_PROGRAM_C.NU_DSE_FEE_TEMPLATE_C = NU_DSE_FEE_TEMPLATE_C_VW.ID
        where NU_DSE_PROGRAM_C.is_deleted_flag = FALSE
        and NU_DSE_FEE_TEMPLATE_C_VW.is_deleted_flag = FALSE
     ),*/
   , TERMINATION_REQUESTED AS (
                              SELECT DISTINCT coalesce(SALESFORCE_PROGRAM_ID, NU_DSE_PROGRAM_C_ID) AS PROGRAM_ID
                              FROM REFINED_PROD.FIVE9.CALL
                              WHERE DISPOSITION ILIKE '%term%'
                              )
   , DNC AS (
            SELECT DISTINCT cast(DNC_NUMBER AS NVARCHAR) AS DNC_NUMBER
            FROM REFINED_PROD.FIVE9.DNC
            )
   , PRIOR_LOAN_APPLICANT AS (
                             SELECT DISTINCT
                                    P.PROGRAM_ID_C
                                  , P.LENDER_STATUS_C
                                  , P.LOAN_APPLICATION_INTEREST_C
                                  , P.LOAN_APPLICATION_STATUS_C
                                  , P.CREATED_DATE_CST
                                  , P.LAST_MODIFIED_DATE_CST
                                  , DSMR.CURRENT_STATUS
                                  , DSMR.APP_SUBMIT_DATE
                             FROM REFINED_PROD.BEDROCK.PROGRAM_LOAN_C AS P
                                  LEFT JOIN CURATED_PROD.CRM.PROGRAM CP
                                            ON P.PROGRAM_ID_C = CP.PROGRAM_ID AND CP.IS_CURRENT_RECORD_FLAG = TRUE
                                  LEFT JOIN (
                                            SELECT *
                                            FROM REFINED_PROD.ABOVE_LENDING.AGL_COMBINED_DETAIL
                                                QUALIFY rank() OVER (PARTITION BY PROGRAM_NAME ORDER BY APP_SUBMIT_DATE DESC) = 1
                                            ) DSMR ON DSMR.PROGRAM_NAME = CP.PROGRAM_NAME

                             WHERE P.IS_DELETED = FALSE
                               AND (DSMR.CURRENT_STATUS IN ('ONBOARDED')
                                 OR (DSMR.CURRENT_STATUS IN ('BACK_END_DECLINED') AND
                                     datediff(DAY, DSMR.APP_SUBMIT_DATE, current_date) <= 90)
                                 OR (DSMR.CURRENT_STATUS IN ('FRONT_END_DECLINED') AND
                                     datediff(DAY, DSMR.APP_SUBMIT_DATE, current_date) <= 90)
                                 )
                             )
   , CFT_MONTHLY_FEES AS (
                         SELECT P.PROGRAM_ID AS PROGRAM_ID
                              , P.PROGRAM_NAME
                              , SUM(ACT.TRANSACTION_AMOUNT) AS CFT_MONTHLY_FEES
                         FROM CURATED_PROD.CRM.PROGRAM P
                              LEFT JOIN CURATED_PROD.CFT.ACTUAL_TRANSACTION ACT
                                        ON ACT.PROGRAM_NAME = P.PROGRAM_NAME AND ACT.IS_CURRENT_RECORD_FLAG = TRUE
                         WHERE ACT.TRANSACTION_STATUS = 'Completed'
                           AND ACT.TRANSACTION_GROUP = 'Fee'
                           AND ACT.TRANSACTION_TYPE = 'Monthly Service Fees'
                           AND datediff(MONTH, TRANSACTION_DATE_CST, current_date) = 1
                           AND P.IS_CURRENT_RECORD_FLAG = TRUE
                         GROUP BY P.PROGRAM_ID, P.PROGRAM_NAME
                         )
   , BLP_MONTHLY_FEE AS (
                        SELECT P.PROGRAM_ID AS PROGRAM_ID
                             , P.PROGRAM_NAME
                             , SUM(ACT.TRANSACTION_AMOUNT) AS CFT_MONTHLY_FEES
                        FROM CURATED_PROD.CRM.PROGRAM P
                             LEFT JOIN CURATED_PROD.CFT.ACTUAL_TRANSACTION ACT
                                       ON ACT.PROGRAM_NAME = P.PROGRAM_NAME AND ACT.IS_CURRENT_RECORD_FLAG = TRUE
                        WHERE ACT.TRANSACTION_STATUS = 'Completed'
                          AND ACT.TRANSACTION_GROUP = 'Fee'
                          AND ACT.TRANSACTION_TYPE = 'Monthly Legal Service Fee'
                          AND datediff(MONTH, TRANSACTION_DATE_CST, current_date) = 1
                          AND P.IS_CURRENT_RECORD_FLAG = TRUE
                        GROUP BY P.PROGRAM_ID, P.PROGRAM_NAME
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
                            WHERE AS_OF_DATE_CST = current_date AND P.IS_CURRENT_RECORD_FLAG = TRUE
                            )
   , CFT_PRIOR_MONTH_PAYMENT AS (
                                SELECT P.PROGRAM_ID AS PROGRAM_ID
                                     , P.PROGRAM_NAME
                                     , SUM(ACT.TRANSACTION_AMOUNT) AS CFT_MONTHLY_FEES
                                FROM CURATED_PROD.CRM.PROGRAM P
                                     LEFT JOIN CURATED_PROD.CFT.ACTUAL_TRANSACTION ACT
                                               ON ACT.PROGRAM_NAME = P.PROGRAM_NAME AND ACT.IS_CURRENT_RECORD_FLAG = TRUE
                                WHERE ACT.TRANSACTION_STATUS = 'Completed'
                                  AND ACT.TRANSACTION_GROUP = 'Deposit'
                                  AND ACT.TRANSACTION_TYPE = 'Deposit'
                                  AND datediff(MONTH, TRANSACTION_DATE_CST, current_date) = 1
                                  AND P.IS_CURRENT_RECORD_FLAG = TRUE
                                GROUP BY P.PROGRAM_ID, P.PROGRAM_NAME
                                )
   , FICO_SCORE AS (
                   SELECT *
                   FROM (
                        SELECT DISTINCT
                               PROGRAM.PROGRAM_ID AS PROGRAM_ID
                             , PROGRAM.PROGRAM_NAME
                             , C.CREDIT_SCORE AS CREDIT_SCORE
                             , C.CREDIT_SCORE_DATE_CST
                             , row_number()
                                       OVER (PARTITION BY PROGRAM.PROGRAM_NAME ORDER BY C.CREDIT_SCORE_DATE_CST ASC) AS RANK
                        FROM CURATED_PROD.CRM.PROGRAM PROGRAM
                             LEFT JOIN CURATED_PROD.CRM.CLIENT C
                                       ON C.CLIENT_ID = PROGRAM.CLIENT_ID
                        WHERE 1 = 1
                          AND C.CREDIT_SCORE IS NOT NULL
                          AND PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
                        )
                   WHERE RANK = 1
                   )
   , NEW_OR_AGED_BOOK AS (
                         SELECT DISTINCT
                                PROGRAM.PROGRAM_ID AS PROGRAM_ID
                              , PROGRAM.PROGRAM_NAME
                              , CASE
                                    WHEN datediff(MONTH, PROGRAM.ENROLLED_DATE_CST, current_date) >= 6 THEN 'Aged Book'
                                    WHEN datediff(MONTH, PROGRAM.ENROLLED_DATE_CST, current_date) < 6 THEN 'New Book'
                                    END AS PROGRAM_AGE_BUCKET
                              , PROGRAM.ENROLLED_DATE_CST
                         FROM CURATED_PROD.CRM.PROGRAM PROGRAM
                         WHERE PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
                         )
   , BEYOND_FEES AS (
                    SELECT DISTINCT
                           PROGRAM_ID
                         , sum(CASE
                                   WHEN TRANSACTION_STATUS = 'Scheduled' THEN TOTAL_AMOUNT * -1
                                   ELSE 0
                                   END) AS REMAINING_BEYOND_FEES
                    FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION
                    WHERE TRANSACTION_TYPE = 'Fee Withdrawal'
                      AND IS_CURRENT_RECORD_FLAG = 'TRUE'
                    GROUP BY PROGRAM_ID
                    )
   , FEES_OUTSTANDING AS (
                         SELECT PROGRAM_ID
                              , sum(TOTAL_AMOUNT * -1) AS FEES_OUTSTANDING
                         FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION
                         WHERE IS_CURRENT_RECORD_FLAG = 'TRUE'
                           AND TRANSACTION_STATUS = 'Scheduled'
                           AND TRANSACTION_TYPE = 'Fee'
                         GROUP BY PROGRAM_ID
                         )
   , CCPA_PHONE AS (
                   SELECT DISTINCT
                          REPLACE(regexp_replace(CONTACT.PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') AS CCPA_PHONE
                   FROM REFINED_PROD.SALESFORCE.COMPLIANCE_REQUEST_C_VW COMPLIANCE
                        LEFT JOIN REFINED_PROD.SALESFORCE.CONTACT ON COMPLIANCE.CONTACT_C = CONTACT.ID
                       AND CONTACT.PHONE IS NOT NULL
                   UNION
                   SELECT REPLACE(regexp_replace(CONTACT.MOBILE_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ',
                                  '') AS CCPA_PHONE
                   FROM REFINED_PROD.SALESFORCE.COMPLIANCE_REQUEST_C_VW COMPLIANCE
                        LEFT JOIN REFINED_PROD.SALESFORCE.CONTACT ON COMPLIANCE.CONTACT_C = CONTACT.ID
                       AND CONTACT.MOBILE_PHONE IS NOT NULL
                   UNION
                   SELECT REPLACE(regexp_replace(CONTACT.HOME_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ',
                                  '') AS CCPA_PHONE
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
                   --FROM REFINED_PROD.MAROPOST.UNSUBSCRIBES U
                   --LEFT JOIN REFINED_PROD.MAROPOST.CONTACTS  C on U.CONTACT_ID=C.CONTACT_ID
                   )
   , FULL_PAYMENTS AS (
                      SELECT PROGRAM_ID
                           , PROGRAM_NAME
                           , count(*)
                      FROM (
                           SELECT *
                                , row_number() OVER (PARTITION BY PROGRAM_ID ORDER BY PAY_MONTH DESC) AS SEQNUM1
                           FROM (
                                SELECT DISTINCT
                                       SCHEDULED_TRANSACTION.PROGRAM_ID
                                     , SCHEDULED_TRANSACTION.PROGRAM_NAME
                                     , last_day(SCHEDULED_TRANSACTION.SCHEDULED_DATE_CST) AS PAY_MONTH
                                     , PROGRAM.MONTHLY_PRINCIPAL_AMOUNT_INCLUDING_FEES AS PLANNED_DEPOSITS
                                     , sum(CASE
                                               WHEN SCHEDULED_TRANSACTION.TRANSACTION_STATUS IN ('Completed', 'Cleared')
                                                   THEN SCHEDULED_TRANSACTION.TOTAL_AMOUNT
                                               ELSE 0
                                               END) AS COMPLETED_DEPOSITS
                                FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION
                                     INNER JOIN CURATED_PROD.CRM.PROGRAM
                                                ON SCHEDULED_TRANSACTION.PROGRAM_ID = PROGRAM.PROGRAM_ID
                                                    AND SCHEDULED_TRANSACTION.IS_CURRENT_RECORD_FLAG = TRUE
                                                    AND PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
                                WHERE SCHEDULED_TRANSACTION.TRANSACTION_TYPE = 'Deposit'
                                  AND SCHEDULED_TRANSACTION.TRANSACTION_STATUS IN ('Completed', 'Cleared')
                                  AND last_day(SCHEDULED_TRANSACTION.SCHEDULED_DATE_CST) < last_day(current_date())
                                GROUP BY SCHEDULED_TRANSACTION.PROGRAM_ID
                                       , SCHEDULED_TRANSACTION.PROGRAM_NAME
                                       , last_day(SCHEDULED_TRANSACTION.SCHEDULED_DATE_CST)
                                       , PROGRAM.MONTHLY_PRINCIPAL_AMOUNT_INCLUDING_FEES
                                HAVING COMPLETED_DEPOSITS > 0
                                ) TT
                           WHERE COMPLETED_DEPOSITS >= PLANNED_DEPOSITS
                               QUALIFY SEQNUM1 <= 3
                           ) TTT
                      WHERE datediff(MONTH, PAY_MONTH, current_date) <= 3
                      GROUP BY PROGRAM_ID
                             , PROGRAM_NAME
                      HAVING count(*) = 3
                      )
   , DEPOSIT_ADHERENCE AS (
                          WITH DATES AS (
                                        SELECT 90 AS TERM
                                             , DATEADD('day', -90, CURRENT_DATE()) AS STARTDATE
                                             , CURRENT_DATE() AS ENDDATE
                                        UNION ALL
                                        SELECT 180 AS TERM
                                             , DATEADD('day', -180, CURRENT_DATE()) AS STARTDATE
                                             , CURRENT_DATE() AS ENDDATE
                                        )
                             , PRG AS (
                                      SELECT P.PROGRAM_NAME
                                           , Last_Day(P.ENROLLED_DATE_CST) AS VINTAGE
                                           , P.ENROLLED_DATE_CST
                                           , P.PROGRAM_STATUS
                                           , E.EFFECTIVE_DATE
                                      FROM CURATED_PROD.CRM.PROGRAM P
                                           LEFT JOIN (
                                                     SELECT S.PROGRAM_NAME
                                                          , min(S.RECORD_EFFECTIVE_START_DATE_TIME_CST) AS EFFECTIVE_DATE
                                                     FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION S
                                                          LEFT JOIN (
                                                                    SELECT PROGRAM_NAME, ENROLLED_DATE_CST
                                                                    FROM CURATED_PROD.CRM.PROGRAM
                                                                    WHERE IS_CURRENT_RECORD_FLAG = TRUE
                                                                    ) P
                                                                    ON S.PROGRAM_NAME = P.PROGRAM_NAME
                                                     WHERE TRANSACTION_TYPE = 'Deposit'
                                                       AND SCHEDULED_DATE_CST IS NOT NULL
                                                       AND TRANSACTION_NUMBER IS NOT NULL
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
                                                      , row_number()
                                                         OVER (PARTITION BY TRANSACTION_NUMBER,DATES.TERM ORDER BY RECORD_EFFECTIVE_START_DATE_TIME_CST DESC) AS RNK
                                                 FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION S
                                                      INNER JOIN PRG P ON S.PROGRAM_NAME = P.PROGRAM_NAME
                                                      CROSS JOIN DATES
                                                 WHERE 1 = 1
                                                   AND S.RECORD_EFFECTIVE_START_DATE_TIME_CST <=
                                                       greatest(DATES.STARTDATE, P.EFFECTIVE_DATE)
                                                     QUALIFY RNK = 1
                                                 )
                                            WHERE TRANSACTION_STATUS IN
                                                  ('Scheduled', 'In Progress', 'Pending', 'Tentative', 'Completed',
                                                   'Failed', 'Processing Failed', 'Returned', 'In_Transit', 'Suspended')
                                              AND TRANSACTION_TYPE = 'Deposit' AND SCHEDULED_DATE_CST >= STARTDATE
                                              AND SCHEDULED_DATE_CST <= ENDDATE
                                            GROUP BY TERM, PROGRAM_NAME, TRANSACTION_TYPE, ENROLLED_DATE_CST, STARTDATE
                                                   , ENDDATE
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
                                                    LEFT JOIN CURATED_PROD.CRM.SCHEDULED_TRANSACTION S
                                                              ON S.TRANSACTION_NUMBER = A.TRANSACTION_NUMBER AND
                                                                 S.IS_CURRENT_RECORD_FLAG = TRUE
                                                    INNER JOIN PRG P ON A.PROGRAM_NAME = P.PROGRAM_NAME
                                               WHERE S.SCHEDULED_DATE_CST >= DATES.STARTDATE
                                                 AND S.SCHEDULED_DATE_CST <= DATES.ENDDATE
                                                 AND A.IS_CURRENT_RECORD_FLAG = TRUE
                                               )
                                          WHERE upper(TRANSACTION_TYPE) IN ('DEPOSIT') AND upper(TRANSACTION_STATUS) IN
                                                                                           ('COMPLETED', 'IN_TRANSIT', 'SCHEDULED', 'IN PROGRESS')
                                          GROUP BY TERM, PROGRAM_NAME, ENROLLED_DATE_CST, STARTDATE, ENDDATE
                                          )
                          SELECT P.PROGRAM_NAME
                               , DATES.TERM
                               , any_value(DATES.STARTDATE)
                               , any_value(DATES.ENDDATE)
                               , any_value(VINTAGE)
                               , nvl(sum(A.ACTUAL_AMOUNT), 0) AS ACTUAL_AMOUNT
                               , sum(S.SCHEDULED_AMOUNT) AS SCHEDULED_AMOUNT
                               , CASE
                                     WHEN sum(S.SCHEDULED_AMOUNT) > 0
                                         THEN nvl(sum(A.ACTUAL_AMOUNT), 0) / sum(S.SCHEDULED_AMOUNT)
                                     END AS DEPADHERENCE
                          FROM PRG P
                               INNER JOIN SCHEDULES S ON P.PROGRAM_NAME = S.PROGRAM_NAME
                               INNER JOIN DATES ON S.TERM = DATES.TERM
                               LEFT JOIN ACTUALS A ON P.PROGRAM_NAME = A.PROGRAM_NAME AND S.STARTDATE = A.STARTDATE AND
                                                      A.TERM = S.TERM AND A.TERM = DATES.TERM
                          WHERE P.PROGRAM_STATUS NOT IN ('Closed', 'Sold to 3rd Party')
                          GROUP BY DATES.TERM, P.PROGRAM_NAME
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
                          , row_number()
                             OVER (PARTITION BY P.NAME ORDER BY BANK_ACCOUNT.LAST_MODIFIED_DATE_CST DESC) AS RNK
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
   , REMAINING_DEPOSITS AS (
                           SELECT T.PROGRAM_NAME
                                , sum(TOTAL_AMOUNT) AS BF_REMAINING_DEPOSITS
                           FROM CURATED_PROD.CRM.SCHEDULED_TRANSACTION T
                                JOIN CURATED_PROD.CRM.PROGRAM P
                                     ON T.PROGRAM_NAME = P.PROGRAM_NAME AND P.IS_CURRENT_RECORD_FLAG
                           WHERE T.IS_CURRENT_RECORD_FLAG
                             AND TRANSACTION_TYPE = 'Deposit'
                             AND SCHEDULED_DATE_CST::DATE > current_date
                             AND TRANSACTION_STATUS IS DISTINCT FROM 'Cancelled'
                           GROUP BY 1
                           )
   , COCLIENTS AS (
                  SELECT P.PROGRAM_NAME, count(COCLIENT.CONTACT_ID) AS CT
                  FROM CURATED_PROD.CRM.PROGRAM P
                       JOIN REFINED_PROD.BEDROCK.PROGRAM_C PC ON PC.NAME = P.PROGRAM_NAME AND PC.IS_DELETED = FALSE
                       LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT ACT ON ACT.ID = PC.ACCOUNT_ID_C AND ACT.IS_DELETED = FALSE
                       LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT_CONTACT_RELATION ACR
                                 ON ACR.ACCOUNT_ID = ACT.ID AND ACR.RELATIONSHIP_C = 'Client' AND ACR.IS_DELETED = FALSE
                       LEFT JOIN REFINED_PROD.BEDROCK.CONTACT CT ON ACR.CONTACT_ID = CT.ID AND CT.IS_DELETED = FALSE
                      --left join REFINED_PROD.BEDROCK.ACCOUNT_CONTACT_RELATION ACR on ACR.CONTACT_ID=CT.ID and ACR.RELATIONSHIP_C='Client' and ACR.IS_DELETED=FALSE
                       LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT_CONTACT_RELATION COCLIENT
                                 ON COCLIENT.ACCOUNT_ID = PC.ACCOUNT_ID_C AND COCLIENT.RELATIONSHIP_C = 'Co-Client' AND
                                    COCLIENT.IS_DELETED = FALSE
                  WHERE IS_CURRENT_RECORD_FLAG = TRUE
                  GROUP BY 1
                  )
   , DQ AS (
           SELECT PROGRAM_NAME, min(DQ_DELTA_BY_DAY75) AS PROGDQB75
           FROM (
                SELECT P.SOURCE_SYSTEM
                     , P.PROGRAM_NAME
                     , P.ENROLLED_DATE_CST
                     , TL.TRADELINE_NAME
                     , coalesce(CASE
                                    WHEN BTL.LAST_PAYMENT_DATE_C < '1990-01-01' THEN CRL.LAST_ACTIVITY_DATE_C
                                    ELSE BTL.LAST_PAYMENT_DATE_C
                                    END, CRL.LAST_ACTIVITY_DATE_C) AS LAST_PAYMENT_DATE
                     , datediff(DAY, coalesce(LAST_PAYMENT_DATE, P.ENROLLED_DATE_CST), CURRENT_DATE) - 30 AS DQ
                     , coalesce(iff(CC.ID = 'a0y3h0000015BqsAAE', 0, CT.DAYS_DELINQUENT_MIN_C), 180) AS DQ_MIN
                     , DQ + 75 - DQ_MIN AS DQ_DELTA_BY_DAY75
                FROM CURATED_PROD.CRM.TRADELINE TL
                     LEFT JOIN CURATED_PROD.CRM.PROGRAM P
                               ON P.PROGRAM_NAME = TL.PROGRAM_NAME AND P.IS_CURRENT_RECORD_FLAG = TRUE
                     LEFT JOIN REFINED_PROD.BEDROCK.PROGRAM_TRADELINE_C AS BTL
                               ON BTL.NAME = TL.TRADELINE_NAME AND BTL.IS_DELETED = 'FALSE'
                     LEFT JOIN REFINED_PROD.BEDROCK.OPPORTUNITY_TRADELINE_C OT
                               ON BTL.OPPORTUNITY_TRADELINE_ID_C = OT.ID AND OT.IS_DELETED = FALSE
                     LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_C CRL
                               ON CRL.ID = OT.CR_LIABILITY_ID_C AND CRL.IS_DELETED = FALSE
                     LEFT JOIN REFINED_PROD.BEDROCK.CREDITOR_TERMS_C CT ON CT.IS_DELETED = FALSE AND CT.CREDITOR_ID_C =
                                                                                                     coalesce(
                                                                                                             BTL.NEGOTIATING_CREDITOR_ID_C,
                                                                                                             BTL.ORIGINATING_CREDITOR_ID_C)
                     LEFT JOIN REFINED_PROD.BEDROCK.CREDITOR_C AS CC
                               ON CC.ID = coalesce(BTL.NEGOTIATING_CREDITOR_ID_C, BTL.ORIGINATING_CREDITOR_ID_C) AND
                                  CC.IS_DELETED = FALSE
                WHERE TL.TRADELINE_SETTLEMENT_STATUS IN ('ENROLLED')
                  AND TL.IS_CURRENT_RECORD_FLAG = TRUE
                  AND P.SOURCE_SYSTEM = 'BEDROCK'
                  AND P.PROGRAM_STATUS IN ('Active', 'New', 'Enrolled')
                    QUALIFY row_number() OVER (PARTITION BY TL.TRADELINE_ID ORDER BY CT.DAYS_DELINQUENT_MIN_C ASC) = 1
                )
           GROUP BY PROGRAM_NAME
           )
   , CREDITOR_MATRIX_SETTLEMENT_LOGIC AS (
                                         --Getting DQ when creditors will first offer settlement
                                         WITH FIRST_ELIGIBLE_INFO AS (

                                                                     SELECT DISTINCT
                                                                            CREDITOR_BUCKET_NAME
                                                                          , ORIGINAL_CREDITOR_ALIAS_ID
                                                                          , min(DAYS_DELINQUENT_MIN) AS DAYS_DELINQUENT_MIN
                                                                          , NEGOTIATION_BALANCE_MIN
                                                                          , NEGOTIATION_BALANCE_MAX
                                                                     FROM REFINED_PROD.CONFIGURATION.CREDITOR_TERMS
                                                                     WHERE IPL_USE_FLAG = TRUE
                                                                     GROUP BY 1, 2, 4, 5
                                                                     ORDER BY 1

                                                                     )
                                            ,

                                            --Grabbing term, offer rate and min payment for intial offers
                                             FIRST_ELIGIBLE_INFO_2 AS (

                                                                      SELECT DISTINCT
                                                                             A.*
                                                                           , coalesce(
                                                                                     CT_ALIAS_LUMP.CURRENT_CREDITOR_BUCKETED,
                                                                                     CT_NO_ALIAS_LUMP.CURRENT_CREDITOR_BUCKETED) AS CURRENT_CREDITOR_BUCKETED
                                                                           , coalesce(CT_ALIAS_LUMP.OFFER_PERCENT,
                                                                                      CT_NO_ALIAS_LUMP.OFFER_PERCENT) AS OFFER_PERCENT_NOT_LEGAL
                                                                           , coalesce(CT_ALIAS_LUMP.OFFER_PERCENT,
                                                                                      CT_NO_ALIAS_LUMP.OFFER_PERCENT) +
                                                                             coalesce(CT_ALIAS_LUMP.LEGAL_RATE_INCREASE,
                                                                                      CT_NO_ALIAS_LUMP.LEGAL_RATE_INCREASE,
                                                                                      0) AS OFFER_PERCENT_IS_LEGAL
                                                                           , coalesce(CT_ALIAS_LUMP.AVG_OFFER_TERM,
                                                                                      CT_NO_ALIAS_LUMP.AVG_OFFER_TERM) AS AVG_OFFER_TERM
                                                                           , coalesce(
                                                                                     CT_ALIAS_LUMP.OFFER_MINIMUM_PAYMENT,
                                                                                     CT_NO_ALIAS_LUMP.OFFER_MINIMUM_PAYMENT) AS OFFER_MINIMUM_PAYMENT
                                                                           , coalesce(
                                                                                     CT_ALIAS_LUMP.CREDITOR_BUCKET_NAME,
                                                                                     CT_NO_ALIAS_LUMP.CREDITOR_BUCKET_NAME) AS CREDITOR_BUCKET_NAME_2

                                                                      FROM FIRST_ELIGIBLE_INFO AS A

                                                                               --Only matching in IPL
                                                                               --when matching on original alias required - When lump sum offer expected (For IPL)
                                                                           LEFT JOIN REFINED_PROD.CONFIGURATION.CREDITOR_TERMS AS CT_ALIAS_LUMP
                                                                                     ON CT_ALIAS_LUMP.CREDITOR_BUCKET_NAME = A.CREDITOR_BUCKET_NAME
                                                                                         AND
                                                                                        A.DAYS_DELINQUENT_MIN = CT_ALIAS_LUMP.DAYS_DELINQUENT_MIN
                                                                                         AND
                                                                                        A.NEGOTIATION_BALANCE_MIN = CT_ALIAS_LUMP.NEGOTIATION_BALANCE_MIN
                                                                                         AND
                                                                                        A.ORIGINAL_CREDITOR_ALIAS_ID = CT_ALIAS_LUMP.ORIGINAL_CREDITOR_ALIAS_ID AND
                                                                                        CT_ALIAS_LUMP.IPL_USE_FLAG = TRUE
                                                                          --when matching on original alias is not required - When lump sum offer expected (For IPL)
                                                                           LEFT JOIN REFINED_PROD.CONFIGURATION.CREDITOR_TERMS AS CT_NO_ALIAS_LUMP
                                                                                     ON CT_NO_ALIAS_LUMP.CREDITOR_BUCKET_NAME = A.CREDITOR_BUCKET_NAME
                                                                                         AND
                                                                                        A.DAYS_DELINQUENT_MIN = CT_NO_ALIAS_LUMP.DAYS_DELINQUENT_MIN
                                                                                         AND
                                                                                        A.NEGOTIATION_BALANCE_MIN = CT_NO_ALIAS_LUMP.NEGOTIATION_BALANCE_MIN
                                                                                         AND
                                                                                        CT_NO_ALIAS_LUMP.ORIGINAL_CREDITOR_ALIAS_ID IS NULL AND
                                                                                        CT_NO_ALIAS_LUMP.IPL_USE_FLAG = TRUE

                                                                      )
                                            , TRADELINES AS (
                                                            SELECT P.SOURCE_SYSTEM
                                                                 , P.PROGRAM_NAME
                                                                 , P.ENROLLED_DATE_CST
                                                                 , TL.TRADELINE_NAME
                                                                 , TL.IS_LEGAL_FLAG
                                                                 , coalesce(CASE
                                                                                WHEN BTL.LAST_PAYMENT_DATE_C < '1990-01-01'
                                                                                    THEN CRL.LAST_ACTIVITY_DATE_C
                                                                                ELSE BTL.LAST_PAYMENT_DATE_C
                                                                                END, CRL.LAST_ACTIVITY_DATE_C,
                                                                            TL.CURRENT_LAST_PAYMENT_DATE_CST) AS LAST_PAYMENT_DATE
                                                                 , datediff(DAY,
                                                                            coalesce(LAST_PAYMENT_DATE, P.ENROLLED_DATE_CST),
                                                                            CURRENT_DATE) - 15 AS DQ
                                                                 , coalesce(TL.CURRENT_CREDITOR_ID,
                                                                            TL.CURRENT_CREDITOR_ALIAS_ID,
                                                                            TL.ORIGINAL_CREDITOR_ID,
                                                                            TL.ORIGINAL_CREDITOR_ALIAS_ID) AS CURRENT_CREDITOR_ID
                                                                 , TL.ORIGINAL_CREDITOR_ALIAS_ID
                                                                 , coalesce(TL.NEGOTIATION_BALANCE,
                                                                            TL.FEE_BASIS_BALANCE,
                                                                            TL.ENROLLED_BALANCE) AS NEGOTIATION_BALANCE
                                                            FROM CURATED_PROD.CRM.TRADELINE TL
                                                                 LEFT JOIN CURATED_PROD.CRM.PROGRAM P
                                                                           ON P.PROGRAM_NAME = TL.PROGRAM_NAME AND P.IS_CURRENT_RECORD_FLAG = TRUE
                                                                 LEFT JOIN REFINED_PROD.BEDROCK.PROGRAM_TRADELINE_C AS BTL
                                                                           ON BTL.NAME = TL.TRADELINE_NAME AND BTL.IS_DELETED = 'FALSE'
                                                                 LEFT JOIN REFINED_PROD.BEDROCK.OPPORTUNITY_TRADELINE_C OT
                                                                           ON BTL.OPPORTUNITY_TRADELINE_ID_C = OT.ID AND OT.IS_DELETED = FALSE
                                                                 LEFT JOIN REFINED_PROD.BEDROCK.CR_LIABILITY_C CRL
                                                                           ON CRL.ID = OT.CR_LIABILITY_ID_C AND CRL.IS_DELETED = FALSE
                                                            WHERE TL.TRADELINE_SETTLEMENT_STATUS NOT IN
                                                                  ('SETTLED', 'ATTRITED', 'NOT ENROLLED')
                                                              AND TL.IS_CURRENT_RECORD_FLAG = TRUE
                                                              AND P.SOURCE_SYSTEM = 'BEDROCK'
                                                              AND P.PROGRAM_STATUS IN ('Active', 'New', 'Enrolled')
                                                              AND TL.INCLUDE_IN_PROGRAM_FLAG = TRUE
                                                            )
                                            , TRADELINES_W_EST_OFFER AS (

                                                                        SELECT DISTINCT
                                                                            T.PROGRAM_NAME
                                                                             , T.TRADELINE_NAME
                                                                             , T.NEGOTIATION_BALANCE
                                                                             , T.CURRENT_CREDITOR_ID
                                                                             , C.CREDITOR_NAME
                                                                             , T.DQ AS CURRENT_DQ
                                                                             , CASE WHEN CB.CREDITOR_BUCKET_NAME IS NULL THEN 'N' ELSE 'Y' END AS TOP_50_CREDITOR
                                                                             , CASE
                                                                                   WHEN coalesce(
                                                                                           CT_ALIAS_LUMP.OFFER_PERCENT,
                                                                                           CT_NO_ALIAS_LUMP.OFFER_PERCENT) IS NULL
                                                                                       THEN 'N'
                                                                                   ELSE 'Y'
                                                                                   END AS SETTLEMENT_ELIGIBLE_NOW --i.e. meeting matrix eligibility criteria
                                                                             , CASE
                                                                                   WHEN SETTLEMENT_ELIGIBLE_NOW = 'Y' AND T.IS_LEGAL_FLAG = FALSE
                                                                                       THEN coalesce(
                                                                                           CT_ALIAS_LUMP.OFFER_PERCENT,
                                                                                           CT_NO_ALIAS_LUMP.OFFER_PERCENT)
                                                                                   WHEN SETTLEMENT_ELIGIBLE_NOW = 'Y' AND T.IS_LEGAL_FLAG = TRUE
                                                                                       THEN (coalesce(
                                                                                                     CT_ALIAS_LUMP.OFFER_PERCENT,
                                                                                                     CT_NO_ALIAS_LUMP.OFFER_PERCENT) +
                                                                                             coalesce(
                                                                                                     CT_ALIAS_LUMP.LEGAL_RATE_INCREASE,
                                                                                                     CT_NO_ALIAS_LUMP.LEGAL_RATE_INCREASE,
                                                                                                     0))
                                                                                   WHEN SETTLEMENT_ELIGIBLE_NOW = 'N' AND T.IS_LEGAL_FLAG = FALSE
                                                                                       THEN coalesce(
                                                                                           CTF_ALIAS_LUMP.OFFER_PERCENT_NOT_LEGAL,
                                                                                           CTF_NO_ALIAS_LUMP.OFFER_PERCENT_NOT_LEGAL)
                                                                                   WHEN SETTLEMENT_ELIGIBLE_NOW = 'N' AND T.IS_LEGAL_FLAG = TRUE
                                                                                       THEN coalesce(
                                                                                           CTF_ALIAS_LUMP.OFFER_PERCENT_IS_LEGAL,
                                                                                           CTF_NO_ALIAS_LUMP.OFFER_PERCENT_IS_LEGAL)
                                                                                   ELSE NULL
                                                                                   END AS EST_OFFER_PERCENT
                                                                             , CASE
                                                                                   WHEN SETTLEMENT_ELIGIBLE_NOW = 'Y'
                                                                                       THEN coalesce(
                                                                                           CT_ALIAS_LUMP.CREDITOR_BUCKET_NAME,
                                                                                           CT_NO_ALIAS_LUMP.CREDITOR_BUCKET_NAME)
                                                                                   WHEN SETTLEMENT_ELIGIBLE_NOW = 'N'
                                                                                       THEN coalesce(
                                                                                           CTF_ALIAS_LUMP.CREDITOR_BUCKET_NAME,
                                                                                           CTF_NO_ALIAS_LUMP.CREDITOR_BUCKET_NAME)
                                                                                   ELSE NULL
                                                                                   END AS CREDITOR_BUCKET_NAME

                                                                        FROM TRADELINES AS T
                                                                             LEFT JOIN
                                                                             CURATED_PROD.CRM.CREDITOR C
                                                                             ON T.CURRENT_CREDITOR_ID = C.CREDITOR_ID AND C.IS_CURRENT_RECORD_FLAG
                                                                             LEFT JOIN REFINED_PROD.CONFIGURATION.CREDITOR_BUCKETS AS CB
                                                                                       ON T.CURRENT_CREDITOR_ID = CB.CURRENT_CREDITOR_ID

                                                                            --currently eligibile tradelines
                                                                            --when matching on original alias required - When lump sum offer expected (For IPL)
                                                                             LEFT JOIN REFINED_PROD.CONFIGURATION.CREDITOR_TERMS AS CT_ALIAS_LUMP
                                                                                       ON CT_ALIAS_LUMP.CREDITOR_BUCKET_NAME = CB.CREDITOR_BUCKET_NAME
                                                                                           AND
                                                                                          T.DQ >= CT_ALIAS_LUMP.DAYS_DELINQUENT_MIN AND
                                                                                          (CT_ALIAS_LUMP.DAYS_DELINQUENT_MAX IS NULL OR
                                                                                           T.DQ < CT_ALIAS_LUMP.DAYS_DELINQUENT_MAX)
                                                                                           AND
                                                                                          T.NEGOTIATION_BALANCE >= CT_ALIAS_LUMP.NEGOTIATION_BALANCE_MIN AND
                                                                                          (CT_ALIAS_LUMP.NEGOTIATION_BALANCE_MAX IS NULL OR
                                                                                           T.NEGOTIATION_BALANCE < CT_ALIAS_LUMP.NEGOTIATION_BALANCE_MAX)
                                                                                           AND
                                                                                          T.ORIGINAL_CREDITOR_ALIAS_ID = CT_ALIAS_LUMP.ORIGINAL_CREDITOR_ALIAS_ID AND
                                                                                          CT_ALIAS_LUMP.IPL_USE_FLAG = TRUE
                                                                            --when matching on original alias is not required - When lump sum offer expected (For IPL)
                                                                             LEFT JOIN REFINED_PROD.CONFIGURATION.CREDITOR_TERMS AS CT_NO_ALIAS_LUMP
                                                                                       ON CT_NO_ALIAS_LUMP.CREDITOR_BUCKET_NAME = CB.CREDITOR_BUCKET_NAME
                                                                                           AND
                                                                                          T.DQ >= CT_NO_ALIAS_LUMP.DAYS_DELINQUENT_MIN AND
                                                                                          (CT_NO_ALIAS_LUMP.DAYS_DELINQUENT_MAX IS NULL OR
                                                                                           T.DQ < CT_NO_ALIAS_LUMP.DAYS_DELINQUENT_MAX)
                                                                                           AND
                                                                                          T.NEGOTIATION_BALANCE >= CT_NO_ALIAS_LUMP.NEGOTIATION_BALANCE_MIN AND
                                                                                          (CT_NO_ALIAS_LUMP.NEGOTIATION_BALANCE_MAX IS NULL OR
                                                                                           T.NEGOTIATION_BALANCE < CT_NO_ALIAS_LUMP.NEGOTIATION_BALANCE_MAX)
                                                                                           AND
                                                                                          CT_NO_ALIAS_LUMP.ORIGINAL_CREDITOR_ALIAS_ID IS NULL AND
                                                                                          CT_NO_ALIAS_LUMP.IPL_USE_FLAG = TRUE
                                                                            --For matching info with future eligibile programs
                                                                            --when matching on original alias required - (IPL)
                                                                             LEFT JOIN FIRST_ELIGIBLE_INFO_2 AS CTF_ALIAS_LUMP
                                                                                       ON CTF_ALIAS_LUMP.CREDITOR_BUCKET_NAME = CB.CREDITOR_BUCKET_NAME
                                                                                           AND
                                                                                          T.NEGOTIATION_BALANCE >= CTF_ALIAS_LUMP.NEGOTIATION_BALANCE_MIN AND
                                                                                          (CTF_ALIAS_LUMP.NEGOTIATION_BALANCE_MAX IS NULL OR
                                                                                           T.NEGOTIATION_BALANCE < CTF_ALIAS_LUMP.NEGOTIATION_BALANCE_MAX)
                                                                                           AND
                                                                                          T.ORIGINAL_CREDITOR_ALIAS_ID =
                                                                                          CTF_ALIAS_LUMP.ORIGINAL_CREDITOR_ALIAS_ID
                                                                            --when no matching on original alias required - (IPL)
                                                                             LEFT JOIN FIRST_ELIGIBLE_INFO_2 AS CTF_NO_ALIAS_LUMP
                                                                                       ON CTF_NO_ALIAS_LUMP.CREDITOR_BUCKET_NAME = CB.CREDITOR_BUCKET_NAME
                                                                                           AND
                                                                                          T.NEGOTIATION_BALANCE >= CTF_NO_ALIAS_LUMP.NEGOTIATION_BALANCE_MIN AND
                                                                                          (CTF_NO_ALIAS_LUMP.NEGOTIATION_BALANCE_MAX IS NULL OR
                                                                                           T.NEGOTIATION_BALANCE < CTF_NO_ALIAS_LUMP.NEGOTIATION_BALANCE_MAX)
                                                                                           AND
                                                                                          CTF_NO_ALIAS_LUMP.ORIGINAL_CREDITOR_ALIAS_ID IS NULL
                                                                        )

                                         SELECT PROGRAM_NAME
                                              , TRADELINE_NAME
                                              , CREDITOR_NAME
                                              --add case when to limit settlement rate to 100% max
                                              , CASE
                                                    WHEN ((EST_OFFER_PERCENT + 3) / 100) > 1 THEN 1
                                                    ELSE ((EST_OFFER_PERCENT + 3) / 100)
                                                    END AS OFFER_PERCENT --adding 3% buffer
                                         FROM TRADELINES_W_EST_OFFER
                                         WHERE EST_OFFER_PERCENT IS NOT NULL
                                         )

   , ALL_DATA AS (
                 SELECT DISTINCT
                        P.PROGRAM_NAME
                      , P.PROGRAM_ID
                      , RIGHT(coalesce(P.LEGACY_MIGRATION_PROGRAM_ID, P.PROGRAM_ID), 6) AS ACTIVATION_CODE
                      , P.CREATED_DATE_CST AS PROGRAM_CREATED
                      , NEW_OR_AGED_BOOK.PROGRAM_AGE_BUCKET
                      , P.PROGRAM_STATUS
                      , CT.FIRST_NAME
                      , CT.LAST_NAME
                      , CT.MAILING_STREET AS MAILING_ADDRESS
                      , CT.MAILING_CITY AS CITY
                      , CT.MAILING_STATE AS STATE
                      , LEFT(CT.MAILING_POSTAL_CODE, 5) AS ZIP_CODE
                      , CT.EMAIL AS EMAIL_ADDRESS
                      , REPLACE(regexp_replace(nvl(CT.MOBILE_PHONE, nvl(CT.HOME_PHONE, nvl(CT.PHONE, CT.OTHER_PHONE))),
                                               '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') AS TELEPHONE_NUMBER
                      , CT.BIRTHDATE
                      , CT.SSN_C AS SSN
                      , nvl(CURRENT_DRAFT_AMT.PER_FREQ_AMOUNT, 0) AS DRAFT_AMOUNT
                      , CASE
                            WHEN P.PAYMENT_FREQUENCY = 'Semi-Monthly' THEN 'Twice Monthly'
                            ELSE P.PAYMENT_FREQUENCY
                            END AS DEPOSIT_FREQUENCY
--                       , P.PAYMENT_FREQUENCY AS DEPOSIT_FREQUENCY
                      , PD.PAYMENT_DATE1 AS LAST_PAYMENT_DATE
                      , PD.PAYMENT_DATE2 AS LAST_PAYMENT_DATE2
                      , CAST(sum(CASE
                                     WHEN upper(TL_LIST.TRADELINE_SETTLEMENT_STATUS) NOT IN ('SETTLED', 'ATTRITED')
                                         THEN (coalesce(TL_LIST.NEGOTIATION_BALANCE, TL_LIST.FEE_BASIS_BALANCE,
                                                        TL_LIST.ORIGINAL_BALANCE) * 1.00) *
                                              coalesce(MATRIX.OFFER_PERCENT, C.TOP_90_PERCENT_SETTLEMENT_PCT, .70)
                                     WHEN upper(TL_LIST.TRADELINE_SETTLEMENT_STATUS) IN ('SETTLED') AND
                                          TL_LIST.TRADELINE_SETTLEMENT_SUB_STATUS NOT LIKE 'BUSTED%' AND
                                          TL_LIST.TRADELINE_SETTLEMENT_SUB_STATUS != 'PAID OFF'
                                         THEN TL_LIST.CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
                                     WHEN upper(TL_LIST.TRADELINE_SETTLEMENT_STATUS) IN ('SETTLED') AND
                                          TL_LIST.TRADELINE_SETTLEMENT_SUB_STATUS LIKE 'BUSTED%'
                                         THEN (coalesce(TL_LIST.NEGOTIATION_BALANCE, TL_LIST.FEE_BASIS_BALANCE,
                                                        TL_LIST.ORIGINAL_BALANCE) * 1.00) *
                                              coalesce(MATRIX.OFFER_PERCENT, C.TOP_90_PERCENT_SETTLEMENT_PCT, .70)
                                     ELSE 0
                                     END) OVER (PARTITION BY P.PROGRAM_ID)
                                 +
                             sum(CASE
                                     WHEN upper(TL_LIST.TRADELINE_SETTLEMENT_STATUS) NOT IN ('SETTLED', 'ATTRITED')
                                         THEN TL_LIST.ESTIMATED_FEES
                                     WHEN upper(TL_LIST.TRADELINE_SETTLEMENT_STATUS) IN ('SETTLED') AND
                                          TL_LIST.TRADELINE_SETTLEMENT_SUB_STATUS != 'PAID OFF'
                                         THEN TL_LIST.FEES_OUTSTANDING_AMOUNT
                                     ELSE 0
                                     END) OVER (PARTITION BY P.PROGRAM_ID)
                     -
                             CFT_ACCOUNT_BALANCE.AVAILABLE_BALANCE AS DECIMAL(18, 2))
                            + 6 * (nvl(P.MONTHLY_LEGAL_SERVICE_FEE, 0) + nvl(P.MONTHLY_TRUST_ACCOUNT_SERVICE_FEE, 0))
                            AS AMOUNT_FINANCED
                      , sum(CASE
                                WHEN upper(TL_LIST.TRADELINE_SETTLEMENT_STATUS) NOT IN ('SETTLED', 'ATTRITED')
                                    THEN TL_LIST.ESTIMATED_FEES
                                WHEN upper(TL_LIST.TRADELINE_SETTLEMENT_STATUS) IN ('SETTLED') AND
                                     TL_LIST.TRADELINE_SETTLEMENT_SUB_STATUS != 'PAID OFF'
                                    THEN TL_LIST.FEES_OUTSTANDING_AMOUNT
                                ELSE 0
                                END) OVER (PARTITION BY P.PROGRAM_ID)
                            + 6 * (nvl(P.MONTHLY_LEGAL_SERVICE_FEE, 0) +
                                   nvl(P.MONTHLY_TRUST_ACCOUNT_SERVICE_FEE, 0)) AS ESTIMATED_BEYOND_PROGRAM_FEES
                      , CFT_ACCOUNT_BALANCE.AVAILABLE_BALANCE AS TOTAL_DEPOSITS
                      , coalesce(CAST(sum(CASE
                                              WHEN upper(TL_LIST.TRADELINE_SETTLEMENT_STATUS) NOT IN
                                                   ('SETTLED', 'ATTRITED')
                                                  THEN (coalesce(TL_LIST.NEGOTIATION_BALANCE, TL_LIST.FEE_BASIS_BALANCE,
                                                                 TL_LIST.ORIGINAL_BALANCE) * 1.00) *
                                                       coalesce(MATRIX.OFFER_PERCENT, C.TOP_90_PERCENT_SETTLEMENT_PCT, .70)
                                              WHEN upper(TL_LIST.TRADELINE_SETTLEMENT_STATUS) IN ('SETTLED') AND
                                                   TL_LIST.TRADELINE_SETTLEMENT_SUB_STATUS NOT LIKE 'BUSTED%' AND
                                                   TL_LIST.TRADELINE_SETTLEMENT_SUB_STATUS != 'PAID OFF'
                                                  THEN TL_LIST.CREDITOR_PAYMENTS_OUTSTANDING_AMOUNT
                                              WHEN upper(TL_LIST.TRADELINE_SETTLEMENT_STATUS) IN ('SETTLED') AND
                                                   TL_LIST.TRADELINE_SETTLEMENT_SUB_STATUS LIKE 'BUSTED%'
                                                  THEN (coalesce(TL_LIST.NEGOTIATION_BALANCE, TL_LIST.FEE_BASIS_BALANCE,
                                                                 TL_LIST.ORIGINAL_BALANCE) * 1.00) *
                                                       coalesce(MATRIX.OFFER_PERCENT, C.TOP_90_PERCENT_SETTLEMENT_PCT, .70)
                                              ELSE 0
                                              END)
                                          OVER (PARTITION BY P.PROGRAM_ID) AS DECIMAL(18, 2)),
                                 0) AS SETTLEMENT_AMOUNT
                      , TL_LIST.CURRENT_CREDITOR AS TRADELINE_NAME
                      , '' AS TRADELINE_ACCOUNT_NUMBER
--                       , '121000248' AS ROUTING_NUMBER
--                       , '4027877604' AS ACCOUNT_NUMBER
                      , try_to_number(TA.ROUTING_NUMBER_C) AS CFT_ROUTING_NUMBER    // Will return junk data with my permission level
                      , try_to_number(TA.ACCOUNT_NUMBER_C) AS CFT_ACCOUNT_NUMBER    // Will return junk data with my permission level
                      , CASE
                            WHEN try_to_number(TA.ROUTING_NUMBER_C) = 053101561 THEN 'WELLS FARGO BANK'
                            WHEN try_to_number(TA.ROUTING_NUMBER_C) = 053112505 THEN 'AXOS BANK'
                            ELSE NULL
                            END AS CFT_BANK_NAME    // Will return junk data with my permission level
                      , concat(CT.FIRST_NAME, ' ', CT.LAST_NAME) AS CFT_ACCOUNT_HOLDER_NAME // Will return junk data with my permission level
                      , CFT_ACCOUNT_BALANCE.PROCESSOR_CLIENT_ID :: VARCHAR AS EXTERNAL_ID
                      , CASE WHEN COCLIENTS.CT > 0 THEN TRUE ELSE FALSE END AS CO_CLIENT
                      , datediff(MONTH, P.ENROLLED_DATE_CST, CURRENT_DATE) AS MONTHS_SINCE_ENROLLMENT
                      , NEXT_DRAFT_DATE.NEXT_DRAFT_DATE AS NEXT_PAYMENT_DATE
                      , nvl(ACTIVE_DEBTS.UNSETTLED_DEBT, 0) +
                        nvl(NUM_OF_SETTLEMENTS.TERM_PAY_BALANCE, 0) AS TOTAL_AMOUNT_ENROLLED_DEBT
                      , P.ENROLLED_DATE_CST AS BEYOND_ENROLLMENT_DATE
                      , P.PROGRAM_STATUS AS BEYOND_ENROLLMENT_STATUS
                      , coalesce(LAST_NSF.NSF_3_MOS, 0) AS NSFS_3_MONTHS
                      , TL_LIST.ORIGINAL_CREDITOR
                      , coalesce(MATRIX.OFFER_PERCENT, C.TOP_90_PERCENT_SETTLEMENT_PCT, .70) AS SETTLEMENT_PERCENT
                      , TL_LIST.TRADELINE_SETTLEMENT_STATUS || ' - ' ||
                        TL_LIST.TRADELINE_SETTLEMENT_SUB_STATUS AS SETTLED_TRADELINED_FLAG
                      , coalesce(DA90.DEPADHERENCE, 0) AS PAYMENT_ADHERENCE_RATIO_3_MONTHS
                      , coalesce(DA180.DEPADHERENCE, 0) AS PAYMENT_ADHERENCE_RATIO_6_MONTHS
                      , BANK_ACCOUNT.HOLDER_S_NAME_C
                      , BANK_ACCOUNT.BANK_NAME_C
                      , BANK_ACCOUNT.ACCOUNT_NUMBER_C
                      , BANK_ACCOUNT.ROUTING_NUMBER_C
                      , BANK_ACCOUNT.TYPE_C
                      , HISTORICAL_SETTLEMENT_PERCENT.HISTORICAL_SETTLEMENT_PERCENT
                      , REMAINING_DEPOSITS.BF_REMAINING_DEPOSITS
                      , CURRENT_DRAFT_AMT.AMOUNT AS MONTHLYAMT
                      , P.SOURCE_SYSTEM
                      , DISCOUNT_FACTOR
                      , coalesce(DNC1.DNC_NUMBER, DNC2.DNC_NUMBER, DNC3.DNC_NUMBER, DNC4.DNC_NUMBER) AS DNC_NUMBER_NEW
                      , DNC.DNC_NUMBER AS DNC_NUMBER_AGED
                      , RECENT_PAYMENTS.PROGRAM_ID AS RECENT_PAYMENTS_PROGRAM_ID
                      , TERMINATION_REQUESTED.program_id AS TERM_REQUESTED_PROGRAM_ID
                      , FICO_SCORE.CREDIT_SCORE AS FICO_SCORE
                      , COALESCE(CCPA1.CCPA_PHONE, CCPA2.CCPA_PHONE, CCPA3.CCPA_PHONE, CCPA4.CCPA_PHONE,
                                 CCPA5.CCPA_EMAIL) AS CCPA_CONTACT
                      , FULL_PAYMENTS.PROGRAM_ID AS FULL_PAYMENTS_PROGRAM_ID
                      , LAST_NSF.LAST_NSF_DT
                      , PRIOR_LOAN_APPLICANT.PROGRAM_ID_C AS PRIOR_LOAN_APPLICANT_PROGRAM_ID
                      , coalesce(DQ.PROGDQB75, 0) AS DQ_DELTA_BY_DAY75
                      , TL_LIST.TRADELINE_NAME AS TL_NAME
                      , P.SERVICE_ENTITY_NAME
                      , PROGRAM_NO_CRED.PROGRAM_NAME AS PROGRAM_NO_CRED_PROGRAM_NAME

                 FROM CURATED_PROD.CRM.PROGRAM P
                      JOIN REFINED_PROD.BEDROCK.PROGRAM_C PC ON PC.NAME = P.PROGRAM_NAME AND PC.IS_DELETED = FALSE
                      LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT ACT ON ACT.ID = PC.ACCOUNT_ID_C AND ACT.IS_DELETED = FALSE
                      LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT_CONTACT_RELATION ACR
                                ON ACR.ACCOUNT_ID = ACT.ID AND ACR.RELATIONSHIP_C = 'Client' AND ACR.IS_DELETED = FALSE
                      LEFT JOIN REFINED_PROD.BEDROCK.CONTACT CT ON ACR.CONTACT_ID = CT.ID AND CT.IS_DELETED = FALSE
                      LEFT JOIN REFINED_PROD.BEDROCK.TRUST_ACCOUNT_C TA
                                ON P.PROGRAM_ID = TA.PROGRAM_ID_C AND TA.IS_DELETED = FALSE
                      LEFT JOIN COCLIENTS ON COCLIENTS.PROGRAM_NAME = P.PROGRAM_NAME
                     --left join REFINED_PROD.BEDROCK.ACCOUNT_CONTACT_RELATION CoClient on CoClient.ACCOUNT_ID=PC.ACCOUNT_ID_C and CoClient.RELATIONSHIP_C='Co-Client'
                      LEFT JOIN LAST_NSF ON LAST_NSF.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN CURRENT_DRAFT_AMT ON CURRENT_DRAFT_AMT.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN NUM_OF_SETTLEMENTS ON NUM_OF_SETTLEMENTS.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN ACTIVE_DEBTS ON ACTIVE_DEBTS.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN PAYMENT_DATES PD ON PD.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN TL_LIST
                                ON TL_LIST.PROGRAM_NAME = P.PROGRAM_NAME
                                    AND (TL_LIST.INCLUDE_IN_PROGRAM_FLAG = TRUE OR
                                         (TL_LIST.CONDITIONAL_DEBT_STATUS IN ('Pending') AND
                                          TL_LIST.TRADELINE_SETTLEMENT_STATUS = 'ENROLLED'))
                      LEFT JOIN DEFERRAL ON DEFERRAL.PROGRAM_ID = P.PROGRAM_ID
--  left join REFINED_PROD.SALESFORCE.NU_DSE_FEE_TEMPLATE_C ft ON ft.id = program.NU_DSE_FEE_TEMPLATE_C
                      LEFT JOIN NEXT_DRAFT_DATE NDD ON NDD.PROGRAM_NAME = P.PROGRAM_NAME
                      LEFT JOIN CREDITOR_SETTLEMENTS C ON C.ORIGINAL_CREDITOR = TL_LIST.ORIGINAL_CREDITOR
                     AND C.CURRENT_CREDITOR = TL_LIST.CURRENT_CREDITOR
                      LEFT JOIN TERMINATION_REQUESTED ON TERMINATION_REQUESTED.program_id = P.PROGRAM_ID
                      LEFT JOIN DNC ON DNC.DNC_NUMBER = REPLACE(
                         regexp_replace(nvl(CT.MOBILE_PHONE, nvl(CT.HOME_PHONE, nvl(CT.PHONE, CT.OTHER_PHONE))),
                                        '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
                      LEFT JOIN DNC DNC1
                                ON DNC1.DNC_NUMBER =
                                   REPLACE(regexp_replace(CT.MOBILE_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
                      LEFT JOIN DNC DNC2 ON DNC2.DNC_NUMBER =
                                            REPLACE(regexp_replace(CT.PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
                      LEFT JOIN DNC DNC3
                                ON DNC3.DNC_NUMBER =
                                   REPLACE(regexp_replace(CT.HOME_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
                      LEFT JOIN DNC DNC4
                                ON DNC4.DNC_NUMBER =
                                   REPLACE(regexp_replace(CT.OTHER_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '')
                      LEFT JOIN PRIOR_LOAN_APPLICANT ON PRIOR_LOAN_APPLICANT.PROGRAM_ID_C = P.PROGRAM_ID
                      LEFT JOIN CFT_MONTHLY_FEES ON CFT_MONTHLY_FEES.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN BLP_MONTHLY_FEE ON BLP_MONTHLY_FEE.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN CFT_ACCOUNT_BALANCE ON CFT_ACCOUNT_BALANCE.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN CFT_PRIOR_MONTH_PAYMENT ON CFT_PRIOR_MONTH_PAYMENT.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN FICO_SCORE ON FICO_SCORE.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN NEW_OR_AGED_BOOK ON NEW_OR_AGED_BOOK.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN RECENT_PAYMENTS ON RECENT_PAYMENTS.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN SCHEDULE_ADHERENCE ON SCHEDULE_ADHERENCE.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN BEYOND_FEES ON BEYOND_FEES.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN FEES_OUTSTANDING ON FEES_OUTSTANDING.PROGRAM_ID = P.PROGRAM_ID
                     --left join fee_template on fee_template.program_id = program.id
                      LEFT JOIN CCPA_PHONE CCPA1
                                ON REPLACE(regexp_replace(CT.MOBILE_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') =
                                   CCPA1.CCPA_PHONE
                      LEFT JOIN CCPA_PHONE CCPA2
                                ON REPLACE(regexp_replace(CT.HOME_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') =
                                   CCPA2.CCPA_PHONE
                      LEFT JOIN CCPA_PHONE CCPA3
                                ON REPLACE(regexp_replace(CT.OTHER_PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') =
                                   CCPA3.CCPA_PHONE
                      LEFT JOIN CCPA_PHONE CCPA4
                                ON REPLACE(regexp_replace(CT.PHONE, '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') =
                                   CCPA4.CCPA_PHONE
                      LEFT JOIN CCPA_EMAIL CCPA5 ON CT.EMAIL = CCPA5.CCPA_EMAIL
                      LEFT JOIN FULL_PAYMENTS ON FULL_PAYMENTS.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN NEXT_DRAFT_DATE ON NEXT_DRAFT_DATE.PROGRAM_ID = P.PROGRAM_ID
                      LEFT JOIN DEPOSIT_ADHERENCE DA90 ON DA90.PROGRAM_NAME = P.PROGRAM_NAME AND DA90.TERM = 90
                      LEFT JOIN DEPOSIT_ADHERENCE DA180 ON DA180.PROGRAM_NAME = P.PROGRAM_NAME AND DA180.TERM = 180
                      LEFT JOIN BANK_ACCOUNT ON BANK_ACCOUNT.NU_DSE_PROGRAM_C = P.PROGRAM_NAME
                      LEFT JOIN HISTORICAL_SETTLEMENT_PERCENT
                                ON HISTORICAL_SETTLEMENT_PERCENT.PROGRAM_NAME = P.PROGRAM_NAME
                      LEFT JOIN REMAINING_DEPOSITS ON REMAINING_DEPOSITS.PROGRAM_NAME = P.PROGRAM_NAME
                      LEFT JOIN DQ ON P.PROGRAM_NAME = DQ.PROGRAM_NAME
                      LEFT JOIN PROGRAM_NO_CRED ON P.PROGRAM_NAME = PROGRAM_NO_CRED.PROGRAM_NAME
                      LEFT JOIN CREDITOR_MATRIX_SETTLEMENT_LOGIC AS MATRIX
                                ON TL_LIST.TRADELINE_NAME = MATRIX.TRADELINE_NAME
                 WHERE 1 = 1
                   AND P.IS_CURRENT_RECORD_FLAG = TRUE
                   AND (TL_LIST.TRADELINE_SETTLEMENT_STATUS NOT IN ('ATTRITED', 'NOT ENROLLED')
                     OR (TL_LIST.TRADELINE_SETTLEMENT_STATUS != 'SETTLED' AND
                         TL_LIST.TRADELINE_SETTLEMENT_SUB_STATUS != 'PAID OFF')
                     OR TL_LIST.TRADELINE_SETTLEMENT_STATUS IS NULL)
                 )

SELECT DISTINCT
       current_date AS LOADED_DATE
     , PROGRAM_NAME
     , PROGRAM_ID
     , ACTIVATION_CODE
     , FIRST_NAME
     , LAST_NAME
     , SOURCE_SYSTEM
     , PROGRAM_CREATED
     , PROGRAM_AGE_BUCKET
     , STATE
     , BEYOND_ENROLLMENT_STATUS
     , AMOUNT_FINANCED
     , SETTLEMENT_AMOUNT
     , ESTIMATED_BEYOND_PROGRAM_FEES
     , TOTAL_DEPOSITS
     , LAST_PAYMENT_DATE
     , LAST_PAYMENT_DATE2
     , NEXT_PAYMENT_DATE
     , MONTHS_SINCE_ENROLLMENT
     , DRAFT_AMOUNT
     , DISCOUNT_FACTOR
     , DEPOSIT_FREQUENCY
     , LAST_NSF_DT
     , PAYMENT_ADHERENCE_RATIO_3_MONTHS
     , PAYMENT_ADHERENCE_RATIO_6_MONTHS
     , FICO_SCORE
     , CASE
           WHEN STATE = 'MI' AND PROGRAM_CREATED < dateadd(MONTH, -5, CAST(CURRENT_DATE - 1 AS DATE)) THEN TRUE
           WHEN STATE <> 'MI' AND PROGRAM_CREATED < dateadd(MONTH, -3, CAST(CURRENT_DATE - 1 AS DATE)) THEN TRUE
           ELSE FALSE
           END AS RULE_PROGRAM_DURATION
     , IFF(STATE IN
           ('CA', 'MI', 'TX', 'IN', 'NC', 'MO', 'AL', 'NM', 'TN', 'MS', 'MT', 'KY', 'FL', 'AK', 'SD', 'DC', 'OK', 'WI',
            'NY', 'PA', 'VA', 'AZ', 'AR', 'UT', 'ID', 'LA', 'MD', 'NE'), TRUE, FALSE) AS RULE_STATE
     , IFF((LAST_NSF_DT IS NULL OR LAST_NSF_DT < dateadd(MONTH, -3, cast(current_date - 1 AS DATE))),
           TRUE, FALSE) AS RULE_RECENT_NSF
     , TRUE AS RULE_CO_CLIENT
     , IFF(DNC_NUMBER_NEW IS NULL AND DNC_NUMBER_AGED IS NULL AND EMAIL_ADDRESS IS NOT NULL AND
           TELEPHONE_NUMBER IS NOT NULL AND SSN IS NOT NULL, TRUE, FALSE) AS RULE_DNC
     , IFF(PRIOR_LOAN_APPLICANT_PROGRAM_ID IS NULL, TRUE, FALSE) AS RULE_PRIOR_APPLICANT
     , IFF(RECENT_PAYMENTS_PROGRAM_ID IS NOT NULL, TRUE, FALSE) AS RULE_RECENT_PAYMENTS
     , CASE
           WHEN PROGRAM_AGE_BUCKET = 'New Book' THEN IFF(coalesce(PAYMENT_ADHERENCE_RATIO_3_MONTHS, 0) >= 0.95, TRUE,
                                                         FALSE)
           WHEN PROGRAM_AGE_BUCKET = 'Aged Book' THEN IFF(coalesce(PAYMENT_ADHERENCE_RATIO_6_MONTHS, 0) >= 0.80, TRUE,
                                                          FALSE)
           END AS RULE_DEPOSIT_ADHERENCE
     , CASE
           WHEN PROGRAM_AGE_BUCKET = 'Aged Book' AND SOURCE_SYSTEM = 'LEGACY' THEN TRUE
           ELSE IFF(TERM_REQUESTED_PROGRAM_ID IS NULL, TRUE, FALSE)
           END AS RULE_TERM_REQUESTED
     , CASE
           WHEN PROGRAM_AGE_BUCKET = 'New Book' THEN IFF(FICO_SCORE >= 540, TRUE, FALSE)
           WHEN PROGRAM_AGE_BUCKET = 'Aged Book' THEN TRUE
           END AS RULE_FICO_SCORE
     , IFF(CCPA_CONTACT IS NULL, TRUE, FALSE) AS RULE_CCPA
     , IFF(FULL_PAYMENTS_PROGRAM_ID IS NULL, TRUE, TRUE) AS RULE_FULL_PAYMENTS
     , CASE
           WHEN PROGRAM_AGE_BUCKET = 'New Book'
               THEN IFF((1 - (MONTHLYAMT - ((AMOUNT_FINANCED / .95) / DISCOUNT_FACTOR)) /
                             (CASE WHEN MONTHLYAMT > 0 THEN MONTHLYAMT END)
                            ) <= 1.30, TRUE, FALSE)
           WHEN PROGRAM_AGE_BUCKET = 'Aged Book'
               THEN IFF((1 - (MONTHLYAMT - ((AMOUNT_FINANCED / .95) / DISCOUNT_FACTOR)) /
                             (CASE WHEN MONTHLYAMT > 0 THEN MONTHLYAMT END)
                            ) <= 1.48, TRUE, FALSE)
           END AS RULE_PAYMENT_SIZE
     , IFF(AMOUNT_FINANCED >= (IFF(STATE = 'CA', 5000, 1000)) AND AMOUNT_FINANCED <= 71250, TRUE,
           FALSE) AS RULE_LOAN_AMOUNT
     , TRUE AS RULE_PROG_REMAINING
     , IFF((PROGRAM_AGE_BUCKET = 'Aged Book' OR DQ_DELTA_BY_DAY75 >= 0)
               AND PROGRAM_NO_CRED_PROGRAM_NAME IS NULL, TRUE, FALSE) AS RULE_TRADELINE_DELINQUENCY
     , RULE_PROGRAM_DURATION AND RULE_STATE AND RULE_RECENT_NSF AND RULE_CO_CLIENT AND RULE_DNC AND
       RULE_PRIOR_APPLICANT AND RULE_RECENT_PAYMENTS AND RULE_DEPOSIT_ADHERENCE AND RULE_TERM_REQUESTED AND
       RULE_FICO_SCORE AND RULE_CCPA AND RULE_FULL_PAYMENTS AND RULE_PAYMENT_SIZE AND RULE_LOAN_AMOUNT AND
       RULE_PROG_REMAINING AND RULE_TRADELINE_DELINQUENCY AS IS_ELIGIBLE
     , SERVICE_ENTITY_NAME
FROM ALL_DATA
WHERE PROGRAM_STATUS IN ('Active', 'New Client', 'Enrolled');

