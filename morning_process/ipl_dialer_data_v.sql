CREATE OR REPLACE VIEW SNO_SANDBOX.IPL.DIALER_DATA_V AS
SELECT *
FROM (
     WITH FIVE9_QUERY AS (
                         SELECT cast("five9_call_log.called_dt_no_tz_date" AS DATE) AS CALLED_DATE
                              , cast("five9_call_log.called_dt_no_tz_time" AS DATETIME) AS CALLED_TIME
                              , "five9_call_log.call_time" AS CALL_TIME_S
                              , "five9_call_log.talk_time" AS TALK_TIME_S
                              , "five9_call_log.dnis" AS DNIS
                              , "five9_call_log.ani" AS ANI
                              , "five9_call_log.lead_id" AS LEAD_ID
                              , "five9_call_log.prospect_name" AS PROSPECT_NAME
                              , "five9_call_log.salesforce_id" AS SALESFORCE_ID
                              , "five9_call_log.campaign" AS CAMPAIGN
                              , "five9_call_log.campaign_type" AS CAMPAIGN_TYPE
                              , "five9_call_log.disposition" AS DISPOSITION
                              , "five9_call_log.disposition_group_sort_order" AS DISPOSITION_GROUP_SORT_ORDER
                              , "five9_call_log.disposition_group" AS DISPOSITION_GROUP
                              , "five9_call_log.agent" AS AGENT
                              --,"five9_call_log.sales_disposition_is_contact"	as	pitched
                              , CASE
                                    WHEN (
                                                 contains(lower(DISPOSITION), 'voicemail')
                                                 OR contains(lower(DISPOSITION), 'voicemal')
                                                 OR (contains(lower(DISPOSITION), 'not available') and disposition not ilike '%pitch%')
                                                 OR contains(lower(DISPOSITION), 'disconnected')
                                                 OR contains(lower(DISPOSITION), 'not in service')
                                                 OR contains(lower(DISPOSITION), 'wrong number')
                                                 OR contains(lower(DISPOSITION), 'agent error')
                                                 OR contains(lower(DISPOSITION), 'dead air')
                                                 OR contains(lower(DISPOSITION), 'no answer')
                                                 OR contains(lower(DISPOSITION), 'answering machine')
                                                 OR contains(lower(DISPOSITION), 'declined')
                                                 OR contains(lower(DISPOSITION), 'busy')
                                                 OR contains(lower(DISPOSITION), 'unable to leave message')
                                                 OR contains(lower(DISPOSITION), 'queue callback timeout')
                                                 OR contains(lower(DISPOSITION), 'operator intercept')
                                                 OR contains(lower(DISPOSITION), 'dial error')
                                                 OR contains(lower(DISPOSITION), 'unauthorized 3rd party')
                                                 OR contains(lower(DISPOSITION), 'abandon')
                                                 OR DISPOSITION IS NULL
                                             ) OR HANDLE_TIME_SECONDS <= 30
                                        THEN 0
                                    ELSE 1
                                    END AS CONTACTED
                              , CASE
                                    WHEN DISPOSITION IN (
                                                         'Client Not Interested in Lending Option',
                                                         'Status of Loan - Loan Questions',
                                                         'Transferred To 3rd Party',
                                                         'Not Interested', 'Transferred to Lender',
                                                         'App Completed on Call', 'Attempted Transfer',
                                                         'Attempted to Transfer - LUSA Unavailable',
                                                         'Attempted Transfer - Call Back Scheduled',
                                                         'Attempted Transfer - Transferred',
                                                         'Client Not Available - Post Pitch - No Call back Scheduled',
                                                         'Client Not Available - Post Pitch - Call Back Scheduled',
                                                         'Not Interested - Post Pitch'
                                        ) THEN 1
                                    ELSE 0
                                    END AS PITCHED
                              , CASE WHEN DISPOSITION ILIKE '%not interested%' THEN 1 ELSE 0 END AS NOT_INTERESTED
                              , CASE
                                    WHEN DISPOSITION NOT ILIKE '%not interested%' AND DISPOSITION ILIKE '%interested%'
                                        THEN 1
                                    WHEN DISPOSITION ILIKE '%attempted to transfer%unavailable%' THEN 1
                                    WHEN DISPOSITION IN ('Attempted Transfer - Call Back Scheduled',
                                                         'Client Not Available - Post Pitch - No Call back Scheduled',
                                                         'Client Not Available - Post Pitch - Call Back Scheduled')
                                        THEN 1
                                    ELSE 0
                                    END AS INTERESTED_UNABLE_TO_TRANSFER
                              , CASE
                                    WHEN DISPOSITION ILIKE '%transferred to lender%'
                                        OR DISPOSITION ILIKE '%app completed on call%'
                                        OR DISPOSITION ILIKE '%transferred to 3rd party%'
                                        OR DISPOSITION IN ('Attempted Transfer - Transferred')
                                        THEN 1
                                    ELSE 0
                                    END AS TRANSFERRED_TO_LENDER
                              , CASE
                                    WHEN CAMPAIGN ILIKE '%Michigan%' OR CAMPAIGN ILIKE '%Central%' THEN 'MI'
                                    WHEN CAMPAIGN ILIKE '%California%' OR CAMPAIGN ILIKE '%Pacific%' THEN 'CA'
                                    WHEN CAMPAIGN ILIKE '%Call Back%' THEN 'Call Back'
                                    WHEN CAMPAIGN ILIKE '%CSD%' THEN 'IB - CSD'
                                    ELSE 'UN'
                                    END AS STATE_CAMPAIGN
                         FROM (
                              SELECT (TO_CHAR(TO_DATE(try_to_timestamp(FIVE9_CALL_LOG.TIMESTAMP)),
                                              'YYYY-MM-DD')) AS "five9_call_log.called_dt_no_tz_date"
                                   , (TO_CHAR(DATE_TRUNC('second', try_to_timestamp(FIVE9_CALL_LOG.TIMESTAMP)),
                                              'YYYY-MM-DD HH24:MI:SS')) AS "five9_call_log.called_dt_no_tz_time"
                                   , 1 * cast(right((CASE
                                                         WHEN LEN(FIVE9_CALL_LOG.CALL_TIME) = 7
                                                             THEN concat('0', FIVE9_CALL_LOG.CALL_TIME)
                                                         ELSE CALL_TIME
                                                         END), 2) AS INT) + 60 * cast(left(right((CASE
                                                                                                      WHEN LEN(FIVE9_CALL_LOG.CALL_TIME) = 7
                                                                                                          THEN concat('0', FIVE9_CALL_LOG.CALL_TIME)
                                                                                                      ELSE CALL_TIME
                                                                                                      END), 5),
                                                                                           2) AS INT) + 60 * 60 * cast(
                                      left((CASE
                                                WHEN LEN(FIVE9_CALL_LOG.CALL_TIME) = 7
                                                    THEN concat('0', FIVE9_CALL_LOG.CALL_TIME)
                                                ELSE CALL_TIME
                                                END), 2) AS INT) AS "five9_call_log.call_time"
                                   , 1 * cast(right((CASE
                                                         WHEN LEN(FIVE9_CALL_LOG.TALK_TIME) = 7
                                                             THEN concat('0', FIVE9_CALL_LOG.TALK_TIME)
                                                         ELSE FIVE9_CALL_LOG.TALK_TIME
                                                         END), 2) AS INT) + 60 * cast(left(right((CASE
                                                                                                      WHEN LEN(FIVE9_CALL_LOG.TALK_TIME) = 7
                                                                                                          THEN concat('0', FIVE9_CALL_LOG.TALK_TIME)
                                                                                                      ELSE FIVE9_CALL_LOG.TALK_TIME
                                                                                                      END), 5),
                                                                                           2) AS INT) +
                                     60 * 60 * cast(left((CASE
                                                              WHEN LEN(FIVE9_CALL_LOG.TALK_TIME) = 7
                                                                  THEN concat('0', FIVE9_CALL_LOG.TALK_TIME)
                                                              ELSE FIVE9_CALL_LOG.TALK_TIME
                                                              END), 2) AS INT) AS "five9_call_log.talk_time"
                                   , FIVE9_CALL_LOG.DNIS AS "five9_call_log.dnis"
                                   , FIVE9_CALL_LOG.ANI AS "five9_call_log.ani"
                                   , coalesce(FIVE9_CALL_LOG.LEADID, FIVE9_CALL_LOG.LEAD_ID,
                                              (nullif(FIVE9_CALL_LOG.CUSTOM_MATCHEDLEADID, 0)),
                                              (nullif(FIVE9_CALL_LOG.CUSTOM_VELOCIFYLEADID, 0))) AS "five9_call_log.lead_id"
                                   , FIVE9_CALL_LOG.PROSPECT_NAME AS "five9_call_log.prospect_name"
                                   , FIVE9_CALL_LOG.SALESFORCE_ID AS "five9_call_log.salesforce_id"
                                   , (CASE WHEN FIVE9_CALL_LOG.CONTACTED = 1 THEN 'Yes' ELSE 'No' END) AS "five9_call_log.contacted"
                                   , coalesce(FIVE9_CALL_LOG.CAMPAIGN_1, FIVE9_CALL_LOG.CAMPAIGN_2) AS "five9_call_log.campaign"
                                   , FIVE9_CALL_LOG.CAMPAIGN_TYPE AS "five9_call_log.campaign_type"
                                   , FIVE9_CALL_LOG.DISPOSITION AS "five9_call_log.disposition"
                                   , CASE
                                         WHEN (CASE
                                                   WHEN FIVE9_CALL_LOG.DISPOSITION LIKE 'Term Lost%' THEN 'Lost'
                                                   WHEN FIVE9_CALL_LOG.DISPOSITION = 'Term Call - Saved' THEN 'Saved'
                                                   WHEN FIVE9_CALL_LOG.DISPOSITION = 'Term Call - Undecided'
                                                       THEN 'Undecided'
                                                   ELSE 'Other'
                                                   END) = 'Saved' THEN 1
                                         WHEN (CASE
                                                   WHEN FIVE9_CALL_LOG.DISPOSITION LIKE 'Term Lost%' THEN 'Lost'
                                                   WHEN FIVE9_CALL_LOG.DISPOSITION = 'Term Call - Saved' THEN 'Saved'
                                                   WHEN FIVE9_CALL_LOG.DISPOSITION = 'Term Call - Undecided'
                                                       THEN 'Undecided'
                                                   ELSE 'Other'
                                                   END) = 'Lost' THEN 2
                                         WHEN (CASE
                                                   WHEN FIVE9_CALL_LOG.DISPOSITION LIKE 'Term Lost%' THEN 'Lost'
                                                   WHEN FIVE9_CALL_LOG.DISPOSITION = 'Term Call - Saved' THEN 'Saved'
                                                   WHEN FIVE9_CALL_LOG.DISPOSITION = 'Term Call - Undecided'
                                                       THEN 'Undecided'
                                                   ELSE 'Other'
                                                   END) = 'Undecided' THEN 3
                                         ELSE 4
                                         END AS "five9_call_log.disposition_group_sort_order"
                                   , CASE
                                         WHEN FIVE9_CALL_LOG.DISPOSITION LIKE 'Term Lost%' THEN 'Lost'
                                         WHEN FIVE9_CALL_LOG.DISPOSITION = 'Term Call - Saved' THEN 'Saved'
                                         WHEN FIVE9_CALL_LOG.DISPOSITION = 'Term Call - Undecided' THEN 'Undecided'
                                         ELSE 'Other'
                                         END
                                  AS "five9_call_log.disposition_group"
                                   , FIVE9_CALL_LOG.AGENT AS "five9_call_log.agent"
                                   , FIVE9_CALL_LOG.HANDLE_TIME_SECONDS

                              FROM REFINED_PROD.FIVE9_LEGACY.CALL_LOG AS FIVE9_CALL_LOG
                              WHERE (try_to_timestamp(FIVE9_CALL_LOG.TIMESTAMP)) >= (TO_TIMESTAMP('2021-04-05'))
                                AND (coalesce(FIVE9_CALL_LOG.CAMPAIGN_1, FIVE9_CALL_LOG.CAMPAIGN_2)) ILIKE
                                    '%above%'
                                AND (coalesce(FIVE9_CALL_LOG.CAMPAIGN_1, FIVE9_CALL_LOG.CAMPAIGN_2)) NOT ILIKE
                                    '%sales%'
                              GROUP BY (DATE_TRUNC('second', try_to_timestamp(FIVE9_CALL_LOG.TIMESTAMP)))
                                     , (TO_DATE(try_to_timestamp(FIVE9_CALL_LOG.TIMESTAMP))), 3, 4, 5, 6, 7, 8, 9, 10
                                     , 11, 12, 13, 14, 15, 16, 17

                              --- end looker query portion
                              )

                         --where salesforce_id is not null
                         )

        , SALESFORCE_QUERY AS (
                              SELECT P.PROGRAM_ID
                                   , P.PROGRAM_NAME
                                   , C.FIRST_NAME AS FIRST_NAME
                                   , C.LAST_NAME AS LAST_NAME
                                   , P.ENROLLED_DATE_CST AS ENROLLED_DATE
                                   , TOTAL_DEBT_INCLUDED AS DEBT_ENROLLED
                                   , COALESCE(P_L.LOAN_OFFER_EMAIL_SENT_C, PLOAN.IS_LOAN_OFFER_EMAIL_SENT_FLAG_C,
                                              FALSE) AS OFFER_EMAIL_SENT
                                   , REPLACE(regexp_replace(
                                                     COALESCE(CT.MOBILE_PHONE, CT.HOME_PHONE, CT.PHONE, CT.OTHER_PHONE,
                                                              P_L.CELL_PHONE_C, P_L.HOME_PHONE_C,
                                                              P_L.WORK_PHONE_C),
                                                     '[.,\/#!$%\^&\*:{}=\_`~()-]'), ' ', '') AS TELEPHONE_NUMBER
                              FROM CURATED_PROD.CRM.PROGRAM P
                                   LEFT JOIN REFINED_PROD.SALESFORCE.NU_DSE_PROGRAM_C AS P_L
                                             ON P.PROGRAM_NAME = P_L.NAME AND P_L.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
                                   LEFT JOIN CURATED_PROD.CRM.CLIENT C
                                             ON P.CLIENT_ID = C.CLIENT_ID AND C.IS_CURRENT_RECORD_FLAG
                                   LEFT JOIN REFINED_PROD.BEDROCK.PROGRAM_C P_B
                                             ON P_B.NAME = P.PROGRAM_NAME AND P_B.IS_DELETED = FALSE
                                   LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT ACT
                                             ON ACT.ID = P_B.ACCOUNT_ID_C AND ACT.IS_DELETED = FALSE
                                   LEFT JOIN REFINED_PROD.BEDROCK.ACCOUNT_CONTACT_RELATION ACR
                                             ON ACR.ACCOUNT_ID = ACT.ID AND ACR.RELATIONSHIP_C = 'Client' AND
                                                ACR.IS_DELETED = FALSE
                                   LEFT JOIN REFINED_PROD.BEDROCK.CONTACT CT
                                             ON ACR.CONTACT_ID = CT.ID AND CT.IS_DELETED = FALSE
                                   LEFT JOIN (
                                             SELECT *
                                             FROM REFINED_PROD.BEDROCK.PROGRAM_LOAN_C
                                             WHERE NOT IS_DELETED
                                                 QUALIFY RANK()
                                                                 OVER (PARTITION BY PROGRAM_ID_C ORDER BY CREATED_DATE_CST DESC, NAME DESC) =
                                                         1
                                             ) PLOAN ON PLOAN.PROGRAM_ID_C = P_B.ID
                              WHERE P.IS_CURRENT_RECORD_FLAG
                              )

     SELECT FN.*
          , SF.PROGRAM_ID
          , SF.PROGRAM_NAME
          , SF.FIRST_NAME
          , SF.LAST_NAME
          , SF.ENROLLED_DATE
          , DEBT_ENROLLED
          , OFFER_EMAIL_SENT
          , FCD.FIRST_PITCHED_DATE
          , iff(P.PROGRAM_NAME IS NULL, NULL, P.SERVICE_ENTITY_NAME) AS SERVICE_ENTITY_NAME
     FROM FIVE9_QUERY FN
          LEFT JOIN SALESFORCE_QUERY SF ON FN.SALESFORCE_ID = SF.PROGRAM_ID OR
                                           SF.TELEPHONE_NUMBER = CASE
                                                                     WHEN FN.CAMPAIGN_TYPE = 'Inbound' THEN FN.ANI::TEXT
                                                                     WHEN FN.CAMPAIGN_TYPE = 'Outbound' THEN FN.DNIS::TEXT
                                                                     ELSE '-1' -- Avoid matching on nulls
                                                                     END
          LEFT JOIN CURATED_PROD.CRM.PROGRAM P ON SF.PROGRAM_NAME = P.PROGRAM_NAME AND P.IS_CURRENT_RECORD_FLAG
          LEFT JOIN (
                    SELECT F.SALESFORCE_ID
                         , min(IFF(PITCHED = 1, CALLED_DATE, NULL)) AS FIRST_PITCHED_DATE
                    FROM FIVE9_QUERY F
                    WHERE TRUE
                    GROUP BY 1
                    ) AS FCD ON FN.SALESFORCE_ID = FCD.SALESFORCE_ID
     WHERE TRUE
     );