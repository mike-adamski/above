CREATE OR REPLACE VIEW SNO_SANDBOX.IPL.DIALER_DATA_V AS
WITH FIVE9_QUERY AS (
                    SELECT CALL_ID
                         , (TO_CHAR(TO_DATE(FIVE9_CALL_LOG.START_DATE_TIME_CST),
                                    'YYYY-MM-DD')) AS CALLED_DATE
                         , (TO_CHAR(DATE_TRUNC('second', FIVE9_CALL_LOG.START_DATE_TIME_CST),
                                    'YYYY-MM-DD HH24:MI:SS')) AS CALLED_TIME
                         , CALL_TIME_IN_SECONDS AS CALL_TIME_S
                         , TALK_TIME_IN_SECONDS AS TALK_TIME_S
                         , DNIS
                         , try_to_number(ANI) AS ANI
                         , null as LEAD_ID
                         , NULL AS PROSPECT_NAME
                         , PROGRAM_ID AS SALESFORCE_ID
                         , CASE WHEN FIVE9_CALL_LOG.CONTACTED_FLAG THEN 'Yes' ELSE 'No' END AS "five9_call_log.contacted"
                         , CALL_CAMPAIGN AS CAMPAIGN
                         , CALL_CAMPAIGN_TYPE AS CAMPAIGN_TYPE
                         , LAST_DISPOSITION AS DISPOSITION
                         , CASE
                               WHEN (CASE
                                         WHEN FIVE9_CALL_LOG.LAST_DISPOSITION LIKE 'Term Lost%' THEN 'Lost'
                                         WHEN FIVE9_CALL_LOG.LAST_DISPOSITION = 'Term Call - Saved' THEN 'Saved'
                                         WHEN FIVE9_CALL_LOG.LAST_DISPOSITION = 'Term Call - Undecided'
                                             THEN 'Undecided'
                                         ELSE 'Other'
                                         END) = 'Saved' THEN 1
                               WHEN (CASE
                                         WHEN FIVE9_CALL_LOG.LAST_DISPOSITION LIKE 'Term Lost%' THEN 'Lost'
                                         WHEN FIVE9_CALL_LOG.LAST_DISPOSITION = 'Term Call - Saved' THEN 'Saved'
                                         WHEN FIVE9_CALL_LOG.LAST_DISPOSITION = 'Term Call - Undecided'
                                             THEN 'Undecided'
                                         ELSE 'Other'
                                         END) = 'Lost' THEN 2
                               WHEN (CASE
                                         WHEN FIVE9_CALL_LOG.LAST_DISPOSITION LIKE 'Term Lost%' THEN 'Lost'
                                         WHEN FIVE9_CALL_LOG.LAST_DISPOSITION = 'Term Call - Saved' THEN 'Saved'
                                         WHEN FIVE9_CALL_LOG.LAST_DISPOSITION = 'Term Call - Undecided'
                                             THEN 'Undecided'
                                         ELSE 'Other'
                                         END) = 'Undecided' THEN 3
                               ELSE 4
                               END AS DISPOSITION_GROUP_SORT_ORDER
                         , CASE
                               WHEN FIVE9_CALL_LOG.LAST_DISPOSITION LIKE 'Term Lost%' THEN 'Lost'
                               WHEN FIVE9_CALL_LOG.LAST_DISPOSITION = 'Term Call - Saved' THEN 'Saved'
                               WHEN FIVE9_CALL_LOG.LAST_DISPOSITION = 'Term Call - Undecided' THEN 'Undecided'
                               ELSE 'Other'
                               END AS DISPOSITION_GROUP
                         , NULL AS AGENT
                         , HANDLE_TIME_IN_SECONDS AS HANDLE_TIME_SECONDS
                         , CASE
                               WHEN ANI_AREA_CODE IN (800, 888) THEN try_to_number(DNIS)
                               WHEN DNIS_AREA_CODE IN (800, 888) THEN try_to_number(ANI)
                               END AS CUSTOMER_PHONE_NUMBER
                         , CASE
                               WHEN (
                                            contains(lower(DISPOSITION), 'voicemail')
                                            OR contains(lower(DISPOSITION), 'voicemal')
                                            OR (contains(lower(DISPOSITION), 'not available') AND
                                                DISPOSITION NOT ILIKE '%pitch%')
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
                    FROM CURATED_PROD.CALL.CALL AS FIVE9_CALL_LOG
                    WHERE FIVE9_CALL_LOG.START_DATE_TIME_CST >= (TO_TIMESTAMP('2021-04-05'))
                      AND CALL_CAMPAIGN ILIKE '%above%'
                      AND CALL_CAMPAIGN NOT ILIKE '%sales%'
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
                                        ON P.PROGRAM_NAME = P_L.NAME AND
                                           P_L.BEDROCK_MIGRATION_TARGET_UUID_C IS NULL
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

SELECT FN.CALLED_DATE
     , FN.CALLED_TIME
     , FN.CALL_TIME_S
     , FN.TALK_TIME_S
     , FN.DNIS
     , FN.ANI
     , FN.LEAD_ID
     , FN.PROSPECT_NAME
     , FN.SALESFORCE_ID
     , FN.CAMPAIGN
     , FN.CAMPAIGN_TYPE
     , FN.DISPOSITION
     , FN.DISPOSITION_GROUP_SORT_ORDER
     , FN.DISPOSITION_GROUP
     , FN.AGENT
     , FN.CONTACTED
     , FN.PITCHED
     , FN.NOT_INTERESTED
     , FN.INTERESTED_UNABLE_TO_TRANSFER
     , FN.TRANSFERRED_TO_LENDER
     , FN.STATE_CAMPAIGN
     , coalesce(SF.PROGRAM_ID, SF2.PROGRAM_ID) AS PROGRAM_ID
     , coalesce(SF.PROGRAM_NAME, SF2.PROGRAM_NAME) AS PROGRAM_NAME
     , coalesce(SF.FIRST_NAME, SF2.FIRST_NAME) AS FIRST_NAME
     , coalesce(SF.LAST_NAME, SF2.LAST_NAME) AS LAST_NAME
     , coalesce(SF.ENROLLED_DATE, SF2.ENROLLED_DATE) AS ENROLLED_DATE
     , coalesce(SF.DEBT_ENROLLED, SF2.DEBT_ENROLLED) AS DEBT_ENROLLED
     , coalesce(SF.OFFER_EMAIL_SENT, SF2.OFFER_EMAIL_SENT) AS OFFER_EMAIL_SENT
     , FCD.FIRST_PITCHED_DATE
     , iff(P.PROGRAM_NAME IS NULL, NULL, P.SERVICE_ENTITY_NAME) AS SERVICE_ENTITY_NAME
FROM FIVE9_QUERY FN
     LEFT JOIN SALESFORCE_QUERY SF ON FN.SALESFORCE_ID = SF.PROGRAM_ID
     LEFT JOIN (
               SELECT *
               FROM SALESFORCE_QUERY SFQ
                   QUALIFY COUNT(DISTINCT PROGRAM_NAME) OVER (PARTITION BY TELEPHONE_NUMBER) = 1
               ) SF2
               ON FN.CUSTOMER_PHONE_NUMBER = TRY_TO_NUMBER(SF2.TELEPHONE_NUMBER)
     LEFT JOIN CURATED_PROD.CRM.PROGRAM P
               ON SF.PROGRAM_NAME = P.PROGRAM_NAME AND P.IS_CURRENT_RECORD_FLAG
     LEFT JOIN (
               SELECT F.SALESFORCE_ID
                    , MIN(IFF(PITCHED = 1, CALLED_DATE, NULL)) AS FIRST_PITCHED_DATE
               FROM FIVE9_QUERY F
               WHERE TRUE
               GROUP BY 1
               ) AS FCD ON FN.SALESFORCE_ID = FCD.SALESFORCE_ID
WHERE TRUE;