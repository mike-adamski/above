-- CREATE OR REPLACE SECURE VIEW CURATED_PROD.OUTBOUND_ABOVELENDING.VW_IPL_CALL
-- AS

SELECT CALL.START_DATE_TIME_CST
     , CALL.END_DATE_TIME_CST
     , CALL.CALL_TIME_IN_SECONDS
     , CALL.TALK_TIME_IN_SECONDS
     , CALL.CALL_ID
     , CALL.SESSION_ID
     , CALL.DNIS
     , CALL.ANI
     , CALL.LEAD_ID
     , CALL.LEAD_SOURCE
     , CALL.CALL_CAMPAIGN
     , CALL.CALL_CAMPAIGN_TYPE
     , CALL.LAST_DISPOSITION
     , AGENT.AGENT_ID
     , AGENT.AGENT_USERNAME
     , CASE
           WHEN (
                        contains(lower(CALL.LAST_DISPOSITION), 'voicemail')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'voicemal')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'not available')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'disconnected')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'not in service')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'wrong number')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'agent error')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'dead air')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'no answer')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'answering machine')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'declined')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'busy')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'unable to leave message')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'queue callback timeout')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'operator intercept')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'dial error')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'unauthorized 3rd party')
                        OR contains(lower(CALL.LAST_DISPOSITION), 'abandon')
                        OR CALL.LAST_DISPOSITION IS NULL
                    ) OR CALL.HANDLE_TIME_IN_SECONDS <= 30
               THEN FALSE
           ELSE TRUE
           END AS CONTACTED_FLAG
     , CASE WHEN lower(LAST_DISPOSITION) LIKE ('%not interested%') THEN TRUE ELSE FALSE END AS NOT_INTERESTED_FLAG
     , CASE
           WHEN CALL.LAST_DISPOSITION ILIKE '%transferred to lender%'
               OR CALL.LAST_DISPOSITION ILIKE '%app completed on call%'
               OR CALL.LAST_DISPOSITION ILIKE '%transferred to 3rd party%'
               THEN TRUE
           ELSE FALSE
           END AS TRANSFERRED_TO_LENDER
     , CALL.PROGRAM_ID
     , PROGRAM.PROGRAM_NAME
     , AGENT.AGENT_NAME AS AGENT
     , CASE
           WHEN CALL.LAST_DISPOSITION NOT ILIKE '%not interested%' AND CALL.LAST_DISPOSITION ILIKE '%interested%' THEN 1
           WHEN CALL.LAST_DISPOSITION ILIKE '%attempted to transfer%unavailable%' THEN 1
           WHEN CALL.LAST_DISPOSITION IN ('Attempted Transfer - Call Back Scheduled',
                                          'Client Not Available - Post Pitch - No Call back Scheduled',
                                          'Client Not Available - Post Pitch - Call Back Scheduled') THEN 1
           ELSE 0
           END AS INTERESTED_UNABLE_TO_TRANSFER
     , PROGRAM.SERVICE_ENTITY_NAME
     , CALL.LIST_NAME
     , AGENT.AGENT_GROUP
     , WARM_TRANSFER.ABOVE_TRANSFER_TIMESTAMP_CST
FROM CURATED_PROD.CALL.CALL CALL
     LEFT JOIN (
               SELECT CALL_ID, MIN(CALL_SEGMENT_START_DATE_TIME_CST) AS ABOVE_TRANSFER_TIMESTAMP_CST
               FROM CURATED_PROD.CALL.CALL_SEGMENT
               WHERE CALL_SEGMENT_RESULT = 'Warm Transfer'
                 AND CALLED_PARTY = '8003014336'
               GROUP BY CALL_ID
               ) WARM_TRANSFER ON CALL.CALL_ID = WARM_TRANSFER.CALL_ID
     LEFT JOIN (
               SELECT CALL_ID
                    , S.AGENT_KEY
               FROM CURATED_PROD.CALL.CALL_SEGMENT S
                   QUALIFY ROW_NUMBER() OVER (PARTITION BY CALL_ID ORDER BY CALL_SEGMENT_START_DATE_TIME_CST NULLS LAST, TALK_TIME_IN_SECONDS DESC) = 1
               ) CALL_SEGMENT ON CALL_SEGMENT.CALL_ID = CALL.CALL_ID
     LEFT JOIN (
               SELECT CALL_ID
                    , S.AGENT_KEY
               FROM CURATED_PROD.CALL.AGENT_SEGMENT S
               WHERE CALL_ID IS NOT NULL
                   QUALIFY row_number() OVER (PARTITION BY CALL_ID
                       ORDER BY TALK_TIME_IN_SECONDS DESC NULLS LAST
                           , ON_CALL_TIME_IN_SECONDS DESC NULLS LAST
                           , CALL_TO_AGENT_TIME_IN_SECONDS DESC NULLS LAST
                           , AGENT_SEGMENT_START_DATE_TIME_CST) = 1
               ) AGENT_SEGMENT ON AGENT_SEGMENT.CALL_ID = CALL.CALL_ID
     LEFT JOIN CURATED_PROD.CALL.AGENT AGENT ON AGENT.AGENT_KEY = COALESCE(CALL_SEGMENT.AGENT_KEY, AGENT_SEGMENT.AGENT_KEY)
     LEFT JOIN CURATED_PROD.CRM.PROGRAM PROGRAM ON PROGRAM.PROGRAM_ID = CALL.PROGRAM_ID AND PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
WHERE CALL.CALL_CAMPAIGN ILIKE '%above%'
  AND CALL.CALL_CAMPAIGN NOT ILIKE '%sales%'
  AND CALL.START_DATE_TIME_CST >= (TO_TIMESTAMP('2021-04-05'))
;
