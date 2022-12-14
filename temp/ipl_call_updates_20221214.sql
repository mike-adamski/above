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
     , AGENT_SEGMENT.AGENT_USERNAME
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
FROM CURATED_PROD.CALL.CALL CALL
     LEFT JOIN CURATED_PROD.CALL.AGENT_SEGMENT AGENT_SEGMENT ON AGENT_SEGMENT.CALL_ID = CALL.CALL_ID AND AGENT_SEGMENT.DOMAIN_ID = CALL.DOMAIN_ID
     LEFT JOIN CURATED_PROD.CALL.AGENT AGENT ON AGENT.AGENT_KEY = AGENT_SEGMENT.AGENT_KEY AND AGENT.DOMAIN_ID = AGENT_SEGMENT.DOMAIN_ID AND AGENT.IS_CURRENT_RECORD_FLAG = TRUE
     LEFT JOIN CURATED_PROD.CRM.PROGRAM PROGRAM ON PROGRAM.PROGRAM_ID = CALL.PROGRAM_ID AND PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
WHERE CALL.CALL_CAMPAIGN ILIKE '%above%'
  AND CALL.CALL_CAMPAIGN NOT ILIKE '%sales%'
  AND CALL.START_DATE_TIME_CST >= (TO_TIMESTAMP('2021-04-05'))
    QUALIFY row_number() OVER (PARTITION BY CALL.CALL_ID ORDER BY CALL.START_DATE_TIME_CST) = 1
;
