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
     , LAST_DISPOSITION IN (
                            'Attempted Transfer',
                            'Attempted Transfer - Call Back Scheduled',
                            'Attempted Transfer - Transferred',
                            'Call Back Scheduled',
                            'Client Interested, unable to be transferred',
                            'Client Not Available',
                            'Client Not Available - Post Pitch',
                            'Client Not Available - Post Pitch - Call Back Scheduled',
                            'Client Not Available - Post Pitch - No Call back Scheduled',
                            'Client Not Available - Pre Pitch',
                            'Client Not Available - Pre Pitch - Call Back Scheduled',
                            'Client Not Available - Pre Pitch - No Call Back Scheduled',
                            'Client Not Interested in Lending Option',
                            'Contacted',
                            'Do Not Call',
                            'Not Interested',
                            'Not Interested - Post Pitch',
                            'Not Interested - Pre Pitch',
                            'Transferred to Lender',
                            'Transferred To 3rd Party'
    ) AS CONTACTED_FLAG
     , CASE WHEN lower(LAST_DISPOSITION) LIKE ('%not interested%') THEN TRUE ELSE FALSE END AS NOT_INTERESTED_FLAG
     , LAST_DISPOSITION IN (
                            'App Completed on Call',
                            'Post-Pitch Callback Scheduled-Transferred',
                            'Pre-Pitch Callback Scheduled-Transferred',
                            'Transferred To 3rd Party',
                            'Transferred to Lender') AS TRANSFERRED_TO_LENDER
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
     , IFF(WARM_TRANSFER.ABOVE_LENDING_TRANSFER_TIMESTAMP_CST IS NOT NULL AND WARM_TRANSFER2.TIMESTAMP_CST IS NOT NULL,
           least(WARM_TRANSFER.ABOVE_LENDING_TRANSFER_TIMESTAMP_CST, WARM_TRANSFER2.TIMESTAMP_CST),
           WARM_TRANSFER.ABOVE_LENDING_TRANSFER_TIMESTAMP_CST) AS ABOVE_LENDING_TRANSFER_TIMESTAMP_CST
FROM CURATED_PROD.CALL.CALL CALL
     LEFT JOIN CURATED_PROD.CRM.PROGRAM PROGRAM ON PROGRAM.PROGRAM_ID = CALL.PROGRAM_ID AND PROGRAM.IS_CURRENT_RECORD_FLAG = TRUE
     LEFT JOIN
     (
     SELECT DOMAIN_ID
          , CALL_ID
          , min(CALL_SEGMENT_START_DATE_TIME_CST) AS ABOVE_LENDING_TRANSFER_TIMESTAMP_CST
     FROM CURATED_PROD.CALL.CALL_SEGMENT
     WHERE CALL_SEGMENT_TYPE IN ('Warm Transfer', 'Transfer to 3d party', 'Conference') AND CALLED_PARTY = '8003014336'
     GROUP BY 1, 2
     ) AS WARM_TRANSFER ON WARM_TRANSFER.CALL_ID = CALL.CALL_ID AND WARM_TRANSFER.DOMAIN_ID = CALL.DOMAIN_ID
     LEFT JOIN (
               SELECT *
               FROM REFINED_PROD.FIVE9.CALL
               WHERE CALL_TYPE IN ('3rd party transfer', '3rd party conference')
                   QUALIFY row_number() OVER (PARTITION BY CALL_ID ORDER BY TIMESTAMP_CST) = 1
               ) AS WARM_TRANSFER2 ON WARM_TRANSFER2.CALL_ID = CALL.CALL_ID
     LEFT JOIN
     (
     SELECT DOMAIN_ID
          , CALL_ID
          , AGENT_KEY
     FROM CURATED_PROD.CALL.CALL_SEGMENT
         QUALIFY ROW_NUMBER() OVER (PARTITION BY DOMAIN_ID, CALL_ID
             ORDER BY CALL_SEGMENT_START_DATE_TIME_CST NULLS LAST,
                 TALK_TIME_IN_SECONDS DESC) = 1
     ) AS CALL_SEGMENT ON CALL_SEGMENT.DOMAIN_ID = CALL.DOMAIN_ID AND CALL_SEGMENT.CALL_ID = CALL.CALL_ID
     LEFT JOIN
     (
     SELECT DOMAIN_ID
          , CALL_ID
          , AGENT_KEY
     FROM CURATED_PROD.CALL.AGENT_SEGMENT
     WHERE CALL_ID IS NOT NULL
         QUALIFY ROW_NUMBER() OVER (PARTITION BY DOMAIN_ID, CALL_ID
             ORDER BY TALK_TIME_IN_SECONDS DESC NULLS LAST,
                 ON_CALL_TIME_IN_SECONDS DESC NULLS LAST,
                 CALL_TO_AGENT_TIME_IN_SECONDS DESC NULLS LAST,
                 AGENT_SEGMENT_START_DATE_TIME_CST) = 1
     ) AS AGENT_SEGMENT ON AGENT_SEGMENT.CALL_ID = CALL.CALL_ID AND AGENT_SEGMENT.DOMAIN_ID = CALL.DOMAIN_ID
     LEFT JOIN CURATED_PROD.CALL.AGENT AGENT ON AGENT.AGENT_KEY = COALESCE(CALL_SEGMENT.AGENT_KEY, AGENT_SEGMENT.AGENT_KEY)
WHERE CALL.CALL_CAMPAIGN ILIKE '%above%'
  AND CALL.CALL_CAMPAIGN NOT ILIKE '%sales%'
  AND CALL.START_DATE_TIME_CST >= (TO_TIMESTAMP('2021-04-05'))
;
