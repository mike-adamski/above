USE ROLE ABOVE_STR_OPS_ROLE;

-- Calculate alarm time by agent + day using Talkdesk status data
TRUNCATE TABLE ABOVE_STR_OPS.ALARM_TIME_RAW;
INSERT INTO ABOVE_STR_OPS.ALARM_TIME_RAW
SELECT *
FROM ABOVE_STR_OPS.UPDATE_ALARM_TIME_RAW_V;

-- Output alarm time data
SELECT * FROM ABOVE_STR_OPS.WEEKLY_ALARM_TIME_UPDATE_V;