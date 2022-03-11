--Update Time: 3/10 8:24 PM--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.student_running` AS

WITH machine AS (
  SELECT machineid,license_hash,sessionid
  FROM `unity-other-liveplatform-prd.ontology.ml_session_mapping`
  GROUP BY 1,2,3
),

user AS (
  SELECT A.compliance_key,A.machineid,A.license_hash,A.country_code_most_freq
  FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` A
  JOIN `unity-other-learn-prd.reynafeng.student_activation` student ON A.compliance_key = student.compliance_key
  GROUP BY 1,2,3,4
)

SELECT *,
     DATE_TRUNC(session_start_date, week) AS session_start_week,
     TIMESTAMP_DIFF(session_start_time, LAG(session_start_time) OVER (PARTITION BY compliance_key ORDER BY session_start_time), hour) AS hrs_between_sessions
FROM(
SELECT compliance_key, sessionid, country_code_most_freq, email, license,
       MIN(submit_date) AS session_start_date,
       MIN(submit_time) AS session_start_time,
       MAX(user_duration) / 3600 as session_user_time_hrs
FROM(
SELECT A.* EXCEPT(compliance_key),student.email AS email,student.license,
       COALESCE(A.compliance_key,user.compliance_key) AS compliance_key,
       user.country_code_most_freq
FROM(
    SELECT raw.compliance_key,raw.context.pipeline_context.submit_time,submit_date,
           enrichments.geo_ip.country_code AS country_code,
           COALESCE(head.machineid, machine.machineid) AS machineid,
           COALESCE(head.license_hash, machine.license_hash) as license_hash,
           head.appid,head.localprojectid,
           head.platform,head.sessionid,head.userid,body.duration,body.focus_duration,body.user_duration
    FROM `unity-ai-data-prd.editor_analytics.editor_analytics_appRunning_v1` AS raw
    JOIN `unity-other-learn-prd.reynafeng.student_activation` student ON raw.compliance_key = student.compliance_key AND raw.context.pipeline_context.submit_time BETWEEN student.licnese_grant_time AND student.licnese_expiration_time
    LEFT JOIN machine ON head.sessionid = machine.sessionid AND head.machineid IS NULL
    WHERE COALESCE(head.license_hash, machine.machineid) IS NOT NULL
          AND submit_date IS NOT NULL
          AND body.user_duration < 1296000
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14) AS A
JOIN user ON user.machineid = A.machineid AND user.license_hash = A.license_hash
JOIN `unity-other-learn-prd.reynafeng.student_activation` student ON COALESCE(A.compliance_key,user.compliance_key) = student.compliance_key AND A.submit_time BETWEEN student.licnese_grant_time AND student.licnese_expiration_time
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17) AS A
GROUP BY 1,2,3,4,5) AS B