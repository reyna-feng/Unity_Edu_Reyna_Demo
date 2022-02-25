CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_running` AS

WITH install AS (
    SELECT TO_BASE64(SHA256(CAST(install.ownerId AS string))) AS user_id,install.serialNumber AS license,
           request.institutionName,E.email
    FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations`AS install
    JOIN `unity-other-learn-prd.reynafeng.egl_records` AS record ON record.license=install.serialNumber
    JOIN `unity-other-learn-prd.reynafeng.egl_requests` AS request ON record.license_record_id=request.license_record_id
    LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr_restricted.user` AS E ON E.id=install.ownerId
    WHERE install.serialCategoryName='Edu Subscription Multi-install'
    GROUP BY 1,2,3,4
),

machine AS (
  SELECT machineid,license_hash,sessionid
  FROM `unity-other-liveplatform-prd.ontology.ml_session_mapping`
  WHERE machine_count_per_session = 1
  GROUP BY 1,2,3
),

user AS (
  SELECT compliance_key,machineid,license_hash
  FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` 
  GROUP BY 1,2,3
)

SELECT *,
     DATE_TRUNC(session_start_date, week) AS session_start_week,
     TIMESTAMP_DIFF(session_start_time, LAG(session_start_time) OVER (PARTITION BY compliance_key ORDER BY session_start_time), hour) AS hrs_between_sessions
FROM(
SELECT compliance_key, sessionid, email,
       MIN(submit_date) AS session_start_date,
       MIN(submit_time) AS session_start_time,
       MAX(user_duration) / 3600 as session_user_time_hrs
FROM(
SELECT A.* EXCEPT(compliance_key),
       COALESCE(A.compliance_key,user.compliance_key) AS compliance_key,
       install.email
FROM(
    SELECT compliance_key,context.pipeline_context.submit_time,submit_date,
           enrichments.geo_ip.country_code AS country_code,
           COALESCE(head.machineid, machine.machineid) AS machineid,
           COALESCE(head.license_hash, machine.license_hash) as license_hash,
           head.appid,head.localprojectid,
           head.platform,head.sessionid,head.userid,body.duration,body.focus_duration,body.user_duration
    FROM `unity-ai-data-prd.editor_analytics.editor_analytics_appRunning_v1` AS raw
    LEFT JOIN machine ON head.sessionid = machine.sessionid AND head.machineid IS NULL
    WHERE COALESCE(head.license_hash, machine.machineid) IS NOT NULL
          AND submit_date IS NOT NULL
          AND body.user_duration < 1296000
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14) AS A
JOIN user ON user.machineid = A.machineid AND user.license_hash = A.license_hash
JOIN install ON COALESCE(A.compliance_key,user.compliance_key)=install.user_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) AS A
GROUP BY 1,2,3) AS B