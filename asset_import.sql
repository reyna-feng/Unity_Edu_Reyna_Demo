--Update Time: 3/24
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.asset_import` AS
WITH machine AS (
SELECT machineid,license_hash,sessionid
FROM `unity-other-liveplatform-prd.ontology.ml_session_mapping`
WHERE machine_count_per_session = 1
),
user AS (
  SELECT compliance_key,machineid,license_hash,country_code_most_freq
  FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` A
  WHERE compliance_key IN (SELECT DISTINCT user_id FROM `unity-other-learn-prd.reynafeng.egl_grant_license`)
        OR
        compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.student_activation`)
        OR
        compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.educator_activation`)
  GROUP BY 1,2,3,4
)

SELECT   COALESCE(A.compliance_key, user.compliance_key) compliance_key,
         A.submit_date,asset_name_import_success,asset_name_import_fail
FROM (
SELECT compliance_key,submit_date,head.license_hash,head.license_kind,
       head.machineid,head.sessionid,head.userid,
       IF(body.package_import_status = 1, 1, 0) AS asset_import_success,
       IF(body.package_import_status = 3, 1, 0) AS asset_import_fail,
       IF(body.package_import_status = 1, body.package_name, NULL) AS asset_name_import_success,
       IF(body.package_import_status = 3, body.package_name, NULL) AS asset_name_import_fail
FROM `unity-ai-data-prd.editor_analytics.editor_analytics_assetImportStatus_v1` raw
WHERE submit_date IS NOT NULL
  ) A
LEFT JOIN machine ON A.machineid IS NULL AND A.sessionid = machine.sessionid
RIGHT JOIN user ON A.compliance_key = user.compliance_key AND user.machineid=COALESCE(A.machineid, machine.machineid)
WHERE COALESCE(A.machineid, machine.machineid) IS NOT NULL AND asset_import_success+asset_import_fail>0