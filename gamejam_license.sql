CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.gamejam_license` AS

WITH license_users AS (
SELECT compliance_key,license,email,institution,license_type,
       MIN(grant_time) AS grant_time,MAX(expire_time) AS expire_time
FROM(
SELECT TO_BASE64(SHA256(CAST(ownerId AS STRING))) AS compliance_key,
       A.serialNumber AS license,
       E.email,SPLIT(E.email,'@')[safe_ordinal(2)] AS institution,
       'Game Jam' AS license_type,
       DATE(ulfCreatedTime) AS grant_time,
       DATE_ADD(DATE(ulfCreatedTime), INTERVAL 1 YEAR) AS expire_time
FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations` A
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr_restricted.user` AS E ON TO_BASE64(SHA256(CAST(E.id AS STRING)))=TO_BASE64(SHA256(CAST(ownerId AS STRING)))
WHERE A.serialCategoryName='Game Jam' 
      AND A.isDeleted != true AND A.isTest != true
GROUP BY 1,2,3,4,5,6,7) AS A
GROUP BY 1,2,3,4,5
),

machine AS (
  SELECT machineid,license_hash,sessionid
  FROM `unity-other-liveplatform-prd.ontology.ml_session_mapping`
  WHERE machine_count_per_session = 1
  GROUP BY 1,2,3
),

user AS (
  SELECT compliance_key,machineid,license_hash,country_code_most_freq
  FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` A
  WHERE compliance_key IN (SELECT DISTINCT compliance_key FROM license_users)
  GROUP BY 1,2,3,4
)

SELECT A.*,B.machineid,B.country_code_most_freq,C.sessionid
FROM license_users A
JOIN user B ON A.compliance_key = B.compliance_key 
JOIN machine C ON C.machineid=B.machineid