--Update Time: 3/22--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.academiclicense` AS

WITH machine AS (
  SELECT machineid,license_hash,sessionid
  FROM `unity-other-liveplatform-prd.ontology.ml_session_mapping`
  WHERE machine_count_per_session = 1
  GROUP BY 1,2,3
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
),

license_users AS (
SELECT user_id AS compliance_key,license,contactEmail AS email,institutionName AS institution,'Education Grant License' AS license_type,
       MIN(grant_time) AS grant_time,
       MAX(expire_time) AS expire_time
FROM `unity-other-learn-prd.reynafeng.egl_grant_license`
GROUP BY 1,2,3,4,5

UNION ALL

SELECT compliance_key,serialNumber AS license,email,SPLIT(email,'@')[OFFSET(0)] AS institution,'Student Plan' AS license_type,
       MIN(DATE(licnese_grant_time)) AS grant_time,
       MAX(DATE(licnese_expiration_time)) AS expire_time
FROM `unity-other-learn-prd.reynafeng.student_activation`
GROUP BY 1,2,3,4,5

UNION ALL

SELECT compliance_key,serialNumber AS license,email,SPLIT(email,'@')[OFFSET(0)] AS institution,'Educator Plan' AS license_type,
       MIN(DATE(licnese_grant_time)) AS grant_time,
       MAX(DATE(licnese_expiration_time)) AS expire_time
FROM `unity-other-learn-prd.reynafeng.educator_activation`
GROUP BY 1,2,3,4,5
)


SELECT A.*,B.machineid,B.country_code_most_freq,C.sessionid
FROM license_users A
JOIN user B ON A.compliance_key = B.compliance_key 
JOIN machine C ON C.machineid=B.machineid