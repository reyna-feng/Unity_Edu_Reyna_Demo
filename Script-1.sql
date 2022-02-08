CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_grant_license` AS

WITH record AS(
SELECT *
FROM (
SELECT *,
       RANK() OVER(PARTITION BY license_record_id, user_id ORDER BY submit_date DESC) AS rnk_num
FROM(
SELECT body.license_record_id,body.license,body.installation_limit,body.grant_time,body.expire_time,
       body.updated_time,submit_date,
       IF(compliance_key IS NULL,body.user_id,compliance_key) AS user_id,
       RANK() OVER(PARTITION BY body.license_record_id, submit_date ORDER BY body.updated_time DESC) AS rnk
FROM `unity-ai-data-prd.genesis_grantLicense.genesis_grantLicense_educationLicenseRecord_v1` 
WHERE submit_date IS NOT NULL
      AND body.deleted = false
      AND body.license_record_id NOT IN (SELECT DISTINCT license_record_id FROM `unity-other-learn-prd.reynafeng.invalid_license`)
      AND body.license IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8) AS A
WHERE rnk = 1) AS B
WHERE rnk_num=1
),
request AS(
SELECT *
FROM(
SELECT body.license_record_id,
       body.created_time,submit_date,body.updated_time,
       body.id,body.status,body.deleted,
       CAST(json_extract (CAST(body.raw_json_payload AS STRING), '$.requestCount') AS int64) AS requestCount,
       TRIM(LOWER(json_extract (CAST(body.raw_json_payload AS STRING), '$.institutionName')), '"') AS institutionName,
       TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.institutionType'), '"' ) AS institutionType ,
       TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.institutionCountry'), '"' ) AS institutionCountry ,
       TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.departmentName'), '"' ) AS departmentName ,
       TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.areaOfStudy'),'"' ) AS areaOfStudy ,
       TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.position'), '"') AS position ,
       body.raw_json_payload as payload,
       RANK() OVER(PARTITION BY body.id ORDER BY body.updated_time DESC) AS rnk,
       RANK() OVER(PARTITION BY body.license_record_id, body.id ORDER BY submit_date DESC) AS rnk_num
FROM `unity-ai-data-prd.genesis_grantLicense.genesis_grantLicense_educationLicenseRequest_v1` 
WHERE submit_date IS NOT NULL
      AND body.deleted = false
      AND body.license_record_id NOT IN (SELECT DISTINCT license_record_id FROM `unity-other-learn-prd.reynafeng.invalid_license`)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15) AS A
WHERE rnk = 1 AND requestcount < 10000 AND rnk_num=1
),

install AS (
SELECT TO_BASE64(SHA256(CAST(ownerId AS STRING))) AS user_id,
       serialNumber AS license,MIN(DATE(initialActivationDate)) AS install_date,
       COUNT(1) AS installs
FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations` 
WHERE serialCategoryName='Edu Subscription Multi-install' 
      AND isDeleted != true 
GROUP BY 1,2),

grant_lic AS ( 
     SELECT request.*,
            IF(NOT record.user_id IS NULL, record.user_id, CAST(install.user_id AS STRING)) AS user_id,
            DATE(grant_time) AS granted_date,
            IF(NOT record.installation_limit IS NULL, record.installation_limit,0) AS granted,
            record.license,install.install_date,
            IF(NOT installs IS NULL,installs, 0) AS installs 
     FROM request
     LEFT JOIN record ON record.license_record_id =request.license_record_id 
     LEFT JOIN install on install.license=record.license)
    
SELECT created_time,granted_date,install_date,
       license_record_id,institutionName,institutionType,institutionCountry,departmentName,
       areaOfStudy,status,position,
       user_id,
       license,requestCount,installs,
       granted,
       IF(status='PENDING' OR status='REJECTED', FALSE, TRUE) AS is_granted,
       IF(installs>0,TRUE, FALSE) AS is_installed
FROM grant_lic 
GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17