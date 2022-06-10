--Update Time: 6/8--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_records` AS

--One license_record_id , One grant_time 
SELECT *,
       IF(days_diff>0,DATE_DIFF(grant_time,lag_expire_time,DAY),NULL) AS days_renew,
       IF(days_diff>0,true,false) AS is_renew,
       IF(NOT LEAD(grant_time) OVER(PARTITION BY license ORDER BY grant_time) IS NULL,LEAD(grant_time) OVER(PARTITION BY license ORDER BY grant_time),'2300-01-01') AS lead_grant_time,
       installation_limit - IF (NOT LAG(installation_limit) OVER(PARTITION BY license ORDER BY grant_time) IS NULL, LAG(installation_limit) OVER(PARTITION BY license ORDER BY grant_time), 0) AS grantCount
FROM(
SELECT license_record_id ,license ,
       first_grant_time ,
       DATE(updated_time) AS grant_time,
       MAX(installation_limit) AS installation_limit ,
       user_id , real_user_id,
       MAX(expire) AS expire_time ,
       MAX(lag_expire) AS lag_expire_time ,
       SUM(days_diff) AS days_diff  
FROM(
SELECT *,
       DATE_DIFF(expire,lag_expire,DAY) AS days_diff
FROM(
SELECT body.license_record_id,
       body.license,
       body.installation_limit,
       body.grant_time AS first_grant_time,
       body.expire_time,
       body.updated_time,
       compliance_key AS user_id,
       body.user_id AS real_user_id,
       DATE(body.expire_time) as expire,
       IF(NOT DATE(LAG(body.expire_time) OVER(PARTITION BY body.license ORDER BY body.updated_time)) IS NULL, DATE(LAG(body.expire_time) OVER(PARTITION BY body.license ORDER BY body.updated_time)), DATE(body.expire_time)) AS lag_expire
FROM `unity-ai-data-prd.genesis_grantLicense.genesis_grantLicense_educationLicenseRecord_v1` 
WHERE submit_date IS NOT NULL
      AND body.deleted = false
      AND body.license_record_id NOT IN (SELECT DISTINCT license_record_id FROM `unity-other-learn-prd.reynafeng.invalid_license`)
      AND body.license IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY body.license_record_id ,body.updated_time ASC) AS A
) AS B
GROUP BY 1,2,3,4,6,7) AS C
ORDER BY license , grant_time 