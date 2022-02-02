CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_records` AS

WITH records AS(
SELECT body.license_record_id,body.license,body.installation_limit,body.grant_time,body.expire_time,
       body.updated_time,submit_date,
       RANK() OVER(PARTITION BY body.license_record_id, submit_date ORDER BY body.updated_time DESC) AS rnk
FROM `unity-ai-data-prd.genesis_grantLicense.genesis_grantLicense_educationLicenseRecord_v1` 
WHERE submit_date IS NOT NULL
      AND body.deleted = false
      AND body.license_record_id NOT IN (SELECT DISTINCT license_record_id FROM `unity-other-learn-prd.reynafeng.invalid_license`)
      AND body.license IS NOT NULL
GROUP BY 1,2,3,4,5,6,7
)
SELECT *
FROM(
SELECT records.* EXCEPT(rnk),
       RANK() OVER(PARTITION BY license_record_id, updated_time ORDER BY submit_date DESC) AS rnk_num
FROM records 
WHERE rnk = 1) AS A 
WHERE rnk_num = 1
