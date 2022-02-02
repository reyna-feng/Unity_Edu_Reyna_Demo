CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_requests` AS

WITH request AS(
SELECT body.license_record_id,body.id,submit_date,body.updated_time,body.status,body.deleted,
       CAST(json_extract (CAST(body.raw_json_payload AS STRING), '$.requestCount') AS int64) requestCount,
       TRIM(LOWER(json_extract (CAST(body.raw_json_payload AS STRING), '$.institutionName')), '"') AS institutionName,
       RANK() OVER(PARTITION BY body.id ORDER BY body.updated_time DESC) rnk  
FROM `unity-ai-data-prd.genesis_grantLicense.genesis_grantLicense_educationLicenseRequest_v1` 
WHERE submit_date IS NOT NULL
      AND body.deleted = false
      AND body.license_record_id NOT IN (SELECT DISTINCT license_record_id FROM `unity-other-learn-prd.reynafeng.invalid_license`)
GROUP BY 1,2,3,4,5,6,7,8
)

SELECT *
FROM(
SELECT request.* EXCEPT(rnk),
       RANK() OVER(PARTITION BY license_record_id, id ORDER BY submit_date DESC) AS rnk_num
FROM request
WHERE rnk = 1 AND requestcount < 10000 ) AS A
WHERE rnk_num = 1
