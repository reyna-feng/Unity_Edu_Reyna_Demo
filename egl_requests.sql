CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_requests` AS

SELECT license_record_id,MIN(created_time) AS created_time,
       MIN(submit_date) AS submit_date,
       MIN(updated_time) AS updated_time,
       status,
       SUM(requestCount) AS requestCount,
       institutionName ,institutionType ,institutionCountry ,departmentName ,areaOfStudy ,position 
FROM(
SELECT body.license_record_id,
       body.created_time,submit_date,body.updated_time,
       body.id,body.status,body.deleted,
       CAST(json_extract (CAST(body.raw_json_payload AS STRING), '$.requestCount') AS int64) AS requestCount,
       FIRST_VALUE(TRIM(LOWER(json_extract (CAST(body.raw_json_payload AS STRING), '$.institutionName')), '"')) OVER(PARTITION BY body.license_record_id ORDER BY body.created_time) AS institutionName,
       FIRST_VALUE(TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.institutionType'), '"' )) OVER(PARTITION BY body.license_record_id ORDER BY body.created_time) AS institutionType ,
       FIRST_VALUE(TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.institutionCountry'), '"' )) OVER(PARTITION BY body.license_record_id ORDER BY body.created_time) AS institutionCountry ,
       FIRST_VALUE(TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.departmentName'), '"' )) OVER(PARTITION BY body.license_record_id ORDER BY body.created_time) AS departmentName ,
       FIRST_VALUE(TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.areaOfStudy'),'"' )) OVER(PARTITION BY body.license_record_id ORDER BY body.created_time) AS areaOfStudy ,
       FIRST_VALUE(TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.position'), '"')) OVER(PARTITION BY body.license_record_id ORDER BY body.created_time) AS position ,
       body.raw_json_payload as payload,
       RANK() OVER(PARTITION BY body.id ORDER BY body.updated_time DESC) AS rnk,
       RANK() OVER(PARTITION BY body.license_record_id, body.id ORDER BY submit_date DESC) AS rnk_num
FROM `unity-ai-data-prd.genesis_grantLicense.genesis_grantLicense_educationLicenseRequest_v1` 
WHERE submit_date IS NOT NULL
      AND body.deleted = false
      AND body.license_record_id NOT IN (SELECT DISTINCT license_record_id FROM `unity-other-learn-prd.reynafeng.invalid_license`)
GROUP BY 1,2,3,4,5,6,7,8,15) AS A
WHERE rnk = 1 AND requestcount < 10000 AND rnk_num=1
GROUP BY 1,5,7,8,9,10,11,12