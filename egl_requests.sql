CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_requests` AS

--One license_record_id, One request date
SELECT *,
       IF(NOT LEAD(request_time) OVER(PARTITION BY license_record_id ORDER BY request_time) IS NULL,LEAD(request_time) OVER(PARTITION BY license_record_id ORDER BY request_time),'2300-01-01') AS lead_request_time,
       SUM(requestCount) OVER(PARTITION BY license_record_id ORDER BY request_time) AS running_request
FROM(
SELECT license_record_id ,
       DATE(updated_time) AS request_time,
       contactEmail,contactName,
       institutionName ,institutionType ,institutionCountry ,departmentName ,areaOfStudy ,position ,
       SUM(requestCount) AS requestCount
FROM(
SELECT * EXCEPT(requestCount,raw_json_payload),
       requestCount - IF (NOT LAG(requestCount) OVER(PARTITION BY license_record_id,id ORDER BY updated_time) IS NULL, LAG(requestCount) OVER(PARTITION BY license_record_id,id ORDER BY updated_time), 0) AS requestCount
FROM(
SELECT body.license_record_id,
       body.id,
       body.created_time,
       body.updated_time,
       body.raw_json_payload,
       CAST(json_extract (CAST(body.raw_json_payload AS STRING), '$.requestCount') AS int64) AS requestCount,
       
       FIRST_VALUE(TRIM(LOWER(json_extract (CAST(body.raw_json_payload AS STRING), '$.contactEmail')), 'Unknown')) OVER(PARTITION BY body.license_record_id ORDER BY body.updated_time) AS contactEmail,
       FIRST_VALUE(TRIM(LOWER(json_extract (CAST(body.raw_json_payload AS STRING), '$.contactName')), 'Unknown')) OVER(PARTITION BY body.license_record_id ORDER BY body.updated_time) AS contactName,
       FIRST_VALUE(TRIM(LOWER(json_extract (CAST(body.raw_json_payload AS STRING), '$.institutionName')), 'Unknown')) OVER(PARTITION BY body.license_record_id ORDER BY body.updated_time) AS institutionName,
       FIRST_VALUE(TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.institutionType'), 'Unknown' )) OVER(PARTITION BY body.license_record_id ORDER BY body.updated_time) AS institutionType ,
       FIRST_VALUE(TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.institutionCountry'), 'Unknown' )) OVER(PARTITION BY body.license_record_id ORDER BY body.updated_time) AS institutionCountry ,
       FIRST_VALUE(TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.departmentName'), 'Unknown' )) OVER(PARTITION BY body.license_record_id ORDER BY body.updated_time) AS departmentName ,
       FIRST_VALUE(TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.areaOfStudy'),'Unknown' )) OVER(PARTITION BY body.license_record_id ORDER BY body.updated_time) AS areaOfStudy ,
       FIRST_VALUE(TRIM(json_extract (CAST(body.raw_json_payload AS STRING), '$.position'), 'Unknown')) OVER(PARTITION BY body.license_record_id ORDER BY body.updated_time) AS position
FROM `unity-ai-data-prd.genesis_grantLicense.genesis_grantLicense_educationLicenseRequest_v1` 
WHERE submit_date IS NOT NULL
      AND body.deleted = false
      AND body.license_record_id NOT IN (SELECT DISTINCT license_record_id FROM `unity-other-learn-prd.reynafeng.invalid_license`)
GROUP BY 1,2,3,4,5,6
HAVING requestcount < 10000) AS A
) AS B
GROUP BY 1,2,3,4,5,6,7,8,9,10) AS C
