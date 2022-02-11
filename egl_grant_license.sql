CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_grant_license` AS

WITH profile AS (
     SELECT request.license_record_id,record.license,
            IF(NOT record.user_id IS NULL, record.user_id, install.user_id) AS user_id
     FROM `unity-other-learn-prd.reynafeng.egl_requests` AS request
     LEFT JOIN `unity-other-learn-prd.reynafeng.egl_records` AS record ON record.license_record_id =request.license_record_id AND grant_time=request_time
     LEFT JOIN `unity-other-learn-prd.reynafeng.egl_installs` AS install ON install.license=record.license AND (install_date BETWEEN request_time AND lead_request_time)
     WHERE record.license IS NOT NULL AND IF(NOT record.user_id IS NULL, record.user_id, install.user_id) IS NOT NULL
     GROUP BY 1,2,3
)

SELECT * EXCEPT(requestCount,grantCount,is_renew),
       IF(rnk>1,0,requestCount) AS requestCount,
       IF(rnk>1,0,grantCount) AS grantCount,
       SUM(IF(rnk>1,0,requestCount)) OVER(PARTITION BY license_record_id ORDER BY request_time) AS running_request,
       SUM(IF(rnk>1,0,grantCount)) OVER(PARTITION BY license_record_id ORDER BY request_time) AS running_grant,
       SUM(installs) OVER(PARTITION BY license_record_id ORDER BY install_time) AS running_installs,
       IF(IF(rnk>1,0,requestCount)>0,true,false) AS is_grant,
       IF(installs>0,true,false) AS is_install,
       CASE WHEN rnk=1 AND is_renew = true THEN true ELSE false END AS is_renew,
       IF(SUM(grantCount) OVER(PARTITION BY license_record_id)>0,'Approved','Pending') AS status,
FROM(
     SELECT 
            request.license_record_id,
            request_time,
            contactEmail,contactName,institutionName,institutionType,institutionCountry,
            departmentName,areaOfStudy,position,
            requestCount,
            IF(NOT profile.license IS NULL, profile.license ,record.license) AS license,
            grant_time,
            IF(grantCount IS NULL, 0, grantCount) AS grantCount,
            IF(is_renew = true, true, false) AS is_renew,
            IF(NOT install_date IS NULL, install_date, request_time) AS install_time,
            IF(NOT profile.user_id IS NULL, profile.user_id ,record.user_id) AS user_id,
            IF(NOT installs IS NULL,installs, 0) AS installs,
            ROW_NUMBER() OVER(PARTITION BY request.license_record_id,request_time ORDER BY request_time) AS rnk
     FROM `unity-other-learn-prd.reynafeng.egl_requests` AS request
     LEFT JOIN `unity-other-learn-prd.reynafeng.egl_records` AS record ON record.license_record_id =request.license_record_id AND grant_time=request_time
     LEFT JOIN `unity-other-learn-prd.reynafeng.egl_installs` AS install ON install.license=record.license AND (install_date >= request_time AND install_date < lead_request_time)
     LEFT JOIN profile ON profile.license_record_id=request.license_record_id
) AS A 
ORDER BY license_record_id , request_time ,install_time, rnk ASC
