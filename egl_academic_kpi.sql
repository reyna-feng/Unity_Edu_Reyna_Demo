 --KPI Report--
--Update Time: 3/9 3:42 PM--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_academic_kpi` AS 

WITH JAPAN AS(
SELECT TO_BASE64(SHA256(CAST(ownerId AS STRING))) AS user_id,
       serialNumber AS license,
       DATE(ulfCreatedTime) AS install_date,
       COUNT(DISTINCT hwidUlf) AS installs
FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations` 
WHERE serialCategoryCode='EXTERNAL' AND countryOfResidence='JP' AND serialCategoryDescription='External Developer Version' AND serialCategoryName='External' AND isTest != true
GROUP BY 1,2,3),

egl_grant_license AS(
WITH profile AS (
     SELECT request.license_record_id,record.license,
            IF(NOT record.user_id IS NULL, record.user_id, install.user_id) AS user_id
     FROM `unity-other-learn-prd.reynafeng.egl_requests` AS request
     LEFT JOIN `unity-other-learn-prd.reynafeng.egl_records` AS record ON record.license_record_id =request.license_record_id AND grant_time=request_time
     LEFT JOIN (
     SELECT TO_BASE64(SHA256(CAST(ownerId AS STRING))) AS user_id,
            serialNumber AS license,
            DATE(ulfCreatedTime) AS install_date,
            COUNT(DISTINCT hwidUlf) AS installs
     FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations` 
     WHERE serialCategoryName='Edu Subscription Multi-install' AND isTest != true
     GROUP BY 1,2,3
     ) AS install ON install.license=record.license AND (install_date BETWEEN request_time AND lead_request_time)
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
            grant_time,expire_time,
            IF(grantCount IS NULL, 0, grantCount) AS grantCount,
            IF(is_renew = true, true, false) AS is_renew,
            IF(NOT install_date IS NULL, install_date, request_time) AS install_time,
            IF(NOT profile.user_id IS NULL, profile.user_id ,record.user_id) AS user_id,
            IF(NOT installs IS NULL,installs, 0) AS installs,
            ROW_NUMBER() OVER(PARTITION BY request.license_record_id,request_time ORDER BY request_time) AS rnk
     FROM `unity-other-learn-prd.reynafeng.egl_requests` AS request
     LEFT JOIN `unity-other-learn-prd.reynafeng.egl_records` AS record ON record.license_record_id =request.license_record_id AND grant_time=request_time
     LEFT JOIN (
     SELECT TO_BASE64(SHA256(CAST(ownerId AS STRING))) AS user_id,
            serialNumber AS license,
            DATE(ulfCreatedTime) AS install_date,
            COUNT(DISTINCT hwidUlf) AS installs
     FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations` 
     WHERE serialCategoryName='Edu Subscription Multi-install' AND isTest != true
     GROUP BY 1,2,3
     ) AS install ON install.license=record.license AND (install_date >= request_time AND install_date < lead_request_time)
     LEFT JOIN profile ON profile.license_record_id=request.license_record_id
) AS A 
ORDER BY license_record_id , request_time ,install_time, rnk ASC
),

installs AS(
SELECT install_month,num_install,num_expire,
       SUM(num_install) OVER(ORDER BY install_month) - SUM(num_expire) OVER(ORDER BY install_month) AS running_installs
FROM(
SELECT DATE_TRUNC(install_time,month) AS install_month,
       SUM(installs) AS num_install,
       SUM(expires) AS num_expire
FROM(
  SELECT IF(NOT install_time IS NULL, install_time, expire_time) AS install_time,
         IF(NOT installs IS NULL, installs, 0) AS installs,
         IF(NOT expires IS NULL, expires, 0) AS expires
  FROM(
    SELECT install_time,SUM(installs) AS installs
    FROM(
      SELECT install_time, SUM(installs) AS installs
      FROM egl_grant_license
      WHERE status='Approved' 
      GROUP BY 1
      UNION ALL
      SELECT install_date AS install_time, SUM(installs) AS installs
      FROM JAPAN
      GROUP BY 1) AS B
    GROUP BY 1
  ) AS A
  FULL OUTER JOIN (
    SELECT expire_time,SUM(expires) AS expires
    FROM(
      SELECT license,expire_time,MAX(running_installs) AS expires
      FROM(
          SELECT license, expire_time, running_installs,
                 DENSE_RANK() OVER(PARTITION BY license ORDER BY expire_time DESC) AS rnk
          FROM egl_grant_license
          WHERE status='Approved') AS A 
          WHERE rnk=1
          GROUP BY 1,2) AS B
      GROUP BY 1
  ) AS B ON A.install_time=B.expire_time
) AS C
GROUP BY 1
) AS D
),

startm AS(
SELECT start_month,COUNT(DISTINCT license_record_id) AS num_start,COUNT(DISTINCT institutionName) AS num_institution_start
FROM(
SELECT license_record_id ,license ,institutionName,
       MIN(DATE_TRUNC(request_time, month)) OVER(PARTITION BY license_record_id) AS start_month,
       MAX(DATE_TRUNC(expire_time, month)) OVER(PARTITION BY license_record_id) AS end_month
FROM egl_grant_license
WHERE status='Approved') AS A
GROUP BY 1
ORDER BY 1
),

endm AS(
SELECT end_month,COUNT(DISTINCT license_record_id) AS num_end,COUNT(DISTINCT institutionName) AS num_institution_end
FROM(
SELECT license_record_id ,license ,institutionName,
       MIN(DATE_TRUNC(request_time, month)) OVER(PARTITION BY license_record_id) AS start_month,
       MAX(DATE_TRUNC(expire_time, month)) OVER(PARTITION BY license_record_id) AS end_month
FROM egl_grant_license
WHERE status='Approved') AS A
GROUP BY 1
ORDER BY 1
),

institution AS(
SELECT *,
       ROW_NUMBER() OVER(ORDER BY report_month) AS rnk,
       SUM(num_start) OVER(ORDER BY report_month) - SUM(num_end) OVER(ORDER BY report_month) AS running_balance,
       SUM(num_institution_start) OVER(ORDER BY report_month) - SUM(num_institution_end) OVER(ORDER BY report_month) AS running_institution_balance
FROM(       
SELECT COALESCE(start_month,end_month,install_month) AS report_month,
       IF(NOT num_start IS NULL, num_start,0) AS num_start,
       IF(NOT num_end IS NULL, num_end,0) AS num_end,
       IF(NOT num_institution_start IS NULL, num_institution_start,0) AS num_institution_start,
       IF(NOT num_institution_end IS NULL, num_institution_end,0) AS num_institution_end,
       IF(NOT installs.num_install IS NULL, installs.num_install,0) AS num_install,
       IF(NOT installs.num_expire IS NULL, installs.num_expire,0) AS num_expire,
       IF(NOT installs.running_installs IS NULL, installs.running_installs,0) AS running_installs
FROM startm
FULL JOIN endm ON startm.start_month = endm.end_month
FULL JOIN installs ON COALESCE(startm.start_month,endm.end_month) = installs.install_month
) AS A
ORDER BY report_month
),

monthly_student AS(
SELECT *,
       AVG(monthly_users) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_average,
       AVG(monthly_institution) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_institution_average,
       SUM(monthly_users) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_sum,
       SUM(monthly_institution) OVER(ORDER BY rnk RANGE BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_institution_sum
FROM(
SELECT visit_month,monthly_users,monthly_institution,
       ROW_NUMBER() OVER(ORDER BY visit_month) AS rnk
FROM `unity-other-learn-prd.reynafeng.egl_mau`
GROUP BY 1,2,3
ORDER BY 1) AS A
)

SELECT *,
       LAG(egl_license_start) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) AS lag_egl_license_start,
       LAG(egl_school_start) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month)) AS lag_egl_school_start,
       IF(egl_license_end=0, NULL, 1-egl_license_end/LAG(egl_license_start) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month))) AS renew_license,
       IF(egl_school_end=0, NULL, 1-egl_school_end/LAG(egl_school_start) OVER(PARTITION BY EXTRACT(MONTH FROM visit_month) ORDER BY EXTRACT(YEAR FROM visit_month))) AS renew_school
FROM(
SELECT A.visit_month ,A.monthly_users ,A.monthly_institution ,
       IF(visit_month = DATE_TRUNC(CURRENT_DATE(),month),true,false) AS current_month,
       A.rolling_average AS monthly_rolling_students,A.rolling_institution_average AS monthly_rolling_schools,
       
       A.rolling_sum AS monthly_rolling_students_sum,A.rolling_institution_sum AS monthly_rolling_schools_sum,
       IF(NOT num_start IS NULL, num_start, 0) AS egl_license_start,
       IF(NOT num_end IS NULL, num_end, 0) AS egl_license_end,
       IF(NOT num_institution_start IS NULL, num_institution_start, 0) AS egl_school_start,
       IF(NOT num_institution_end IS NULL, num_institution_end, 0) AS egl_school_end,
       IF(NOT running_balance IS NULL, running_balance,0) AS egl_license_balance,
       IF(NOT running_institution_balance IS NULL, running_institution_balance,0) AS egl_school_balance,
       IF(NOT B.num_install IS NULL, B.num_install,0) AS num_install,
       IF(NOT B.num_expire IS NULL, B.num_expire,0) AS num_expire,
       IF(NOT B.running_installs IS NULL, B.running_installs,0) AS monthly_rolling_seats_sum,
       3 AS activation_multiplier
FROM monthly_student AS A
LEFT JOIN institution AS B ON A.visit_month = B.report_month) AS A