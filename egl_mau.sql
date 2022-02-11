CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_mau` AS

WITH profile AS (
     SELECT request.license_record_id,record.license,
            IF(NOT record.user_id IS NULL, record.user_id, install.user_id) AS user_id,
            request.institutionName
     FROM `unity-other-learn-prd.reynafeng.egl_requests` AS request
     LEFT JOIN `unity-other-learn-prd.reynafeng.egl_records` AS record ON record.license_record_id =request.license_record_id AND grant_time=request_time
     LEFT JOIN `unity-other-learn-prd.reynafeng.egl_installs` AS install ON install.license=record.license AND DATE(install_date) >= request_time AND DATE(install_date)<lead_request_time
     WHERE record.license IS NOT NULL AND IF(NOT record.user_id IS NULL, record.user_id, install.user_id) IS NOT NULL
     GROUP BY 1,2,3,4
)

SELECT visit_date,visit_month,editor_license,
       COUNT(compliance_key) AS daily_users_type,
       COUNT(institutionName) AS daily_institution_type,
       monthly_users,daily_users,
       monthly_institution,daily_institution ,
       daily_users / monthly_users AS retention
FROM(
SELECT DATE(login_date) AS visit_date,
       DATE_TRUNC(login_date, month) AS visit_month,
       compliance_key,editor_license,institutionName,
       COUNT(compliance_key) OVER(PARTITION BY DATE_TRUNC(login_date, month)) AS monthly_users,
       COUNT(compliance_key) OVER(PARTITION BY DATE(login_date)) AS daily_users,
       COUNT(institutionName) OVER(PARTITION BY DATE_TRUNC(login_date, month)) AS monthly_institution,
       COUNT(institutionName) OVER(PARTITION BY DATE(login_date)) AS daily_institution
FROM `unity-other-liveplatform-prd.ontology.ckey_daily_login` AS editor_act
JOIN profile ON editor_act.compliance_key = profile.user_id
GROUP BY 1,2,3,4,5,login_date) AS A
GROUP BY 1,2,3,6,7,8,9,10
ORDER BY 1 DESC, 2 DESC