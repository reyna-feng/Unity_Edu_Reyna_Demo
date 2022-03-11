--Update Time: 3/9 4:11 PM--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_mau` AS

WITH install AS (
    SELECT TO_BASE64(SHA256(CAST(install.ownerId AS string))) AS user_id,install.serialNumber AS license,
           request.institutionName
    FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations`AS install
    JOIN `unity-other-learn-prd.reynafeng.egl_records` AS record ON record.license=install.serialNumber
    JOIN `unity-other-learn-prd.reynafeng.egl_requests` AS request ON record.license_record_id=request.license_record_id
    WHERE install.serialCategoryName='Edu Subscription Multi-install'
    GROUP BY 1,2,3
),

user AS (
  SELECT compliance_key,machineid,license_hash
  FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` 
  GROUP BY 1,2,3
)


SELECT visit_date,visit_month,
       COUNT(compliance_key) AS daily_users_type,
       COUNT(institutionName) AS daily_institution_type,
       monthly_users,daily_users,
       monthly_institution,daily_institution ,
       daily_users / monthly_users AS retention
FROM(
SELECT DATE(login_date) AS visit_date,
       DATE_TRUNC(login_date, month) AS visit_month,
       compliance_key,institutionName,
       COUNT(DISTINCT compliance_key) OVER(PARTITION BY DATE_TRUNC(login_date, month)) AS monthly_users,
       COUNT(DISTINCT compliance_key) OVER(PARTITION BY DATE(login_date)) AS daily_users,
       COUNT(DISTINCT institutionName) OVER(PARTITION BY DATE_TRUNC(login_date, month)) AS monthly_institution,
       COUNT(DISTINCT institutionName) OVER(PARTITION BY DATE(login_date)) AS daily_institution
FROM (
SELECT COALESCE(daily_logins.compliance_key,user.compliance_key) AS compliance_key, 
       daily_logins.login_date
FROM `unity-other-liveplatform-prd.ontology.cml_daily_login` daily_logins
JOIN user ON user.machineid = daily_logins.machineid AND user.license_hash = daily_logins.license_hash
GROUP BY 1,2
) AS editor_act
JOIN install ON editor_act.compliance_key = install.user_id
GROUP BY 1,2,3,4,login_date) AS A
GROUP BY 1,2,5,6,7,8
ORDER BY 1 DESC, 2 DESC
