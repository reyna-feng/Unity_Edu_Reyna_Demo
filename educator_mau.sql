--Update Time: 3/10 9:40 PM--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.educator_mau` AS

WITH user AS (
  SELECT compliance_key,machineid,license_hash
  FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping` 
  GROUP BY 1,2,3
)

SELECT visit_date,visit_month,license,
       DAU,MAU,
       DAU_License,
       MAU_License,
       DAU / MAU AS retention,
       DAU_License / MAU_License AS retention_license
FROM(
SELECT DATE(login_date) AS visit_date,
       DATE_TRUNC(login_date, month) AS visit_month,
       login_date,
       editor_act.compliance_key,
       license,
       COUNT(DISTINCT editor_act.compliance_key) OVER(PARTITION BY DATE_TRUNC(login_date, month)) AS MAU,
       COUNT(DISTINCT editor_act.compliance_key) OVER(PARTITION BY DATE_TRUNC(login_date, month),license) AS MAU_License,
       COUNT(DISTINCT editor_act.compliance_key) OVER(PARTITION BY DATE(login_date)) AS DAU,
       COUNT(DISTINCT editor_act.compliance_key) OVER(PARTITION BY DATE(login_date),license) AS DAU_License
FROM (
SELECT COALESCE(daily_logins.compliance_key,user.compliance_key) AS compliance_key, 
       daily_logins.login_date
FROM `unity-other-liveplatform-prd.ontology.cml_daily_login` daily_logins
JOIN user ON user.machineid = daily_logins.machineid AND user.license_hash = daily_logins.license_hash
GROUP BY 1,2
) AS editor_act
JOIN `unity-other-learn-prd.reynafeng.educator_activation` AS install ON editor_act.compliance_key = install.compliance_key AND install.activation_status=True AND DATE(login_date) BETWEEN DATE(install.licnese_grant_time) AND DATE(install.licnese_expiration_time)
GROUP BY 1,2,3,4,5) AS A
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY 1 DESC, 2 DESC