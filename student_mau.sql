CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.student_mau` AS

SELECT visit_date,visit_month,editor_license,
       COUNT(compliance_key) AS daily_users_type,
       monthly_users,daily_users,verification_type,
       daily_users / monthly_users AS retention
FROM(
SELECT DATE(login_date) AS visit_date,
       DATE_TRUNC(login_date, month) AS visit_month,
       editor_act.compliance_key,editor_license,verification_type,
       COUNT(DISTINCT editor_act.compliance_key) OVER(PARTITION BY DATE_TRUNC(login_date, month)) AS monthly_users,
       COUNT(DISTINCT editor_act.compliance_key) OVER(PARTITION BY DATE(login_date)) AS daily_users
FROM `unity-other-liveplatform-prd.ontology.ckey_daily_login` AS editor_act
JOIN `unity-other-learn-prd.reynafeng.student_activation` AS install ON editor_act.compliance_key = install.compliance_key AND install.activation_status=true
GROUP BY 1,2,3,4,5,login_date) AS A
GROUP BY 1,2,3,5,6,7,8
ORDER BY 1 DESC, 2 DESC

