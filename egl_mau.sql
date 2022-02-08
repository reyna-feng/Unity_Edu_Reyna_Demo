CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_mau` AS

SELECT DATE_TRUNC(login_date, month) AS visit_month,COUNT(DISTINCT compliance_key) as monthly_users
FROM `unity-other-liveplatform-prd.ontology.ckey_daily_login` AS editor_act
INNER JOIN `unity-other-learn-prd.reynafeng.egl_installs` AS install ON editor_act.compliance_key=install.user_id AND install.install_date <= editor_act.login_date
GROUP BY 1
ORDER BY 1 DESC