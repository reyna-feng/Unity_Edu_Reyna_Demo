CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_mau` AS

WITH install AS (
    SELECT TO_BASE64(SHA256(CAST(install.userId AS string))) AS user_id,install.serialNumber AS license,
           request.institutionName
    FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations`AS install
    JOIN `unity-other-learn-prd.reynafeng.egl_records` AS record ON record.license=install.serialNumber
    JOIN `unity-other-learn-prd.reynafeng.egl_requests` AS request ON record.license_record_id=request.license_record_id
    WHERE install.serialCategoryName='Edu Subscription Multi-install'
    GROUP BY 1,2,3
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
JOIN install ON editor_act.compliance_key = install.user_id
GROUP BY 1,2,3,4,5,login_date) AS A
GROUP BY 1,2,3,6,7,8,9,10
ORDER BY 1 DESC, 2 DESC