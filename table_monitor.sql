CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.table_monitor` AS

SELECT DATE(ulfCreatedTime) AS table_date,
       'unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations' AS table,
       COUNT(DISTINCT hwidUlf) AS datapoints
FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations` 
WHERE serialCategoryName='Edu Subscription Multi-install' AND isTest != true
GROUP BY 1,2

UNION ALL

SELECT DATE(first_hub_login) AS table_date,
       'unity-other-liveplatform-prd.nuo.nuo_funnel_all_sessions' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-other-liveplatform-prd.nuo.nuo_funnel_all_sessions` nuo
GROUP BY 1,2

UNION ALL

SELECT DATE(first_login_ml_session) AS table_date,
       'unity-other-liveplatform-prd.ontology.ml_session_mapping' AS table,
       COUNT(DISTINCT sessionid) AS datapoints
FROM `unity-other-liveplatform-prd.ontology.ml_session_mapping`
WHERE machine_count_per_session = 1
GROUP BY 1,2

UNION ALL

SELECT DATE(first_login_date) AS table_date,
       'unity-other-liveplatform-prd.ontology.ckey_ml_mapping' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping`
GROUP BY 1,2

UNION ALL

SELECT DATE(daily_logins.login_date) AS table_date,      
       'unity-other-liveplatform-prd.ontology.cml_daily_login' AS table,
       COUNT(DISTINCT daily_logins.compliance_key) AS datapoints
FROM `unity-other-liveplatform-prd.ontology.cml_daily_login` daily_logins
GROUP BY 1,2

UNION ALL

SELECT DATE(mapping.submit_date) AS table_date,
       'unity-ai-data-prd.editor_analytics.editor_analytics_appRunning_v1' AS table,
       COUNT(DISTINCT mapping.compliance_key) AS datapoints
FROM `unity-ai-data-prd.editor_analytics.editor_analytics_appRunning_v1` mapping
WHERE submit_date IS NOT NULL
GROUP BY 1,2

UNION ALL

SELECT DATE(body.grant_time) AS table_date,
       'unity-ai-data-prd.genesis_grantLicense.genesis_grantLicense_educationLicenseRecord_v1' AS table,
       COUNT(DISTINCT IF(compliance_key IS NULL,body.user_id,compliance_key)) AS user_id
FROM `unity-ai-data-prd.genesis_grantLicense.genesis_grantLicense_educationLicenseRecord_v1` 
WHERE submit_date IS NOT NULL
      AND body.deleted = false
GROUP BY 1,2

UNION ALL

SELECT DATE(body.created_time) AS table_date,
       'unity-ai-data-prd.genesis_grantLicense.genesis_grantLicense_educationLicenseRequest_v1' AS table,
       COUNT(DISTINCT body.license_record_id) AS datapoints
FROM `unity-ai-data-prd.genesis_grantLicense.genesis_grantLicense_educationLicenseRequest_v1` 
WHERE submit_date IS NOT NULL
      AND body.deleted = false
GROUP BY 1,2

