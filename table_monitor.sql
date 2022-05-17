--Update Time: 5/17--
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.table_monitor` AS

SELECT DATE(ulfCreatedTime) AS table_date,
       'unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations' AS table,
       COUNT(DISTINCT hwidUlf) AS datapoints
FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations` 
WHERE serialCategoryName='Edu Subscription Multi-install' AND isTest != true 
GROUP BY 1,2

UNION ALL

SELECT DATE(ulfCreatedTime) AS table_date,
       'unity-ai-unity-insights-prd.ai_feature_catalog.user_serial_activations' AS table,
       COUNT(DISTINCT hwidUlf) AS datapoints
FROM `unity-ai-unity-insights-prd.ai_feature_catalog.user_serial_activations`
WHERE serialCategoryName='Edu Subscription Multi-install' AND isTest != true 
GROUP BY 1,2

UNION ALL

SELECT DATE(first_hub_login) AS table_date,
       'unity-other-liveplatform-prd.nuo.nuo_funnel_all_sessions' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-other-liveplatform-prd.nuo.nuo_funnel_all_sessions` nuo
WHERE compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.academiclicense`)
GROUP BY 1,2

UNION ALL

SELECT DATE(first_login_ml_session) AS table_date,
       'unity-other-liveplatform-prd.ontology.ml_session_mapping' AS table,
       COUNT(DISTINCT sessionid) AS datapoints
FROM `unity-other-liveplatform-prd.ontology.ml_session_mapping`
WHERE machine_count_per_session = 1 AND sessionid IN (SELECT DISTINCT sessionid FROM `unity-other-learn-prd.reynafeng.academiclicense`)
GROUP BY 1,2

UNION ALL

SELECT DATE(first_login_date) AS table_date,
       'unity-other-liveplatform-prd.ontology.ckey_ml_mapping' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-other-liveplatform-prd.ontology.ckey_ml_mapping`
WHERE compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.academiclicense`)
GROUP BY 1,2

UNION ALL

SELECT DATE(daily_logins.login_date) AS table_date,      
       'unity-other-liveplatform-prd.ontology.cml_daily_login' AS table,
       COUNT(DISTINCT daily_logins.compliance_key) AS datapoints
FROM `unity-other-liveplatform-prd.ontology.cml_daily_login` daily_logins
WHERE compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.academiclicense`)
GROUP BY 1,2

UNION ALL

SELECT DATE(mapping.submit_date) AS table_date,
       'unity-ai-data-prd.editor_analytics.editor_analytics_appRunning_v1' AS table,
       COUNT(DISTINCT mapping.compliance_key) AS datapoints
FROM `unity-ai-data-prd.editor_analytics.editor_analytics_appRunning_v1` mapping
WHERE submit_date IS NOT NULL AND compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.academiclicense`)
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

UNION ALL

SELECT DATE(submit_date) AS table_date,
       'unity-ai-data-prd.assetStore_storeFront.assetStore_storeFront_packageEditorDownload_v1' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-ai-data-prd.assetStore_storeFront.assetStore_storeFront_packageEditorDownload_v1`
WHERE submit_date IS NOT NULL AND compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.academiclicense`)
GROUP BY 1,2

UNION ALL

SELECT DATE(submit_date) AS table_date,
       'unity-ai-data-prd.editor_analytics.editor_analytics_assetImportStatus_v1' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-ai-data-prd.editor_analytics.editor_analytics_assetImportStatus_v1`
WHERE submit_date IS NOT NULL AND compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.academiclicense`)
GROUP BY 1,2

UNION ALL

SELECT DATE(submit_date) AS table_date,
       'unity-ai-data-prd.assetStore_storeFront.assetStore_storeFront_packagePurchased_v2' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-ai-data-prd.assetStore_storeFront.assetStore_storeFront_packagePurchased_v2`
WHERE submit_date IS NOT NULL AND compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.academiclicense`)
GROUP BY 1,2

UNION ALL

SELECT DATE(editor_date) AS table_date,
       'unity-other-learn-prd.reynafeng.editor_usage' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-other-learn-prd.reynafeng.editor_usage`
GROUP BY 1,2

UNION ALL

SELECT DATE(grant_time) AS table_date,
       'unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.user_educator_license' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.user_educator_license`
GROUP BY 1,2

UNION ALL

SELECT DATE(submit_date) AS table_date,
       'unity-ai-data-prd.genesis_educatorLicense.genesis_educatorLicense_callbackURL_v1' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-ai-data-prd.genesis_educatorLicense.genesis_educatorLicense_callbackURL_v1`
GROUP BY 1,2

UNION ALL

SELECT DATE(learn_date) AS table_date,
       'unity-other-learn-prd.reynafeng.learn' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-other-learn-prd.reynafeng.learn`
GROUP BY 1,2

UNION ALL

SELECT DATE(usage_date) AS table_date,
       'unity-other-learn-prd.reynafeng.other_usage' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-other-learn-prd.reynafeng.other_usage`
GROUP BY 1,2

UNION ALL

SELECT DATE(grant_time) AS table_date,
       'unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.user_student_license_sheer' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.user_student_license_sheer`
GROUP BY 1,2

UNION ALL

SELECT DATE(submit_date) AS table_date,
       'unity-ai-data-prd.genesis_studentLicense.genesis_studentLicense_callbackURL_v1' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-ai-data-prd.genesis_studentLicense.genesis_studentLicense_callbackURL_v1`
GROUP BY 1,2

UNION ALL

SELECT DATE(submit_date) AS table_date,
       'unity-ai-data-prd.genesis_studentLicense.genesis_studentLicense_activation_v1' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-ai-data-prd.genesis_studentLicense.genesis_studentLicense_activation_v1`
WHERE submit_date IS NOT NULL
GROUP BY 1,2

UNION ALL

SELECT DATE(submit_date) AS table_date,
       'unity-ai-data-prd.cloud_build_raw.cloud_build_buildAttempt_v1' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-ai-data-prd.cloud_build_raw.cloud_build_buildAttempt_v1`
WHERE submit_date IS NOT NULL AND compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.academiclicense`)
GROUP BY 1,2

UNION ALL

SELECT DATE(submit_date) AS table_date,
       'unity-ai-data-prd.cloud_build_raw.cloud_build_buildDeploy_v1' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-ai-data-prd.cloud_build_raw.cloud_build_buildDeploy_v1`
WHERE submit_date IS NOT NULL AND compliance_key IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.academiclicense`)
GROUP BY 1,2

UNION ALL

SELECT DATE(ts) AS table_date,
       'unity-other-learn-prd.reynafeng.cloud_collaborate' AS table,
       COUNT(DISTINCT compliance_key) AS datapoints
FROM `unity-other-learn-prd.reynafeng.cloud_collaborate`
GROUP BY 1,2

