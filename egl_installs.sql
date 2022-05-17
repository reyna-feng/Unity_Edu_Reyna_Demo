--Update Time: 5/17--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_installs` AS

WITH salorma AS(
SELECT hwidUlf,serialId
FROM `unity-ai-unity-insights-prd.ai_feature_catalog.user_serial_activations`
WHERE isTest != true
      AND customerNumber_source='serial-salorma'
      AND (serialCategoryDescription LIKE 'Educational%' OR serialCategoryDescription LIKE 'Deactive%')
GROUP BY 1,2
)

SELECT TO_BASE64(SHA256(CAST(ownerId AS STRING))) AS user_id,
       TO_BASE64(SHA256(CAST(userId AS STRING))) AS real_user_id,
       userUuid,
       serialNumber AS license,
       DATE(ulfCreatedTime) AS install_date,
       COUNT(DISTINCT hwidUlf) AS installs
FROM `unity-ai-unity-insights-prd.ai_feature_catalog.user_serial_activations`
WHERE serialCategoryName='Edu Subscription Multi-install' 
      AND isDeleted != true AND isTest != true
      AND (serialId NOT IN (SELECT DISTINCT serialId FROM salorma) OR hwidUlf NOT IN (SELECT DISTINCT hwidUlf FROM salorma))
GROUP BY 1,2,3,4,5