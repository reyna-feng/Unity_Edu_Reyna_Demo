CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.egl_installs` AS

SELECT TO_BASE64(SHA256(CAST(ownerId AS STRING))) AS user_id,
       serialNumber AS license,MIN(DATE(initialActivationDate)) AS install_date,
       COUNT(DISTINCT hwidUlf) AS installs
FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations` 
WHERE serialCategoryName='Edu Subscription Multi-install' 
      AND isDeleted != true AND isTest != true
GROUP BY 1,2