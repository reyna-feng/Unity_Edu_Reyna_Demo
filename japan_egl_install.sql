--Update Time: 4/8--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.japan_egl_install` AS

SELECT TO_BASE64(SHA256(CAST(ownerId AS STRING))) AS user_id,
       serialNumber AS license,
       DATE(ulfCreatedTime) AS install_date,
       COUNT(DISTINCT hwidUlf) AS installs
FROM `unity-it-open-dataplatform-prd.dw_customer_insights.UserSerialActivations` 
WHERE serialCategoryName='External' AND serialCategoryDescription='External Developer Version' 
      AND isDeleted != true AND isTest != true AND countryOfResidence='JP'
GROUP BY 1,2,3
