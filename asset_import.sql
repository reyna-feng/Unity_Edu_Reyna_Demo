--Update Time: 4/28
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.asset_import` AS

WITH 
user AS (
  SELECT compliance_key,institution,country_code_most_freq,fullName,
         if_expired,grant_time,expire_time,license_type
  FROM `unity-other-learn-prd.reynafeng.academiclicense` A
  GROUP BY 1,2,3,4,5,6,7,8
)

SELECT   COALESCE(A.compliance_key, user.compliance_key) compliance_key,
         A.submit_date,asset_name_import_success,user.* EXCEPT(compliance_key)
FROM (
SELECT compliance_key,submit_date,head.license_kind,
       IF(body.package_import_status = 1, body.package_name, NULL) AS asset_name_import_success
FROM `unity-ai-data-prd.editor_analytics.editor_analytics_assetImportStatus_v1` raw
WHERE submit_date IS NOT NULL AND body.package_import_status=1
GROUP BY 1,2,3,4
  ) A
JOIN user ON A.compliance_key=user.compliance_key
GROUP BY 1,2,3,4,5,6,7,8,9,10
