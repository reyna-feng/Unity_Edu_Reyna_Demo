--Update Time: 5/2
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.asset_import` AS

WITH asset AS(
SELECT * EXCEPT(rnk)
FROM(
  SELECT name,publisher_name,category_name,price,ROW_NUMBER() OVER(PARTITION BY name ORDER BY created_at) AS rnk
  FROM `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.assetstore_asset_metadata`
  WHERE name IS NOT NULL
) AS A
WHERE rnk=1
),

user AS (
  SELECT compliance_key,institution,country_code_most_freq,fullName,
         if_expired,grant_time,expire_time,license_type
  FROM `unity-other-learn-prd.reynafeng.academiclicense` A
  GROUP BY 1,2,3,4,5,6,7,8
)

SELECT   COALESCE(A.compliance_key, user.compliance_key) compliance_key,
         A.submit_date,asset_name_import_success,user.* EXCEPT(compliance_key),
         IF(NOT asset.publisher_name IS NULL, asset.publisher_name,'Unknown') AS publisher_name,
         IF(NOT asset.category_name IS NULL, asset.category_name, 'Unknown') AS category_name,
         CASE WHEN asset.price IS NULL THEN 'Unknown' 
            WHEN asset.price IS NOT NULL AND asset.price>0 THEN 'Paid'
            ELSE 'Free' END AS price_type,
FROM (
SELECT compliance_key,submit_date,head.license_kind,
       IF(body.package_import_status = 1, body.package_name, NULL) AS asset_name_import_success
FROM `unity-ai-data-prd.editor_analytics.editor_analytics_assetImportStatus_v1` raw
WHERE submit_date IS NOT NULL AND body.package_import_status=1
GROUP BY 1,2,3,4
  ) A
JOIN user ON A.compliance_key=user.compliance_key
LEFT JOIN asset ON A.asset_name_import_success=asset.name
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13