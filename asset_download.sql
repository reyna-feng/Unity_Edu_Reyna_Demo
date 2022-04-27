--Update Time: 4/27
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.asset_download` AS

WITH asset AS(
  SELECT id,name,publisher_name
  FROM `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.assetstore_asset_metadata`
  WHERE name IS NOT NULL
)

SELECT *,
       COUNT(*) OVER(PARTITION BY compliance_key) AS total_downloads
FROM(
SELECT body.ts AS timestamp,A.submit_date,A.compliance_key,
       IF(NOT name IS NULL, name, 'Unknown') AS asset_name,
       IF(NOT publisher_name IS NULL, publisher_name, 'Unknown') AS publisher_name,
       B.* EXCEPT(compliance_key)
FROM `unity-ai-data-prd.assetStore_storeFront.assetStore_storeFront_packageEditorDownload_v1` A
JOIN `unity-other-learn-prd.reynafeng.academiclicense` B ON A.compliance_key=B.compliance_key AND A.submit_date BETWEEN DATE(B.grant_time) AND DATE(B.expire_time)
LEFT JOIN asset D ON SAFE_CAST(A.body.package_id AS INT64)=D.id
WHERE submit_date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
) AS A 
