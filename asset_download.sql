--Update Time: 5/26
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.asset_download` AS

WITH asset AS(
SELECT * EXCEPT(rnk)
FROM(
SELECT *,
       ROW_NUMBER() OVER(PARTITION BY id ORDER BY A.created_at) AS rnk
FROM(
  SELECT COALESCE(A.id,B.id) AS id,
         COALESCE(A.name,B.name) AS name,
         COALESCE(A.publisher_name,B.publisher_name) AS publisher_name,
         COALESCE(A.created_at,B.created_at) AS created_at,
         IF(NOT B.category_name IS NULL, B.category_name,CAST(A.category_id AS STRING)) AS category_name,
         COALESCE(A.price,B.price) AS price
  FROM `unity-ai-unity-insights-prd.ai_live_platform_analytics_extract.assetstore_asset_metadata` A
  FULL JOIN `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.assetstore_asset_metadata` B ON A.id=B.id AND A.publisher_id=B.publisher_id
  GROUP BY 1,2,3,4,5,6
) AS A
) AS B
WHERE rnk=1
),

users AS(
  SELECT compliance_key,institution,country_code_most_freq,fullName,
         if_expired,grant_time,expire_time,license_type
  FROM `unity-other-learn-prd.reynafeng.academiclicense`
  WHERE grant_time >= DATE('2020-02-15')
  GROUP BY 1,2,3,4,5,6,7,8
)

SELECT *,
       COUNT(*) OVER(PARTITION BY compliance_key) AS total_downloads
FROM(
SELECT body.ts AS timestamp,A.submit_date,A.compliance_key,
       IF(NOT name IS NULL, name, 'Unknown') AS asset_name,
       IF(NOT publisher_name IS NULL, publisher_name, 'Unknown') AS publisher_name,
       IF(NOT category_name IS NULL, category_name, 'Unknonw') AS category_name,
       CASE WHEN price IS NULL THEN 'Unknown' 
            WHEN price IS NOT NULL AND price>0 THEN 'Paid'
            ELSE 'Free' END AS price_type,
       B.* EXCEPT(compliance_key)
FROM `unity-ai-data-prd.assetStore_storeFront.assetStore_storeFront_packageEditorDownload_v1` A
JOIN users B ON A.compliance_key=B.compliance_key AND A.submit_date BETWEEN DATE(B.grant_time) AND DATE(B.expire_time)
LEFT JOIN asset D ON SAFE_CAST(A.body.package_id AS INT64)=D.id
WHERE submit_date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
) AS A 