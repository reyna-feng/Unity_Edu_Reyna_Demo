--Update Time: 5/17
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.asset_store` AS

WITH asset AS(
SELECT * EXCEPT(rnk)
FROM(
  SELECT A.id,A.name,A.publisher_name,A.created_at,
         IF(NOT B.category_name IS NULL, B.category_name,CAST(A.category_id AS STRING)) AS category_name,
         A.price,ROW_NUMBER() OVER(PARTITION BY A.name ORDER BY A.created_at) AS rnk
  FROM `unity-ai-unity-insights-prd.ai_live_platform_analytics_extract.assetstore_asset_metadata` A
  LEFT JOIN `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.assetstore_asset_metadata` B ON A.id=B.id AND A.publisher_id=B.publisher_id
  WHERE A.name IS NOT NULL
  GROUP BY 1,2,3,4,5,6
) AS A
WHERE rnk=1
),

users AS(
  SELECT compliance_key,institution,country_code_most_freq,fullName,
         if_expired,grant_time,expire_time,license_type
  FROM `unity-other-learn-prd.reynafeng.academiclicense`
  --WHERE grant_time >= DATE('2020-02-15')
  GROUP BY 1,2,3,4,5,6,7,8
)
SELECT *,
       SUM(amount_final_usd) OVER(PARTITION BY compliance_key) AS total_usd_user
FROM(
SELECT body.ts AS timestamp,A.submit_date,A.compliance_key,body.amount_final_usd,body.purchase_type,
       body.quantity,body.sale_type,body.source,
       IF(NOT name IS NULL, name, 'Unknown') AS asset_name,
       IF(NOT publisher_name IS NULL, publisher_name, 'Unknown') AS publisher_name,
       IF(NOT category_name IS NULL, category_name, 'Unknonw') AS category_name,
       B.* EXCEPT(compliance_key)
FROM `unity-ai-data-prd.assetStore_storeFront.assetStore_storeFront_packagePurchased_v2` A
JOIN users B ON A.compliance_key=B.compliance_key AND A.submit_date BETWEEN DATE(B.grant_time) AND DATE(B.expire_time)
LEFT JOIN asset D ON SAFE_CAST(A.body.package_id AS INT64)=D.id
WHERE submit_date IS NOT NULL AND body.amount_final_usd!=0 AND submit_date >= DATE('2020-02-15')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
) AS A 