CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.asset_download` AS

WITH asset AS(
  SELECT id,name,publisher_name
  FROM `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.assetstore_asset_metadata`
  WHERE name IS NOT NULL
)

SELECT *,
       COUNT(*) OVER(PARTITION BY compliance_key,sp,egl,ep) AS total_downloads
FROM(
SELECT body.ts AS timestamp,A.submit_date,A.compliance_key,
       IF(NOT name IS NULL, name, 'Unknown') AS asset_name,
       IF(NOT publisher_name IS NULL, publisher_name, 'Unknown') AS publisher_name,
       IF(NOT B.compliance_key IS NULL, True, False) AS sp,
       IF(NOT B.license IS NULL, B.license, 'Not Applied') AS license,
       IF(NOT C.user_id IS NULL, True, False) AS egl,
       IF(NOT E.compliance_key IS NULL, True, False) AS ep,
       IF(NOT E.license IS NULL, E.license, 'Not Applied') AS educator_license
FROM `unity-ai-data-prd.assetStore_storeFront.assetStore_storeFront_packageEditorDownload_v1` A
LEFT JOIN asset D ON SAFE_CAST(A.body.package_id AS INT64)=D.id
LEFT JOIN `unity-other-learn-prd.reynafeng.student_activation` B ON A.compliance_key=B.compliance_key AND A.submit_date BETWEEN LEAST(DATE(B.licnese_create_time),DATE(B.licnese_grant_time)) AND DATE(B.licnese_expiration_time)
LEFT JOIN `unity-other-learn-prd.reynafeng.egl_grant_license` C ON A.compliance_key=C.user_id AND A.submit_date BETWEEN C.grant_time AND C.expire_time
LEFT JOIN `unity-other-learn-prd.reynafeng.educator_activation` E ON A.compliance_key=E.compliance_key AND A.submit_date BETWEEN LEAST(DATE(E.licnese_create_time),DATE(E.licnese_grant_time)) AND DATE(E.licnese_expiration_time)
WHERE submit_date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10
) AS A 