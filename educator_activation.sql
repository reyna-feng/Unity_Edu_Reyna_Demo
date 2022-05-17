--Update Time: 5/17--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.educator_activation` AS

WITH license AS(
SELECT COALESCE(A.compliance_key,B.compliance_key) AS compliance_key,
       B.userId AS user_id,
       COALESCE(A.created_time,B.triggeredTime,C.createdTime) AS create_time,
       C.initialActivationDate,originSystem,C.serialCategorySlug,
       C.serialNumber,
       COALESCE(A.grant_time,C.validStart) AS grant_time,
       COALESCE(A.expiration_time,C.validEnd) AS expiration_time,
       IF(NOT A.license IS NULL, A.license, IF(NOT C.serialCategorySlug IS NULL, C.serialCategorySlug, 'Unknown')) AS license
FROM `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.user_educator_license` AS A
FULL JOIN (
SELECT DISTINCT compliance_key,body.userId,submit_date,body.triggeredTime
FROM `unity-ai-data-prd.genesis_educatorLicense.genesis_educatorLicense_callbackURL_v1`
WHERE submit_date IS NOT NULL
GROUP BY 1,2,3,4
) AS B ON B.compliance_key=A.compliance_key
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr_restricted.serial` C ON TO_BASE64(SHA256(CAST(C.ownerId AS STRING)))=COALESCE(A.compliance_key,B.compliance_key)
)

SELECT A.compliance_key,
       A.originSystem,A.serialCategorySlug,A.serialNumber,A.license,A.user_id,
       D.created_date AS user_create_date,
       CASE WHEN DATE(D.created_date)=DATE(A.create_time) THEN 'New Account' ELSE 'Existing Account' END AS account_type,
       A.create_time AS licnese_create_time,
       A.grant_time AS licnese_grant_time,
       A.expiration_time AS licnese_expiration_time,
       E.email AS email,
       IF(A.initialActivationDate IS NULL, False, True) AS activation_status,
       A.initialActivationDate AS first_activation_ts
FROM license AS A
LEFT JOIN `unity-ai-unity-insights-prd.ai_live_platform_analytics_extract.user_creation_date` AS D ON D.compliance_key=A.compliance_key
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr_restricted.user` AS E ON TO_BASE64(SHA256(CAST(E.id AS STRING)))=A.compliance_key
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14