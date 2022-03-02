CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.educator_activation` AS

SELECT A.compliance_key,
       A.license,
       D.created_date AS user_create_date,
       CASE WHEN DATE(D.created_date)=DATE(A.created_time) THEN 'New Account'
            ELSE 'Existing Account' END AS account_type,
       A.created_time AS licnese_create_time,
       A.grant_time AS licnese_grant_time,
       A.expiration_time AS licnese_expiration_time,
       E.email AS email
FROM (
SELECT compliance_key,created_time,grant_time,expiration_time,license,
       ROW_NUMBER() OVER(PARTITION BY compliance_key ORDER BY created_time DESC) AS rnk
FROM `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.user_educator_license`
GROUP BY 1,2,3,4,5
) AS A 
LEFT JOIN `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.user_creation_date` AS D ON D.compliance_key=A.compliance_key
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr_restricted.user` AS E ON TO_BASE64(SHA256(CAST(E.id AS STRING)))=A.compliance_key
WHERE A.rnk=1
GROUP BY 1,2,3,4,5,6,7,8




