CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.student_activation` AS

SELECT COALESCE(A.compliance_key,B.compliance_key,C.compliance_key) AS compliance_key,
       CASE WHEN COALESCE(A.compliance_key,B.compliance_key,C.compliance_key) IN (SELECT DISTINCT compliance_key FROM `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.user_student_license`) THEN 'Github'
            WHEN A.compliance_key IS NOT NULL THEN 'SheerID'
            ELSE 'No Licnese' END AS verification_type,
       D.created_date AS user_create_date,
       A.created_time AS licnese_create_time,
       A.grant_time AS licnese_grant_time,
       A.expiration_time AS licnese_expiration_time,
       E.email AS email,
       MIN(B.body.triggeredTime) AS first_activation_ts,
       MAX(B.body.triggeredTime) AS last_activation_ts,
       MAX(B.body.status) AS activation_status
FROM (
SELECT compliance_key,created_time,grant_time,expiration_time,
       ROW_NUMBER() OVER(PARTITION BY compliance_key ORDER BY created_time DESC) AS rnk
FROM `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.user_student_license_sheer`
GROUP BY 1,2,3,4
) AS A 
FULL OUTER JOIN `unity-ai-data-prd.genesis_studentLicense.genesis_studentLicense_activation_v1` AS B ON B.compliance_key=A.compliance_key AND B.submit_date IS NOT NULL 
FULL OUTER JOIN `unity-ai-data-prd.genesis_studentLicense.genesis_studentLicense_callbackURL_v1` AS C ON C.compliance_key=COALESCE(A.compliance_key,B.compliance_key) AND C.submit_date IS NOT NULL
LEFT JOIN `unity-it-open-dataplatform-prd.dw_live_platform_analytics_extract.user_creation_date` AS D ON D.compliance_key=COALESCE(A.compliance_key,B.compliance_key,C.compliance_key)
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr_restricted.user` AS E ON TO_BASE64(SHA256(CAST(E.id AS STRING)))=COALESCE(A.compliance_key,B.compliance_key,C.compliance_key)
WHERE A.rnk=1
GROUP BY 1,2,3,4,5,6,7
