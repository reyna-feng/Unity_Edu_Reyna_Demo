--Update Time: 7/12
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.genesis_system` AS
WITH item AS(
SELECT id,slug
FROM `unity-ai-unity-insights-prd.source_genesis_mq_cr.item`
--WHERE (slug LIKE '%unity-pro%' OR slug LIKE '%unity-enterprise%')
--      AND subscriptionGroup IN ('','Unity Pro','Unity Editor')
--      AND (name LIKE 'Unity Pro%' OR name LIKE 'Unity Enterprise%')
GROUP BY 1,2
),

subscription AS(
SELECT A.*
FROM `unity-ai-unity-insights-prd.ai_live_platform_analytics_extract.genesis_subscription_cr` A
JOIN item B ON A.itemId=B.id
--WHERE A.group!='Unity Industrial Collection'
),

entitlement AS(
SELECT A.*,B.slug,subscription.* EXCEPT(id,ownerId,ownerType,orderId,createdByClient,createdTime,updatedBy,updatedByClient,updatedTime,itemId,LoadDate,uuid)
FROM `unity-ai-unity-insights-prd.source_genesis_mq_cr.entitlement` A
JOIN item B ON A.itemId=B.id
JOIN subscription ON subscription.id=A.subscriptionId
--WHERE A.itemComponentkey!='UnityProTrial' AND A.itemComponentKey!='UnityProSupport' AND A.namespace!='unity_analytics'
)

SELECT A.rootEntitlementId,A.id,A.isCourseware,A.isEdu,B.isActive,B.expirationTime,B.grantTime,
       A.isGovoucherGranted,A.itemComponentKey,
       B.ownerId,A.userId,B.slug,B.deleted,
       B.group,B.itemId,B.itemRev,B.createdTime,B.currency,B.totalseats,B.assignedSeats,
       C.serialNumber,D.serialCategoryName,
       E.C_Company_Internal_Segment_SFDC1,E.C_Company_Internal_Sub_Segment_SFDC1,
       CASE WHEN B.slug LIKE '%-ent%' THEN 'Unity Enterprise' ELSE 'Unity Pro' END AS type,
       E.C_EmailAddress,E.C_FirstName,E.C_LastName,E.C_Company,E.C_Country,
       E.C_SFDC_EmailOptOut1,E.C_Industry1,COALESCE(E.C_Job_Role1,E.C_Job_Primary_Role1) AS C_Job_Role,COALESCE(E.C_Region1,E.C_Region11) AS C_Region,
       CASE WHEN TO_BASE64(SHA256(CAST(B.ownerId AS STRING))) IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.student_activation`) THEN 'Student Plan'
            WHEN TO_BASE64(SHA256(CAST(B.ownerId AS STRING))) IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.educator_activation`) THEN 'Educator Plan'
            WHEN TO_BASE64(SHA256(CAST(B.ownerId AS STRING))) IN (SELECT DISTINCT user_id FROM `unity-other-learn-prd.reynafeng.egl_installs`) THEN 'Education Grant License' 
            ELSE 'Other Licenses' END AS license_type
FROM `unity-ai-unity-insights-prd.source_genesis_mq_cr.entitlement_assignment` A
JOIN entitlement B ON A.rootEntitlementId=CAST(B.id AS STRING)
JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr.serial` C ON A.serialNumber=C.serialNumber
JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr.serial_category` D ON D.serialCategoryId=C.serialCategoryId
JOIN `unity-ai-unity-insights-prd.salesops_insights.marketing_eloqua_raw_restricted_contacts` E ON E.C_Unity_Account_ID1=CAST(A.userId AS STRING) 
WHERE E.C_Company_Internal_Segment_SFDC1='Education'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34

UNION ALL

SELECT A.rootEntitlementId,A.id,A.isCourseware,A.isEdu,B.isActive,B.expirationTime,B.grantTime,
       A.isGovoucherGranted,A.itemComponentKey,
       B.ownerId,A.userId,B.slug,B.deleted,
       B.group,B.itemId,B.itemRev,B.createdTime,B.currency,B.totalseats,B.assignedSeats,
       C.serialNumber,D.serialCategoryName,
       E.C_Company_Internal_Segment_SFDC1,E.C_Company_Internal_Sub_Segment_SFDC1,
       CASE WHEN B.slug LIKE '%-ent%' THEN 'Unity Enterprise' ELSE 'Unity Pro' END AS type,
       E.C_EmailAddress,E.C_FirstName,E.C_LastName,E.C_Company,E.C_Country,
       E.C_SFDC_EmailOptOut1,E.C_Industry1,COALESCE(E.C_Job_Role1,E.C_Job_Primary_Role1) AS C_Job_Role,COALESCE(E.C_Region1,E.C_Region11) AS C_Region,
       CASE WHEN TO_BASE64(SHA256(CAST(B.ownerId AS STRING))) IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.student_activation`) THEN 'Student Plan'
            WHEN TO_BASE64(SHA256(CAST(B.ownerId AS STRING))) IN (SELECT DISTINCT compliance_key FROM `unity-other-learn-prd.reynafeng.educator_activation`) THEN 'Educator Plan'
            WHEN TO_BASE64(SHA256(CAST(B.ownerId AS STRING))) IN (SELECT DISTINCT user_id FROM `unity-other-learn-prd.reynafeng.egl_installs`) THEN 'Education Grant License' 
            ELSE 'Other Licenses' END AS license_type
FROM `unity-ai-unity-insights-prd.source_genesis_mq_cr.entitlement_assignment` A
JOIN entitlement B ON A.id=B.id
JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr.serial` C ON A.serialNumber=C.serialNumber
JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr.serial_category` D ON D.serialCategoryId=C.serialCategoryId
JOIN `unity-ai-unity-insights-prd.salesops_insights.marketing_eloqua_raw_restricted_contacts` E ON E.C_Unity_Account_ID1=CAST(A.userId AS STRING) 
WHERE E.C_Company_Internal_Segment_SFDC1='Education'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34