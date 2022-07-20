--Update Time: 7/19
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.genesis_entitlement` AS

SELECT A.compliance_key,A.body.entitlement_id,A.body.user_id,
       DATE(A.body.grant_time) AS entitlement_grant_time,DATE(A.body.expiration_time) AS entitlement_expire_time,
       A.body.tag,A.body.org_id,A.body.subscription_id,
       B.body.subscription_type,B.body.total_seat,
       B.body.seat_type,B.body.country_code,B.body.currency,B.body.local_price,
       B.body.subs_start_date,B.body.subs_end_date,
       C.* EXCEPT(compliance_key,machineid,sessionid,if_expired,user_id),
       COALESCE(E.subscription_group,D.subscription_group) AS subscription_group,
       COALESCE(E.subscription_source,D.subscription_source) AS subscription_source,
       COALESCE(E.subscription_name,D.subscription_name) AS subscription_name
FROM `unity-ai-data-prd.genesis_commerce_raw.genesis_commerce_entitlement_v0` A
JOIN `unity-ai-data-prd.genesis_commerce_raw.genesis_commerce_subscription_v0` B ON B.submit_date IS NOT NULL AND A.body.subscription_id=B.body.subscription_id
JOIN `unity-other-learn-prd.reynafeng.academiclicense` C ON A.body.user_id=C.user_id AND DATE(B.body.subs_start_date) BETWEEN C.grant_time AND C.expire_time
LEFT JOIN `unity-other-learn-prd.reynafeng.genesis_subscription_metadata` D ON D.ownerType='USER' AND CAST(A.body.subscription_id AS STRING)=D.subscription_id AND CAST(D.ownerId AS STRING)=A.body.user_id
LEFT JOIN `unity-other-learn-prd.reynafeng.genesis_subscription_metadata` E ON E.ownerType='ORGANIZATION' AND CAST(A.body.subscription_id AS STRING)=E.subscription_id AND E.ownerId=A.body.org_id
WHERE A.submit_date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30