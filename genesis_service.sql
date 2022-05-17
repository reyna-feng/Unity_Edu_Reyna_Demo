--Update Time: 5/17--
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.genesis_service` AS

SELECT A.id,A.ownerId,A.subsStartDate,A.subsEndDate,A.group,A.itemId,A.itemRev,A.createdTime,
       A.currency,A.totalseats,A.assignedSeats,B.* EXCEPT(item_id)
FROM `unity-ai-unity-insights-prd.ai_live_platform_analytics_extract.genesis_subscription_cr` A
JOIN `unity-other-liveplatform-prd.subscriptions_core_models.genesis_items_online` B ON B.item_id=A.itemId
WHERE A.group IN ('Cloud Build','Cloud Build Addon','Unity Learn Premium','Unity Reflect','Unity Reflect Collaborate','Unity Teams')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22