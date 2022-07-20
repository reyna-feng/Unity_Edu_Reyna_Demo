--Update Time: 6/24
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.certification` AS

WITH salesforce_temp AS (
SELECT account_name,Opportunity_ID__c,Channel__c,ACV_USD,region,Sub_Region__c,
       internal_segment,internal_subsegment,
       opportunity_start_date,opportunity_end_date,
       Name,POB_Category__c,Sub_Product_Family__c,
       CASE WHEN Name LIKE '%Professional%' THEN 'Professional'
            WHEN Name LIKE '%Associate%' THEN 'Associate'
            WHEN Name LIKE '%Exper%' THEN 'Expert' ELSE 'Error' END AS type
FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity`
WHERE  Name LIKE 'Cert %' AND 
       SBQQ__ProductFamily__c='Education' AND Stage_Category='Won'
),

salesforce AS (
SELECT DATE_TRUNC(A.opportunity_start_date,MONTH) AS report_month,SUM(ACV_USD) AS salesforce
FROM salesforce_temp A
GROUP BY 1
ORDER BY 1 DESC
),

pearson_candidate AS (
SELECT DATE_TRUNC(Date,MONTH) AS report_month,SUM(NetCollAmt*ExchangeRate) AS pearson_candidate
FROM `unity-other-learn-prd.reynafeng.Pearson_Candidate`
WHERE Status='Delivered'
GROUP BY 1
ORDER BY 1 DESC
),

pearson_minhub AS (
SELECT DATE_TRUNC(PaymentDate,MONTH) AS report_month,
       SUM(DiscountedTotalUSD)+SUM(ProductLossAmountUSD)-SUM(CreditCardFee)-SUM(PearsonVUEFee) AS pearson_mindhub
FROM `unity-other-learn-prd.reynafeng.Pearson_Mindhub`
WHERE OrderStatus='Completed'
GROUP BY 1
ORDER BY 1 DESC
)

SELECT A.report_month,A.salesforce,B.pearson_candidate,C.pearson_mindhub
FROM salesforce A
LEFT JOIN pearson_candidate B ON A.report_month=B.report_month
LEFT JOIN pearson_minhub C ON A.report_month=C.report_month
WHERE A.report_month<=DATE_TRUNC(CURRENT_DATE(),MONTH)
ORDER BY 1 DESC