--Update Time: 4/26
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.opportunity_uaa` AS

WITH start_contract AS(
SELECT DATE_TRUNC(opportunity_start_date,MONTH) AS start_month,
       COUNT(DISTINCT account_name) AS num_start
FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity`
WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%'
GROUP BY 1
ORDER BY 1
),

end_contract AS(
SELECT DATE_TRUNC(opportunity_end_date,MONTH) AS end_month,
       COUNT(DISTINCT account_name) AS num_end
FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity`
WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%'
GROUP BY 1
ORDER BY 1
),

total AS(
SELECT COALESCE(start_contract.start_month,end_contract.end_month) AS report_month,
       IF(NOT num_start IS NULL,num_start,0) AS num_start,
       IF(NOT num_end IS NULL, num_end,0) AS num_end,
       IF(NOT num_start IS NULL,num_start,0)-IF(NOT num_end IS NULL, num_end,0) AS monthly,
       SUM(IF(NOT num_start IS NULL,num_start,0)-IF(NOT num_end IS NULL, num_end,0)) OVER(ORDER BY COALESCE(start_contract.start_month,end_contract.end_month)) AS total_valid
FROM start_contract
FULL JOIN end_contract ON start_contract.start_month=end_contract.end_month
ORDER BY 1
)

SELECT COALESCE(DATE_TRUNC(A.closedate,MONTH),B.report_month) AS report_month,
       B.num_start,B.num_end,B.total_valid,
       SUM(A.ACV_USD) AS uaa_revenue,
FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
FULL JOIN total B ON DATE_TRUNC(A.closedate,MONTH)=B.report_month
WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%'
GROUP BY 1,2,3,4