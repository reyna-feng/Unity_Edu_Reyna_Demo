--Update Time: 7/19
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.opportunity_uaa` AS

SELECT DATE_TRUNC(A.closedate,MONTH) AS report_month,
       SUM(A.ACV_USD) AS uaa_revenue,
FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%' AND SBQQ__ProductFamily__c='Education' AND Stage_Category='Won'
      AND quoteline_start_date=opportunity_start_date AND DATE_TRUNC(closedate,YEAR)=DATE_TRUNC(opportunity_start_date,YEAR)
GROUP BY 1
ORDER BY 1 DESC