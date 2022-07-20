--Update Time: 7/7
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.opportunity_quarter_uaa_name` AS

WITH account AS(
SELECT *,
       ROW_NUMBER() OVER(PARTITION BY account_name ORDER BY start_quarter) AS rnk
FROM(
SELECT DATE_TRUNC(opportunity_start_date,quarter) AS start_quarter,
       DATE_TRUNC(opportunity_end_date,quarter) AS end_quarter,
       account_name,region
FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity`
WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%' AND SBQQ__ProductFamily__c='Education' AND Stage_Category='Won'
GROUP BY 1,2,3,4
ORDER BY account_name
) A
),

quarter AS(
SELECT report_quarter
FROM(
  SELECT DATE_TRUNC(A.closedate,quarter) AS report_quarter
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%' AND SBQQ__ProductFamily__c='Education' AND Stage_Category='Won'
  GROUP BY 1
  UNION ALL
  SELECT DATE_TRUNC(opportunity_start_date,quarter) AS report_quarter
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%' AND SBQQ__ProductFamily__c='Education' AND Stage_Category='Won'
  GROUP BY 1
  UNION ALL
  SELECT DATE_TRUNC(opportunity_end_date,quarter) AS report_quarter
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%' AND SBQQ__ProductFamily__c='Education' AND Stage_Category='Won'
  GROUP BY 1) AS A
GROUP BY 1
)


SELECT A.report_quarter,B.account_name,B.region,
       CASE WHEN rnk=1 THEN 'New Account' ELSE 'Returning Account' END AS account_type
FROM quarter A
LEFT JOIN account B ON A.report_quarter>=B.start_quarter AND A.report_quarter<B.end_quarter
GROUP BY 1,2,3,4