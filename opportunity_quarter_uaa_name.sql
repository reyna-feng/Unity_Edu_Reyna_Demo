--Update Time: 4/26
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.opportunity_quarter_uaa_name` AS

WITH account AS(
SELECT DATE_TRUNC(opportunity_start_date,quarter) AS start_quarter,
       DATE_TRUNC(opportunity_end_date,quarter) AS end_quarter,
       account_name
FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity`
WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%'
GROUP BY 1,2,3
),

quarter AS(
SELECT report_quarter
FROM(
  SELECT DATE_TRUNC(A.closedate,quarter) AS report_quarter
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%'
  GROUP BY 1
  UNION ALL
  SELECT DATE_TRUNC(opportunity_start_date,quarter) AS report_quarter
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%'
  GROUP BY 1
  UNION ALL
  SELECT DATE_TRUNC(opportunity_end_date,quarter) AS report_quarter
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%'
  GROUP BY 1) AS A
GROUP BY 1
)


SELECT A.report_quarter,B.account_name
FROM quarter A
LEFT JOIN account B ON A.report_quarter BETWEEN B.start_quarter AND B.end_quarter
GROUP BY 1,2