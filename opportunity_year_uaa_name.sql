--Update Time: 4/26
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.opportunity_year_uaa_name` AS

WITH account AS(
SELECT DATE_TRUNC(opportunity_start_date,year) AS start_year,
       DATE_TRUNC(opportunity_end_date,year) AS end_year,
       account_name
FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity`
WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%'
GROUP BY 1,2,3
),

year AS(
SELECT report_year
FROM(
  SELECT DATE_TRUNC(A.closedate,year) AS report_year
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%'
  GROUP BY 1
  UNION ALL
  SELECT DATE_TRUNC(opportunity_start_date,year) AS report_year
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%'
  GROUP BY 1
  UNION ALL
  SELECT DATE_TRUNC(opportunity_end_date,year) AS report_year
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%'
  GROUP BY 1) AS A
GROUP BY 1
)


SELECT A.report_year,B.account_name
FROM year A
LEFT JOIN account B ON A.report_year BETWEEN B.start_year AND B.end_year
GROUP BY 1,2