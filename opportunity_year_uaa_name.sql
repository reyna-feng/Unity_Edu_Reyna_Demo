--Update Time: 7/7
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.opportunity_year_uaa_name` AS

WITH account AS(
SELECT *,
       ROW_NUMBER() OVER(PARTITION BY account_name ORDER BY start_year) AS rnk
FROM(
SELECT DATE_TRUNC(opportunity_start_date,year) AS start_year,
       DATE_TRUNC(opportunity_end_date,year) AS end_year,
       account_name,region,opportunity_start_date
FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity`
WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%' AND SBQQ__ProductFamily__c='Education' AND Stage_Category='Won'
GROUP BY 1,2,3,4,5
ORDER BY account_name
) A
),

year AS(
SELECT report_year
FROM(
  SELECT DATE_TRUNC(A.closedate,year) AS report_year
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%' AND SBQQ__ProductFamily__c='Education' AND Stage_Category='Won'
  GROUP BY 1
  UNION ALL
  SELECT DATE_TRUNC(opportunity_start_date,year) AS report_year
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%' AND SBQQ__ProductFamily__c='Education' AND Stage_Category='Won'
  GROUP BY 1
  UNION ALL
  SELECT DATE_TRUNC(opportunity_end_date,year) AS report_year
  FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity` A
  WHERE SBQQ__ProductName__c LIKE '%Academic Alliance%' AND SBQQ__ProductFamily__c='Education' AND Stage_Category='Won'
  GROUP BY 1) AS A
GROUP BY 1
)


SELECT A.report_year,B.account_name,B.region,B.opportunity_start_date,
       CASE WHEN rnk=1 THEN 'New Account' ELSE 'Returning Account' END AS account_type
FROM year A
LEFT JOIN account B ON A.report_year>=B.start_year AND A.report_year<B.end_year
GROUP BY 1,2,3,4,5
ORDER BY 2