--Update time: 6/15
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.edu_revpro` AS

SELECT A.report_month,A.region,rc_line_customer_name,segment,A.product_code,
       CASE WHEN r_ttl_rev_activity IS NOT NULL AND r_ttl_rev_activity!='-' THEN CAST(REPLACE(r_ttl_rev_activity,',','') AS INTEGER) 
            ELSE 0 END AS r_ttl_rev_activity,
       B.internal_segment AS segment_per_report,
       B.internal_subsegment AS internal_segment_per_report,
       B.Channel__c AS direct_resller
FROM `unity-other-learn-prd.reynafeng.data_loader_revpro` A
LEFT JOIN (
      SELECT Opportunity_ID__c,internal_segment,internal_subsegment,Channel__c
      FROM `unity-other-learn-prd.reynafeng.salesforce_opportunity`
      WHERE Stage_Category='Won'
      GROUP BY 1,2,3,4
      ) B ON A.opportunity_id=B.Opportunity_ID__c
LEFT JOIN `unity-other-learn-prd.reynafeng.edu_product_codes` C ON C.product_code=A.product_code
WHERE A.report_month>='2021-01-01'
      AND (A.segment='Education' OR B.internal_segment='Education' OR C.name IS NOT NULL)
      AND A.exclude_atp='' AND A.rev_rec_hold_flag='N'
      AND segment!='DO NOT USE' AND r_ttl_rev_activity IS NOT NULL AND r_ttl_rev_activity!='-' AND r_ttl_rev_activity!='0'
ORDER BY 1 DESC