--Update Time: 7/15
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.salesforce_opportunity` AS

SELECT CASE WHEN a.`StageName` IN ('Closed Won','Closed Won/Online Sale') THEN 'Won'
            WHEN a.`StageName` IN ('Closed Lost') THEN 'Lost'
            ELSE 'Open' END AS Stage_Category,
       a.`AccountId`,usr.name AS Account_Owner,acct.name AS account_name,a.`Opportunity_ID__c`, 
       a.name AS opportunity_name,a.`Channel__c`,c.SBQQ__ProductFamily__c,c.SBQQ__ProductName__c, 
       c.Effective_of_paid_contracted_seats__c,
	   CASE WHEN (c.Effective_of_paid_contracted_seats__c > 0 AND (CASE WHEN a.Internal_Segment__c = 'Education' AND c.SBQQ__NetTotal__c = 0 THEN 'Yes' ELSE 'No' END) = 'No') THEN 'Yes'
	        ELSE 'No' END AS PCL_Yes_No,c.ACV__c/cur.ConversionRate AS ACV_USD,c.CurrencyIsoCode AS CurrencyISOCode_, 
       c.SBQQ__NetTotal__c/cur.ConversionRate AS Net_Total_USD,a.Amount AS TCV,
       c.SBQQ__TotalDiscountAmount__c/cur.ConversionRate AS Total_Discount_USD,
       (c.SBQQ__NetTotal__c + c.SBQQ__TotalDiscountAmount__c)/cur.ConversionRate AS Total_no_Discount_USD,
       a.`ForecastCategoryName`,a.`StageName`,c.Line_Type_Sales__c AS line_type,a.`Region__c` AS region,
       acct.BillingCountry AS country,
       a.Sub_Region__c,a.`Internal_Segment__c` AS internal_segment,a.Internal_Sub_Segment__c AS internal_subsegment,
       CASE WHEN a.`Internal_Segment__c` = 'Education' THEN 'GG&E'
            WHEN a.`Internal_Segment__c` = 'AEC' THEN 'AEC'
            WHEN a.`Internal_Segment__c` = 'Gambling' THEN 'GG&E'
            WHEN a.`Internal_Segment__c` = 'ATM' THEN 'ATM'
            WHEN a.`Internal_Segment__c` = 'Platform Partner' THEN 'Other'
            WHEN a.`Internal_Segment__c` = 'Media & Entertainment' THEN 'M&E'
            WHEN a.`Internal_Segment__c` = 'Games' THEN 'GG&E'
            ELSE 'ATM' END AS summary_segment,
       d.qtr,DATE_TRUNC(a.`Start_Date__c`, month) AS THEMONTH,d.weekofquarter,c.`Start_Date__c` AS quoteline_start_date,
       c.End_Date__c AS quoteline_end_date,a.Start_Date__c AS opportunity_start_date,a.End_Date__c AS opportunity_end_date,
       a.closedate,a.SubscriptionTerm__c,us.User_Type__c,
       CASE WHEN us.User_Type__c IN ('Core Sales - Manager','Cloud Sales - Business Development','Core Sales - Manager - Business Development','Core Sales - Business Development') THEN 'BD'
            WHEN us.User_Type__c IN ('Core Sales - Account Executive','Core Sales - Manager - Account Executive') THEN 'AE'
            ELSE 'Other' END AS role_type,
       acct.Ultimate_Parent_ID_18__c,acct.Top_Account_Name__c,us.employeenumber,us.name AS opportunity_owner,
       E.*,
       CASE WHEN 
	     (CASE WHEN (c.Effective_of_paid_contracted_seats__c > 0 AND (CASE WHEN a.Internal_Segment__c = 'Education' AND c.SBQQ__NetTotal__c = 0 THEN 'Yes' ELSE 'No' END) = 'No') THEN 'Yes'
	           ELSE 'No' END) = 'Yes' THEN 'Subscriptions'
            WHEN SBQQ__ProductFamily__c IN ('Professional Services','Solutions') THEN 'Solutions'
            WHEN SBQQ__ProductFamily__c = 'Education' THEN 'Training & Education'
            WHEN SBQQ__ProductFamily__c = 'Training' THEN 'Training & Education'
            WHEN SBQQ__ProductFamily__c = 'Support' THEN 'Support'
            WHEN SBQQ__ProductFamily__c = 'Source' THEN 'Source' 
            ELSE 'Other' END AS Summary_Product_Family,
       CASE WHEN 
	     (CASE WHEN (c.Effective_of_paid_contracted_seats__c > 0 AND (CASE WHEN a.Internal_Segment__c = 'Education' AND c.SBQQ__NetTotal__c = 0 THEN 'Yes' ELSE 'No' END) = 'No') THEN 'Yes'
	           ELSE 'No' END) = 'Yes' THEN 'ARR ACV'
            WHEN SBQQ__ProductFamily__c in ('Professional Services','Solutions') THEN 'One-Time'
            WHEN SBQQ__ProductFamily__c = 'Education' THEN 'One-Time'
            WHEN SBQQ__ProductFamily__c = 'Training' THEN 'One-Time'
            WHEN SBQQ__ProductFamily__c = 'Support' THEN 'ARR ACV'
            WHEN SBQQ__ProductFamily__c = 'Source' THEN 'ARR ACV' 
            END AS product_group,
       a.Use_Case__c,a.SBQQ__Renewal__c,a.Pass_Through_Reseller__c,
       CASE WHEN a.Channel__c = 'Reseller' THEN reseller.name ELSE NULL END AS reseller,
       CASE WHEN a.Channel__c = 'Reseller' THEN reseller.Internal_Segment__c ELSE NULL END AS reseller_internal_segment,
       CASE WHEN a.Channel__c = 'Reseller' THEN reseller.Geo_Region__c ELSE NULL END AS reseller_region,
       CASE WHEN a.Channel__c = 'Reseller' THEN reseller.Geo_Subregion__c ELSE NULL END AS reseller_subregion,
       CASE WHEN a.Channel__c = 'Reseller' THEN reseller.Reseller_Approval_Requested__c ELSE NULL END AS reseller_Approval_Request,
       CASE WHEN a.Channel__c = 'Reseller' THEN reseller.Reseller_Discount__c ELSE NULL END AS reseller_Discount,
       CASE WHEN a.Channel__c = 'Reseller' THEN reseller.Reseller_Start_Date__c ELSE NULL END AS reseller_Start_Date__c,
       CASE WHEN a.Channel__c = 'Reseller' THEN reseller.Reseller_Status__c ELSE NULL END AS reseller_status,
       CASE WHEN a.Channel__c = 'Reseller' THEN reseller.Reseller_Type__c ELSE NULL END AS reseller_type,
       a.Deal_Category__c, p.Revenue_Category__c AS Revenue_Category, c.SBQQ__TotalDiscountRate__c/100 AS SFDC_Discount,
       c.name AS Quote_Line_Name, 
       CASE WHEN c.`End_Date__c` >= current_date() THEN 'ACTIVE' ELSE 'INACTIVE' END AS ACTIVE_CHECKER
FROM `unity-ai-unity-insights-prd.salesops_insights`.`Opportunity` a 
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights`.`Quotes` b ON a.id = b.SBQQ__Opportunity2__c
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights`.`Quotelines` c ON b.id = c.SBQQ__Quote__c
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights`.currencytype cur ON c.CurrencyIsoCode = cur.IsoCode
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights`.ref_calendar d ON DATE(a.`Start_Date__c`)=d.THEDATE
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights`.Users us ON a.OwnerId = us.id
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights`.Users mg ON us.managerid = mg.id
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights.Account` acct ON a.`AccountId` = acct.id
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights`.Users usr ON acct.OwnerId = usr.id
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights.Account` reseller ON a.Bill_To_Account__c = reseller.id
LEFT JOIN `unity-ai-unity-insights-prd.salesops_insights.Products` p ON c.SBQQ__Product__c = p.Product_ID_CaseSafe__c
LEFT JOIN `unity-ai-unity-insights-prd.source_sfdc2_cr_restricted.product2` E ON e.Name = SBQQ__ProductName__c
WHERE b.SBQQ__Primary__c = true
      --AND d.qtr >= '2020-Q1'
      --AND a.`StageName` IN ('Closed Won','Closed Won/Online Sale') 
      --AND c.`End_Date__c` >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) 
      AND (a.`Internal_Segment__c` LIKE 'Education' OR c.SBQQ__ProductFamily__c LIKE 'Education' OR usr.name LIKE 'Israel Macias')
      --We only look at ACV
      --AND c.`Start_Date__c`=a.Start_Date__c