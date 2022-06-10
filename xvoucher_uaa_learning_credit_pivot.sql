--Update Time: 6/6
CREATE OR REPLACE VIEW `unity-other-learn-prd.reynafeng.xvoucher_uaa_learning_credit_pivot` AS 

WITH data_source AS(
SELECT * EXCEPT(ExpirationDate),
       DATE(ExpirationDate) AS ExpirationDate,
       SUM(TransactionAmount) OVER(PARTITION BY CustomerName,ExpirationDate ORDER BY TransactionDate) AS running_balance,
       CASE WHEN TransactionAmount>-1 THEN 'Assignations' ELSE 'Redemptions' END AS TransactionType
FROM `unity-other-learn-prd.reynafeng.xvoucher_uaa_learning_credit`
ORDER BY CustomerName,TransactionDate
),

assign_date AS(
SELECT CustomerName,DATE(TransactionDate) AS AssignationDate,DATE(ExpirationDate) AS ExpirationDate,
       DATE_ADD(DATE(TransactionDate),INTERVAL 1 YEAR) AS ExpirationDate_Temp
FROM data_source
WHERE TransactionType='Assignations' AND OrderId!='Manual Adjustment'
GROUP BY 1,2,3,4
ORDER BY 1,2
),

redeem AS(
SELECT CustomerName,AssignationDate,AVG(DaysDiff) AS days_diff,MAX(DATE(A.TransactionDate)) AS max_transactiondate
FROM(
SELECT A.*,
       B.AssignationDate,
       DATE_DIFF(DATE(A.TransactionDate), B.AssignationDate, DAY) AS DaysDiff
FROM data_source A
JOIN assign_date B ON A.CustomerName=B.CustomerName AND A.ExpirationDate=B.ExpirationDate
WHERE TransactionType='Redemptions' AND OrderId!='Manual Adjustment' AND DATE(A.TransactionDate)>=B.AssignationDate) A
GROUP BY 1,2
ORDER BY 3 ASC
)

SELECT A.CustomerName,
       B.AssignationDate,
       CASE WHEN B.ExpirationDate_Temp<A.ExpirationDate THEN B.ExpirationDate_Temp ELSE A.ExpirationDate END AS ExpirationDate,
       C.days_diff,C.max_transactiondate,
       CASE WHEN C.days_diff IS NOT NULL THEN A.CustomerName ELSE null END AS RedeemCustomer,
       CASE WHEN C.days_diff BETWEEN 0 AND 30 THEN '0-30 Days'
            WHEN C.days_diff BETWEEN 31 AND 60 THEN '31-60 Days'
            WHEN C.days_diff BETWEEN 61 AND 90 THEN '61-90 Days'
            WHEN C.days_diff>90 THEN '91+ Days' ELSE 'Non Redeem' END AS days_range
FROM data_source A
JOIN assign_date B ON A.CustomerName=B.CustomerName AND A.ExpirationDate=B.ExpirationDate
LEFT JOIN redeem C ON A.CustomerName=C.CustomerName AND B.AssignationDate=C.AssignationDate
WHERE OrderId!='Manual Adjustment' AND DATE(A.TransactionDate)>=B.AssignationDate
GROUP BY 1,2,3,4,5,6
ORDER BY 1