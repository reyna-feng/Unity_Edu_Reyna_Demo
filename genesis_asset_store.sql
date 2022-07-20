--Update Time: 7/14
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.genesis_asset_store` AS

SELECT A.createdTime,A.updatedTime,A.currency,
       A.id,A.invoiceNumber,A.orderId,A.ownerId,A.paymentTransactionId,
       C.countryOfPurchase AS country,C.totalAmount,C.totalTax,
       IF(H.exchange_rate IS NOT NULL, (C.totalAmount-A.taxAmount)*H.exchange_rate,IF(A.currency='USD', (C.totalAmount - A.taxAmount),0)) AS invoice_amount_confirmed_total,
       H.currency_name,A.taxAmount
FROM `unity-ai-unity-insights-prd.source_genesis_mq_cr.assetstore_invoice` A
JOIN (
SELECT DISTINCT organizationId
FROM(
  SELECT DISTINCT organizationId
  FROM `unity-create-data-prd.segmentation.org_internalsegment`
  WHERE internal_Segment='Education'
  UNION ALL
  SELECT DISTINCT organization_id
  FROM `unity-create-data-prd.subscriptions_core_models.organizations_segments`
  WHERE segment='Education') A ) E ON E.organizationId=A.ownerId
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr.order` C ON A.orderId=C.id
LEFT JOIN (
  select currency_name, validfrom, 
         IF(NOT LEAD(validfrom) OVER(PARTITION BY currency_name ORDER BY validfrom) IS NULL, LEAD(validfrom) OVER(PARTITION BY currency_name ORDER BY validfrom),'9999-01-01T00:00:00') AS validto,update_method_id, base_currency_name,exchange_rate
  from `unity-ai-unity-insights-prd.ai_feature_catalog.currencyexchangerates`
  where update_method_id='DIRECT' AND base_currency_name='USD'
  order by 1, 2) H ON H.currency_name=A.currency AND A.createdTime>=TIMESTAMP(H.validfrom) AND A.createdTime<TIMESTAMP(H.validto)
