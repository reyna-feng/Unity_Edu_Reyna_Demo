--Update Time: 7/14
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.genesis_service` AS

SELECT A.createdTime,A.dueDate,A.updatedTime,A.currency,A.type,A.status,
       A.id,A.invoiceNumber,A.itemId,A.orderId,A.ownerId,A.paymentTransactionId,A.subscriptionId,
       B.name,B.slug,B.subscriptionGroup,
       COALESCE(C.countryOfPurchase,F.country) AS country,C.rev,
       COALESCE(C.source,F.source) AS source,
       COALESCE(D.quantity,G.quantity) AS quantity,D.componentKey,
       F.autoRenew,F.autoRenewOffReason,F.cancelReason,
       IF(A.status != 'DUNNING_PENDING_CHARGE',IF(H.exchange_rate IS NOT NULL, (A.amount-A.taxAmount)*H.exchange_rate,IF(A.currency='USD', (A.amount - A.taxAmount),0)),0) AS invoice_amount_confirmed_total,
       A.amount,H.currency_name
FROM `unity-ai-unity-insights-prd.source_genesis_mq_cr.subscription_invoice` A
JOIN (
  SELECT DISTINCT organizationId
  FROM `unity-ai-unity-insights-prd.verticals_inference.org_internalsegment`
  WHERE internal_Segment='Education' ) E ON E.organizationId=A.ownerId
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr.item` B ON A.itemId=B.id AND A.itemRevision=B.revision
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr.order` C ON A.orderId=C.id
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr.subscription_invoice_lineItems` D ON D.subscription_invoice_id=A.id AND D.itemId=A.itemId AND D.itemRevision=A.itemRevision AND A.uuid=D.uuid
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr.subscription` F ON F.id=A.subscriptionId AND F.itemId=A.itemId
LEFT JOIN `unity-ai-unity-insights-prd.source_genesis_mq_cr.order_item` G ON G.orderId=A.orderId AND G.itemId=A.itemId AND G.uuid=A.uuid

LEFT JOIN (
  select currency_name, validfrom, 
         IF(NOT LEAD(validfrom) OVER(PARTITION BY currency_name ORDER BY validfrom) IS NULL, LEAD(validfrom) OVER(PARTITION BY currency_name ORDER BY validfrom),'9999-01-01T00:00:00') AS validto,update_method_id, base_currency_name,exchange_rate
  from `unity-ai-unity-insights-prd.ai_feature_catalog.currencyexchangerates`
  where update_method_id='DIRECT' AND base_currency_name='USD'
  order by 1, 2) H ON H.currency_name=A.currency AND A.createdTime>=TIMESTAMP(H.validfrom) AND A.createdTime<TIMESTAMP(H.validto)
/*
LEFT JOIN (
  select currency_name, validfrom, 
         IF(NOT LEAD(validfrom) OVER(PARTITION BY currency_name ORDER BY validfrom) IS NULL, LEAD(validfrom) OVER(PARTITION BY currency_name ORDER BY validfrom),'9999-01-01T00:00:00') AS validto,update_method_id, base_currency_name,exchange_rate
  from `unity-ai-unity-insights-prd.ai_feature_catalog.currencyexchangerates`
  where update_method_id='DIRECT' AND base_currency_name='USD'
  order by 1, 2) H ON H.currency_name=A.currency AND A.updatedTime>=TIMESTAMP(H.validfrom) AND A.updatedTime<TIMESTAMP(H.validto)
*/