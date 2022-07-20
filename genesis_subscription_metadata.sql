--Update Time: 7/19
CREATE OR REPLACE TABLE `unity-other-learn-prd.reynafeng.genesis_subscription_metadata` AS
    select
        subs.ownerId,subs.ownerType,subs.uuid,
        cast(subs.id as string) as subscription_id
        , subs.createdTime as created_time
        , subs.updatedTime as updated_time
        , subs.subsStartDate as subscription_start_date
        , subs.subsEndDate as subscription_end_date
        , subs.nextBillingDate as subscription_next_billing_date
        , subs.group as subscription_group
        , subs.source as subscription_source
        , item.name as subscription_name
        , item.slug as subscription_slug
        , item.subscriptionBillingCycleUnit as subscription_billing_cycle
        , item.subscriptionCommitmentRequired as subscription_commitment_required
        , item.subscriptionAutoRenew as subscription_autorenew
        , item.subscriptionDurationUnit as subscription_duration
    --from `unity-it-open-dataplatform-prd.dw_genesis_mq_cr_restricted.subscription` subs
    from `unity-ai-unity-insights-prd.source_genesis_mq_cr.subscription` subs
    --left join `unity-it-open-dataplatform-prd.dw_genesis_mq_cr_restricted.item` item
    left join `unity-ai-unity-insights-prd.source_genesis_mq_cr.item` item
    on subs.itemId = item.id and subs.itemRev = item.revision