with events as (
    select * from "defaultdb"."analytics"."stg_inference_events"
),

aggregated as (
    select
        event_date,
        team,
        model,
        provider,
        count(*)                                        as call_count,
        sum(cost_usd)                                   as total_cost_usd,
        avg(cost_usd)                                   as avg_cost_per_call,
        sum(input_tokens)                               as total_input_tokens,
        sum(output_tokens)                              as total_output_tokens,
        avg(latency_ms)                                 as avg_latency_ms,
        min(latency_ms)                                 as min_latency_ms,
        max(latency_ms)                                 as max_latency_ms,
        count(distinct user_id)                         as unique_users,
        count(distinct feature)                         as unique_features,
        sum(case when latency_ms < 500
            then cost_usd else 0 end)                   as fast_response_cost_usd,
        sum(case when latency_ms > 2000
            then cost_usd else 0 end)                   as slow_response_cost_usd
    from events
    group by 1, 2, 3, 4
)

select * from aggregated
order by event_date desc, total_cost_usd desc