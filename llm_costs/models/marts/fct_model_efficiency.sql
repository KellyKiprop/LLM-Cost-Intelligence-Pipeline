with events as (
    select * from {{ ref('stg_inference_events') }}
),

efficiency as (
    select
        model,
        provider,
        count(*)                                        as total_calls,
        sum(cost_usd)                                   as total_cost_usd,
        avg(cost_usd)                                   as avg_cost_per_call,
        avg(latency_ms)                                 as avg_latency_ms,
        sum(output_tokens)                              as total_output_tokens,
        round((sum(cost_usd) /
            nullif(sum(output_tokens), 0) * 1000)::numeric, 6)
                                                        as cost_per_1k_output_tokens,
        round((avg(output_tokens) /
            nullif(avg(latency_ms), 0) * 1000)::numeric, 2)
                                                        as tokens_per_second,
        count(distinct team)                            as teams_using_model
    from events
    group by 1, 2
)

select * from efficiency
order by cost_per_1k_output_tokens asc
