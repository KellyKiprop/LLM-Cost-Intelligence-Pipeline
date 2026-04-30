with hourly as (
    select
        event_hour,
        team,
        model,
        count(*)            as call_count,
        sum(cost_usd)       as hourly_cost_usd,
        avg(latency_ms)     as avg_latency_ms
    from {{ ref('stg_inference_events') }}
    group by 1, 2, 3
),

baseline as (
    select
        team,
        model,
        avg(hourly_cost_usd)    as avg_hourly_cost,
        stddev(hourly_cost_usd) as stddev_hourly_cost,
        max(hourly_cost_usd)    as max_hourly_cost
    from hourly
    group by 1, 2
),

flagged as (
    select
        h.event_hour,
        h.team,
        h.model,
        h.call_count,
        h.hourly_cost_usd,
        h.avg_latency_ms,
        b.avg_hourly_cost,
        b.stddev_hourly_cost,
        round((h.hourly_cost_usd /
            nullif(b.avg_hourly_cost, 0))::numeric, 2)  as cost_ratio,
        case
            when b.stddev_hourly_cost > 0
             and h.hourly_cost_usd > (b.avg_hourly_cost + 2 * b.stddev_hourly_cost)
            then true
            else false
        end                                              as is_anomaly
    from hourly h
    join baseline b using (team, model)
)

select * from flagged
order by event_hour desc, cost_ratio desc
