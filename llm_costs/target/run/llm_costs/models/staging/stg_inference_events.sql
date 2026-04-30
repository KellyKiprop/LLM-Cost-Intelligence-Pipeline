
  create view "defaultdb"."analytics"."stg_inference_events__dbt_tmp"
    
    
  as (
    with source as (
    select * from "defaultdb"."public"."inference_events"
),

cleaned as (
    select
        id,
        to_timestamp(event_ts / 1000)           as event_at,
        lower(trim(team))                        as team,
        lower(trim(user_id))                     as user_id,
        lower(trim(feature))                     as feature,
        lower(trim(model))                       as model,
        lower(trim(provider))                    as provider,
        coalesce(input_tokens, 0)                as input_tokens,
        coalesce(output_tokens, 0)               as output_tokens,
        coalesce(cost_usd, 0)                    as cost_usd,
        coalesce(latency_ms, 0)                  as latency_ms,
        request_id,
        date_trunc('hour', to_timestamp(event_ts / 1000))  as event_hour,
        date_trunc('day',  to_timestamp(event_ts / 1000))  as event_date
    from source
    where event_ts is not null
)

select * from cleaned
  );