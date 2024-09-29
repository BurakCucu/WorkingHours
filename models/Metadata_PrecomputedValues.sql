with Cases as (
    select * from {{ ref('Cases') }}
),

Event_log as (
    select * from {{ ref('Event_log') }}
),

Metadata_PrecomputedValues as (
    select
        {{ pm_utils.as_varchar('Number of cases') }} as "Key",
        count(*) as "Value"
    from Cases
    union
    select
        {{ pm_utils.as_varchar('Number of events') }} as "Key",
        count(*) as "Value"
    from Event_log
)

select * from Metadata_PrecomputedValues
