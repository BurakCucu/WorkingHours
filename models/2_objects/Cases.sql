with Event_log_input as (
    select * from {{ ref('Event_log_input') }}
),

/* Aggregate of the event log table to define a cases table with case fields.*/
Cases as (
    select
        Event_log_input."Case_ID",
        min(Event_log_input."Case") as "Case",
        min(Event_log_input."Case_status") as "Case_status",
        min(Event_log_input."Case_type") as "Case_type",
        min(Event_log_input."Case_value") as "Case_value",
        count(*) as "Event_count"
    from Event_log_input
    group by Event_log_input."Case_ID"
)

select
    *,
    {{ pm_utils.id() }} as "Case_ID_internal"
from Cases
