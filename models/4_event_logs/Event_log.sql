with Activity_configuration as (
    select * from {{ ref('Activity_configuration') }}
),

Automation_estimates as (
    select * from {{ ref('Automation_estimates') }}
),

Cases as (
    select * from {{ ref('Cases') }}
),

Event_log as (
    select
        Cases."Case_ID_internal",
        {{ pm_utils.star(ref('Event_log_input'), except=['Event_cost', 'Event_processing_time', 'Case', 'Case_status', 'Case_type', 'Case_value']) }},
        coalesce({{ ref('Event_log_input') }}."Event_cost", Automation_estimates."Event_cost") as "Event_cost",
        coalesce({{ ref('Event_log_input') }}."Event_processing_time", Automation_estimates."Event_processing_time") as "Event_processing_time",
        Activity_configuration."Activity_order"
    from {{ ref('Event_log_input') }}
    left join Activity_configuration
        on {{ ref('Event_log_input') }}."Activity" = Activity_configuration."Activity"
    left join Automation_estimates
        on {{ ref('Event_log_input') }}."Activity" = Automation_estimates."Activity"
    left join Cases
        on {{ ref('Event_log_input') }}."Case_ID" = Cases."Case_ID"
)

select
    *,
    {{ pm_utils.id() }} as "Event_ID"
from Event_log
