with Automation_estimates_raw as (
    select * from {{ ref('Automation_estimates_raw') }}
),

-- Cast fields from the seeds file to the correct data type.
Automation_estimates as (
    select
        {{ pm_utils.to_varchar('Automation_estimates_raw."Activity"') }} as "Activity",
        {{ pm_utils.to_double('Automation_estimates_raw."Event_cost"') }} as "Event_cost",
        {{ pm_utils.to_integer('Automation_estimates_raw."Event_processing_time"') }} as "Event_processing_time"
    from Automation_estimates_raw
)

select * from Automation_estimates
