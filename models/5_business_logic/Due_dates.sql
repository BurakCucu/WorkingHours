with Cases as (
    select * from {{ ref('Cases') }}
),

Event_log as (
    select * from {{ ref('Event_log') }}
),

Due_dates_configuration as (
    select * from {{ ref('Due_dates_configuration') }}
),

/* Table containing all definitions of the due dates. */
Due_dates_base as (
    select
        Event_log."Case_ID",
        {{ pm_utils.as_varchar('The name of the due date') }} as "Due_date",
        Event_log."Event_end" as "Actual_date",
        -- Add logic which should be used as the Expected_date timestamp field
        {{ pm_utils.to_timestamp('null') }} as "Expected_date"
    from Event_log
    -- Insert your logic to determine when the due date should be calculated
    where 1 = 0
),

-- Join with the due dates configuration for additional properties.
Due_dates as (
    select
        Due_dates_base."Case_ID",
        Due_dates_base."Due_date",
        Due_dates_base."Actual_date",
        Due_dates_base."Expected_date",
        Due_dates_configuration."Due_date_type",
        case
            when Due_dates_base."Actual_date" <= Due_dates_base."Expected_date"
                then {{ pm_utils.to_boolean('true') }}
            else {{ pm_utils.to_boolean('false') }}
        end as "On_time",
        case
            -- when the due date is not configured, the cost is null
            when (
                Due_dates_configuration."Fixed_costs" is null
                or (Due_dates_configuration."Fixed_costs" = {{ pm_utils.to_boolean('false') }} and (Due_dates_configuration."Time" is null or Due_dates_configuration."Time_type" is null))
            )
                then {{ pm_utils.to_double('null') }}
            -- when the due date is configured and on time, the cost is 0
            when Due_dates_base."Actual_date" <= Due_dates_base."Expected_date"
                then {{ pm_utils.to_double('0') }}
            -- when the costs are fixed, the cost is known
            when Due_dates_configuration."Fixed_costs" = {{ pm_utils.to_boolean('true') }}
                then Due_dates_configuration."Cost"
            -- when the costs are not fixed, the cost is calculated based on time and time_type
            when Due_dates_configuration."Time_type" = 'millisecond'
                then floor({{ pm_utils.datediff('millisecond', 'Due_dates_base."Expected_date"', 'Due_dates_base."Actual_date"') }} / Due_dates_configuration."Time") * Due_dates_configuration."Cost"
            when Due_dates_configuration."Time_type" = 'second'
                then floor({{ pm_utils.datediff('millisecond', 'Due_dates_base."Expected_date"', 'Due_dates_base."Actual_date"') }} / 1000 / Due_dates_configuration."Time") * Due_dates_configuration."Cost"
            when Due_dates_configuration."Time_type" = 'minute'
                then floor({{ pm_utils.datediff('millisecond', 'Due_dates_base."Expected_date"', 'Due_dates_base."Actual_date"') }} / (60 * 1000) / Due_dates_configuration."Time") * Due_dates_configuration."Cost"
            when Due_dates_configuration."Time_type" = 'hour'
                then floor({{ pm_utils.datediff('millisecond', 'Due_dates_base."Expected_date"', 'Due_dates_base."Actual_date"') }} / (60 * 60 * 1000) / Due_dates_configuration."Time") * Due_dates_configuration."Cost"
            when Due_dates_configuration."Time_type" = 'day'
                then floor({{ pm_utils.datediff('millisecond', 'Due_dates_base."Expected_date"', 'Due_dates_base."Actual_date"') }} / (24 * 60 * 60 * 1000) / Due_dates_configuration."Time") * Due_dates_configuration."Cost"
            else {{ pm_utils.to_double('null') }}
        end as "Cost",
        {{ pm_utils.datediff('millisecond', 'Due_dates_base."Expected_date"', 'Due_dates_base."Actual_date"') }} as "Difference"
    from Due_dates_base
    left join Due_dates_configuration
        on Due_dates_base."Due_date" = Due_dates_configuration."Due_date"
)

select
    Due_dates.*,
    {{ pm_utils.id() }} as "Due_date_ID",
    Cases."Case_ID_internal"
from Due_dates
left join Cases
    on Due_dates."Case_ID" = Cases."Case_ID"
