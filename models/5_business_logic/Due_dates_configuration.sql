with Due_dates_configuration_raw as (
    select * from {{ ref('Due_dates_configuration_raw') }}
),

-- Cast fields from the seed file to the correct data type.
Due_dates_configuration as (
    select
        {{ pm_utils.to_varchar('Due_dates_configuration_raw."Due_date"') }} as "Due_date",
        {{ pm_utils.to_varchar('Due_dates_configuration_raw."Due_date_type"') }} as "Due_date_type",
        {{ pm_utils.to_boolean('Due_dates_configuration_raw."Fixed_costs"') }} as "Fixed_costs",
        {{ pm_utils.to_double('Due_dates_configuration_raw."Cost"') }} as "Cost",
        {{ pm_utils.to_integer('Due_dates_configuration_raw."Time"') }} as "Time",
        {{ pm_utils.to_varchar('Due_dates_configuration_raw."Time_type"') }} as "Time_type"
    from Due_dates_configuration_raw
)

select * from Due_dates_configuration
