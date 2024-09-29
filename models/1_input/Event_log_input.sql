{{ config(
    pre_hook="{{ pm_utils.create_index(source('sources', 'Event_log_raw')) }}"
) }}

{% set source_table = source('sources', 'Event_log_raw') %}

/* Input table for the event log containing case and event properties.
The macro optional() creates the field with null values when it is not present in the source table.
Convert the non-text fields to the correct data type. */
with Event_log_input as (
    select
        -- Select all fields in the source table. Fields that need type casting are defined separately with the appropriate type casting applied.
        {{ pm_utils.star(source_table, except=['Event_end', 'Automated', 'Event_cost', 'Event_processing_time', 'Event_start', 'User',
            'Case', 'Case_status', 'Case_type', 'Case_value'] ) }},
        -- Mandatory
        -- The Case_ID and Activity fields are mandatory text fields that don't need to be type casted. 
        {{ pm_utils.mandatory(source_table, '"Event_end"', 'datetime') }} as "Event_end",
        -- Optional
        {{ pm_utils.optional(source_table, '"Automated"', 'boolean') }} as "Automated",
        {{ pm_utils.optional(source_table, '"Event_cost"', 'double') }} as "Event_cost",
        {{ pm_utils.optional(source_table, '"Event_processing_time"', 'integer') }} as "Event_processing_time",
        {{ pm_utils.optional(source_table, '"Event_start"', 'datetime') }} as "Event_start",
        {{ pm_utils.optional(source_table, '"User"') }} as "User",
        {{ pm_utils.optional(source_table, '"Case"') }} as "Case",
        {{ pm_utils.optional(source_table, '"Case_status"') }} as "Case_status",
        {{ pm_utils.optional(source_table, '"Case_type"') }} as "Case_type",
        {{ pm_utils.optional(source_table, '"Case_value"', 'double') }} as "Case_value"
    from {{ source_table }}
)

select * from Event_log_input
