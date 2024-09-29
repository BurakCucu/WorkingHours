with Event_log as (
    select * from {{ ref('Event_log') }}
),

Cases as (
    select * from {{ ref('Cases') }}
),

-- Cases with rework, where the same activity is executed by different users.
-- Use select distinct to only get one record per case when multiple activities show this behavior.
Cases_with_rework_different_users as (
    select distinct
        Actitvities_per_user."Case_ID"
    from (
        select
            Event_log."Case_ID",
            Event_log."Activity"
        from Event_log
        group by Event_log."Case_ID", Event_log."Activity", Event_log."User"
    ) as Actitvities_per_user
    -- When a case has multiple records of an activity after grouped by user, it was executed by different users.
    group by Actitvities_per_user."Case_ID", Actitvities_per_user."Activity"
    having count(*) > 1
),

Event_log_tag_preprocessing as (
    -- Create new fields to determine if a tag occurs
    select
        Event_log."Case_ID"
    from Event_log
    -- Determine the applicable filtering
    where 1 = 0
    group by Event_log."Case_ID"
),

Tags as (
    select
        Cases_with_rework_different_users."Case_ID",
        {{ pm_utils.as_varchar('Multiple users for same activity') }} as "Tag",
        {{ pm_utils.as_varchar('Rework') }} as "Tag_type"
    from Cases_with_rework_different_users
    union all
    -- Tag example based on Cases
    select
        Cases."Case_ID",
        {{ pm_utils.as_varchar('The name of the tag') }} as "Tag",
        {{ pm_utils.as_varchar('The type of the tag') }} as "Tag_type"
    from Cases
    -- Insert your logic to determine when the tag should trigger
    where 1 = 0
    union all
    -- Tag example based on Event log
    select
        Event_log_tag_preprocessing."Case_ID",
        {{ pm_utils.as_varchar('The name of the tag') }} as "Tag",
        {{ pm_utils.as_varchar('The type of the tag') }} as "Tag_type"
    from Event_log_tag_preprocessing
    -- Insert your logic to determine when the tag should trigger
    where 1 = 0
)

select
    Tags.*,
    {{ pm_utils.id() }} as "Tag_ID",
    Cases."Case_ID_internal"
from Tags
left join Cases
    on Tags."Case_ID" = Cases."Case_ID"
