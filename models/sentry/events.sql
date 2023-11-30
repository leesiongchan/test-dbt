SET enable_case_sensitive_super_attribute to TRUE;

WITH
stg_events AS (
    SELECT
        e._airbyte_data.dateCreated::TEXT AS date_created,
        e._airbyte_data.id::TEXT,
        e._airbyte_data.groupID::INTEGER AS group_id,
        e._airbyte_data.title::TEXT AS culprit,
        e._airbyte_data.tags
    FROM {{ source('airbyte', '_airbyte_raw_events')}} e
),
events AS (
    SELECT
        e.date_created,
        e.id,
        e.group_id,
        e.culprit,
        et.value::TEXT AS report_type
    FROM stg_events e, e.tags AS et
    WHERE et.key = 'ReportType'
)

SELECT *
FROM events
